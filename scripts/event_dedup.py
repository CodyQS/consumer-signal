"""Conservative event-level de-duplication for Consumer Signal.

The central feeds intentionally retain every independently attributable source.
This module is used only when preparing a reader's digest: it selects one
primary item for a substantially identical event and attaches all other items
as corroborating links.  The matching is deliberately conservative so that a
launch, a later review and a separate supply-chain development for the same
product do not get collapsed into one event.
"""

from __future__ import annotations

import hashlib
import re
import unicodedata
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any


EVIDENCE_PRIORITY = {
    "primary_disclosure": 50,
    "market_data": 40,
    "industry_reporting": 30,
    "review_feedback": 20,
    "early_signal": 10,
}

# These normalizations cover the common Chinese / English names that occur in
# the configured sources.  They are not translations; they make the same named
# entity comparable when one source writes in Chinese and another in English.
ENTITY_ALIASES = (
    ("苹果", " apple "),
    ("三星", " samsung "),
    ("谷歌", " google "),
    ("小米", " xiaomi "),
    ("红米", " redmi "),
    ("华为", " huawei "),
    ("荣耀", " honor "),
    ("联发科", " mediatek "),
    ("高通", " qualcomm "),
    ("台积电", " tsmc "),
    ("鸿海", " foxconn "),
    ("富士康", " foxconn "),
    ("立讯精密", " luxshare "),
    ("歌尔", " goertek "),
    ("京东方", " boe "),
    ("智能眼镜", " smart glasses "),
    ("ai眼镜", " ai glasses "),
    ("ai 眼镜", " ai glasses "),
    ("混合现实", " mixed reality "),
    ("空间计算", " spatial computing "),
    ("端侧ai", " on device ai "),
    ("端侧 ai", " on device ai "),
)

GENERIC_TOKENS = frozenset(
    {
        "a", "an", "and", "announces", "announcement", "announced", "at", "by",
        "for", "from", "in", "introduces", "introducing", "is", "launch", "launches",
        "latest", "new", "of", "on", "our", "release", "releases", "reveals", "the",
        "to", "unveils", "with", "update", "updates", "first", "look", "news", "report",
        "reports", "says", "will", "产品", "公司", "公布", "发布", "推出", "新闻", "最新",
    }
)

PRODUCT_TOKENS = frozenset(
    {
        "airpods", "galaxy", "harmonyos", "iphone", "ipad", "macbook", "mate", "pixel",
        "pura", "quest", "rayban", "redmi", "snapdragon", "surface", "thinkpad", "vision",
        "watch", "xiaomi", "xr",
    }
)

COMPANY_TOKENS = frozenset(
    {
        "acer", "apple", "asus", "boe", "cxmt", "dell", "foxconn", "google", "goertek",
        "honor", "huawei", "lenovo", "logitech", "luxshare", "mediatek", "meta", "motorola",
        "oneplus", "oppo", "qualcomm", "redmi", "samsung", "smic", "sony", "transsion",
        "tsmc", "vivo", "xiaomi",
    }
)

ACTION_KEYWORDS = {
    "launch": ("launch", "announce", "release", "unveil", "available", "pricing", "发布", "上市", "开售", "发售", "定价"),
    "review": ("review", "benchmark", "teardown", "hands on", "评测", "体验", "拆解", "跑分"),
    "supply_chain": ("supplier", "supply chain", "order", "stocking", "inventory", "production", "yield", "capacity", "供应链", "供应商", "订单", "备货", "产能", "良率", "量产"),
    "market": ("shipment", "sales", "market share", "sell through", "demand", "forecast", "渠道", "销量", "出货", "份额", "需求", "预测"),
    "financial": ("earnings", "financial", "revenue", "results", "营收", "财报", "业绩"),
}


def clean_event_text(value: Any) -> str:
    text = unicodedata.normalize("NFKC", str(value or "")).casefold()
    text = re.sub(r"https?://\S+", " ", text)
    for original, replacement in ENTITY_ALIASES:
        text = text.replace(original, replacement)
    text = re.sub(r"\b(?:first|1st)\s+quarter\b|\bq1\b", " q1 ", text)
    text = re.sub(r"\b(?:second|2nd)\s+quarter\b|\bq2\b", " q2 ", text)
    text = re.sub(r"\b(?:third|3rd)\s+quarter\b|\b3q\b|\bq3\b", " q3 ", text)
    text = re.sub(r"\b(?:fourth|4th)\s+quarter\b|\b4q\b|\bq4\b", " q4 ", text)
    text = text.replace("第一季度", " q1 ").replace("第二季度", " q2 ")
    text = text.replace("第三季度", " q3 ").replace("第四季度", " q4 ")
    text = re.sub(r"[^a-z0-9\u4e00-\u9fff]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def event_tokens(value: Any) -> set[str]:
    tokens = set(token_sequence(value))
    normalized = clean_event_text(value)
    for run in re.findall(r"[\u4e00-\u9fff]{2,}", normalized):
        # Chinese sources do not have whitespace word boundaries.  Bigrams are
        # enough to compare a repeated named phrase without making a one-letter
        # match look meaningful.
        tokens.update(run[index:index + 2] for index in range(len(run) - 1))
    return {token for token in tokens if token not in GENERIC_TOKENS and len(token) > 1}


def token_sequence(value: Any) -> list[str]:
    normalized = clean_event_text(value)
    return [
        token for token in re.findall(r"[a-z]+[a-z0-9]*|\d+(?:\.\d+)?", normalized)
        if token not in GENERIC_TOKENS and len(token) > 1
    ]


def event_phrases(value: Any) -> set[str]:
    sequence = token_sequence(value)
    return {" ".join(sequence[index:index + 2]) for index in range(len(sequence) - 1)}


def action_class(value: Any) -> str:
    normalized = clean_event_text(value)
    for label, keywords in ACTION_KEYWORDS.items():
        if any(keyword in normalized for keyword in keywords):
            return label
    return ""


def is_same_event(left: dict[str, Any], right: dict[str, Any]) -> bool:
    """Return True only for high-confidence duplicate coverage of one event."""
    left_text = left["normalized"]
    right_text = right["normalized"]
    if not left_text or not right_text:
        return False
    if left_text == right_text:
        return True

    left_action, right_action = left["action"], right["action"]
    if left_action and right_action and left_action != right_action:
        return False

    left_tokens, right_tokens = left["tokens"], right["tokens"]
    shared = left_tokens & right_tokens
    if not shared:
        return False
    union = left_tokens | right_tokens
    similarity = len(shared) / len(union)
    product_overlap = PRODUCT_TOKENS & shared
    company_overlap = COMPANY_TOKENS & shared
    numeric_overlap = {
        token for token in shared
        if token.isdigit() and not (2020 <= int(token) <= 2035)
    }

    # A product family + a model number is a strong event anchor, but still
    # requires a modest title overlap.  This handles e.g. Chinese and English
    # coverage of the same iPhone / Galaxy launch.
    if product_overlap and numeric_overlap and similarity >= 0.35:
        return True
    quarter_overlap = {"q1", "q2", "q3", "q4"} & shared
    if company_overlap and quarter_overlap and left_action == right_action == "financial":
        return True
    phrase_overlap = left["phrases"] & right["phrases"]
    if company_overlap and left_action and left_action == right_action:
        if phrase_overlap and similarity >= 0.25:
            return True
        if len(shared) >= 3 and similarity >= 0.32:
            return True
    # For company, component and market stories without a model number, demand
    # more overlapping non-generic terms to avoid merging related developments.
    if len(shared) >= 3 and similarity >= 0.55:
        return True
    return len(shared) >= 4 and similarity >= 0.45


def parse_timestamp(value: Any) -> datetime:
    try:
        parsed = datetime.fromisoformat(str(value or "").replace("Z", "+00:00"))
        return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
    except ValueError:
        return datetime.min.replace(tzinfo=timezone.utc)


def record_from_item(kind: str, item: dict[str, Any], parent: dict[str, Any] | None = None) -> dict[str, Any]:
    parent = parent or {}
    title = str(item.get("title") or item.get("text") or "").strip()
    detail = str(item.get("summary") or item.get("description") or "").strip()
    match_text = title if title else detail
    timestamp = item.get("published") or item.get("created_at") or ""
    return {
        "kind": kind,
        "item": item,
        "parent": parent,
        "id": str(item.get("id") or item.get("url") or ""),
        "title": title,
        "detail": detail,
        "url": str(item.get("url") or ""),
        "timestamp": str(timestamp),
        "normalized": clean_event_text(match_text),
        "tokens": event_tokens(match_text),
        "phrases": event_phrases(match_text),
        "action": action_class(f"{title} {detail}"),
        "evidence_class": item.get("evidence_class") or parent.get("evidence_class") or "",
        "source": (
            item.get("source_name") or item.get("source")
            or parent.get("name") or parent.get("handle") or kind
        ),
    }


def source_reference(record: dict[str, Any]) -> dict[str, Any]:
    return {
        "kind": record["kind"],
        "id": record["id"],
        "source": record["source"],
        "title": record["title"],
        "url": record["url"],
        "timestamp": record["timestamp"],
        "evidence_class": record["evidence_class"],
    }


def primary_rank(record: dict[str, Any]) -> tuple[int, int, datetime, str]:
    # For equal evidence, a source article generally carries more self-contained
    # context than a social post.  Newer material is a deterministic final tie
    # breaker, never a reason to outrank stronger evidence.
    return (
        EVIDENCE_PRIORITY.get(record["evidence_class"], 0),
        1 if record["kind"] == "article" else 0,
        parse_timestamp(record["timestamp"]),
        record["id"],
    )


def cluster_records(records: list[dict[str, Any]]) -> list[list[dict[str, Any]]]:
    parents = list(range(len(records)))

    def find(index: int) -> int:
        while parents[index] != index:
            parents[index] = parents[parents[index]]
            index = parents[index]
        return index

    def union(left: int, right: int) -> None:
        left_root, right_root = find(left), find(right)
        if left_root != right_root:
            parents[right_root] = left_root

    for left in range(len(records)):
        for right in range(left + 1, len(records)):
            if is_same_event(records[left], records[right]):
                union(left, right)

    groups: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for index, record in enumerate(records):
        groups[find(index)].append(record)
    return list(groups.values())


def dedupe_digest_events(
    x_accounts: list[dict[str, Any]],
    articles: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]], dict[str, set[str]]]:
    """Select primary records and return reader-facing event groups.

    ``suppressed_ids`` includes every linked raw item, including the selected
    primary.  Callers use it to mark all sources of a delivered event as seen,
    preventing an identical corroborating article from surfacing tomorrow.
    """
    records = []
    for account_index, account in enumerate(x_accounts):
        for tweet_index, tweet in enumerate(account.get("tweets", [])):
            record = record_from_item("x", tweet, account)
            record["account_index"] = account_index
            record["tweet_index"] = tweet_index
            records.append(record)
    for article_index, article in enumerate(articles):
        record = record_from_item("article", article)
        record["article_index"] = article_index
        records.append(record)

    selected_tweets: set[tuple[int, int]] = set()
    selected_articles: set[int] = set()
    ranked_event_groups = []
    suppressed_ids = {"tweets": set(), "articles": set()}

    for cluster in cluster_records(records):
        primary = max(cluster, key=primary_rank)
        references = [source_reference(record) for record in sorted(cluster, key=primary_rank, reverse=True)]
        primary_reference = source_reference(primary)
        event_id = "event-" + hashlib.sha1(primary["normalized"].encode("utf-8")).hexdigest()[:12]
        event_group = {
            "id": event_id,
            "title": primary["title"],
            "primary": primary_reference,
            "sources": references,
            "supporting_sources": references[1:],
            "source_count": len(references),
            "duplicate_count": max(0, len(references) - 1),
        }
        ranked_event_groups.append((primary_rank(primary), event_group))
        for record in cluster:
            if record["id"]:
                suppressed_ids["tweets" if record["kind"] == "x" else "articles"].add(record["id"])

        annotated_item = dict(primary["item"])
        annotated_item.update(
            {
                "event_id": event_id,
                "event_source_count": len(references),
                "event_sources": references,
            }
        )
        primary["annotated_item"] = annotated_item
        if primary["kind"] == "x":
            selected_tweets.add((primary["account_index"], primary["tweet_index"]))
        else:
            selected_articles.add(primary["article_index"])

    deduped_accounts = []
    for account_index, account in enumerate(x_accounts):
        tweets = []
        for tweet_index, tweet in enumerate(account.get("tweets", [])):
            if (account_index, tweet_index) in selected_tweets:
                record = next(
                    record for record in records
                    if record.get("account_index") == account_index
                    and record.get("tweet_index") == tweet_index
                )
                tweets.append(record["annotated_item"])
        deduped_accounts.append({**account, "tweets": tweets})

    deduped_articles = []
    for article_index, article in enumerate(articles):
        if article_index in selected_articles:
            record = next(
                record for record in records
                if record.get("article_index") == article_index
            )
            deduped_articles.append(record["annotated_item"])

    ranked_event_groups.sort(key=lambda item: item[0], reverse=True)
    event_groups = [event for _rank, event in ranked_event_groups]
    return deduped_accounts, deduped_articles, event_groups, suppressed_ids
