"""Render prepared Consumer Signal JSON into a readable Markdown digest.

This lightweight, no-LLM fallback groups already-filtered public-feed items
into the four Consumer Signal research lenses. The Agent workflow can produce
a richer analytical digest by following the prompts in ``prompts/``.

Usage:
    python scripts/prepare_digest.py | python scripts/render_digest.py
"""

import json
import re
import sys
from datetime import datetime, timezone
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError


NOISE_PATTERNS = (
    r"^agree$", r"^haha", r"^thank", r"^thanks", r"^ty$",
    r"good morning", r"please report back",
)

EVIDENCE_LABELS = {
    "primary_disclosure": "官方",
    "market_data": "数据/研究",
    "industry_reporting": "报道/分析",
    "early_signal": "线索/传闻",
    "review_feedback": "评测/口碑",
}

SECTIONS = (
    (
        "brand_product",
        "终端新品、研发与市场评价",
        (
            "launch", "announce", "release", "ship", "available", "pricing", "price", "review",
            "hands-on", "benchmark", "camera", "design", "unveil", "prototype", "新品", "发布",
            "上市", "开售", "发售", "定价", "评测", "体验", "研发", "规格", "影像",
        ),
    ),
    (
        "supply_chain",
        "供应链、备货与新技术",
        (
            "supply chain", "supplier", "component", "display", "oled", "memory", "dram", "nand",
            "battery", "camera module", "lens", "semiconductor", "soc", "chip", "foundry", "order",
            "inventory", "stocking", "production", "yield", "bom", "供应链", "供应商", "零部件",
            "面板", "存储", "电池", "镜头", "芯片", "备货", "产能", "良率", "量产", "订单",
        ),
    ),
    (
        "market",
        "行业供给、需求与渠道",
        (
            "shipment", "sales", "sell-through", "market share", "asp", "demand", "channel",
            "retail", "promotion", "forecast", "guidance", "export", "import", "市场份额", "销量",
            "出货", "需求", "渠道", "库存", "促销", "预测", "景气", "出口", "进口",
        ),
    ),
    (
        "emerging",
        "端侧 AI 与新形态设备",
        (
            "ai phone", "ai pc", "ai glasses", "smart glasses", "ar glasses", "xr", "vr", "spatial",
            "wearable", "smartwatch", "earbuds", "smart speaker", "on-device", "edge ai", "ai眼镜",
            "ai 手机", "ai手机", "ai pc", "ai电脑", "端侧", "智能眼镜", "穿戴", "智能手表",
            "耳机", "智能音箱", "空间计算", "xr", "ar", "vr",
        ),
    ),
)


def clean_text(text):
    return "".join(ch for ch in str(text or "") if not 0xD800 <= ord(ch) <= 0xDFFF)


def configure_stdio():
    for stream in (sys.stdout, sys.stderr):
        if hasattr(stream, "reconfigure"):
            stream.reconfigure(encoding="utf-8", errors="replace")


def short_text(text, limit=280):
    text = re.sub(r"\s+", " ", clean_text(text)).strip()
    return text if len(text) <= limit else text[:limit].rstrip() + "…"


def digest_timezone(data):
    return (data.get("config") or {}).get("timezone") or "Asia/Shanghai"


def format_source_time(value, timezone_name):
    if not value:
        return "未验证"
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return "未验证"
    if parsed.tzinfo is None:
        return "未验证"
    try:
        target_timezone = ZoneInfo(timezone_name)
        label = "北京时间" if timezone_name == "Asia/Shanghai" else timezone_name
    except ZoneInfoNotFoundError:
        target_timezone = timezone.utc
        label = "UTC"
    return f"{parsed.astimezone(target_timezone):%Y-%m-%d %H:%M}（{label}）"


def is_noise_tweet(text):
    normalized = re.sub(r"\s+", " ", (text or "").lower()).strip()
    if len(normalized) < 25 and not re.search(r"https?://|@\w+", normalized):
        return True
    return any(re.search(pattern, normalized) for pattern in NOISE_PATTERNS)


def evidence_label(item, parent=None):
    evidence_class = item.get("evidence_class") or (parent or {}).get("evidence_class")
    return EVIDENCE_LABELS.get(evidence_class, "来源")


def classify(text):
    normalized = f" {str(text or '').lower()} "
    for section_id, _heading, keywords in SECTIONS:
        if any(keyword.lower() in normalized for keyword in keywords):
            return section_id
    return "brand_product"


def selected_tweets(data):
    return [
        (account, tweet)
        for account in data.get("x") or []
        for tweet in account.get("tweets", [])
        if not is_noise_tweet(tweet.get("text", ""))
    ]


def selected_articles(data):
    return [article for article in data.get("articles") or [] if article.get("title") or article.get("url")]


def event_consumer_items(data):
    """Render one primary item per prepared cross-source event group."""
    raw_by_event = {}
    for article in selected_articles(data):
        if article.get("event_id"):
            raw_by_event[article["event_id"]] = article
    for account, tweet in selected_tweets(data):
        if tweet.get("event_id"):
            raw_by_event[tweet["event_id"]] = {**tweet, "_parent": account}

    items = []
    for event in data.get("event_groups") or []:
        primary = event.get("primary") or {}
        raw = raw_by_event.get(event.get("id"), {})
        body = raw.get("summary") or raw.get("description") or raw.get("text") or ""
        item = {
            "section": classify(f"{event.get('title', '')} {body}"),
            "evidence": evidence_label(primary),
            "source": primary.get("source") or "Web",
            "title": event.get("title") or primary.get("title") or "未命名事件",
            "body": short_text(body),
            "url": primary.get("url") or raw.get("url") or "",
            "time": primary.get("timestamp") or raw.get("published") or raw.get("created_at"),
            "time_label": "来源时间",
            "supporting_sources": event.get("supporting_sources") or [],
        }
        items.append(item)
    return items


def consumer_items(data):
    if data.get("event_groups") is not None:
        return event_consumer_items(data)

    items = []
    for article in selected_articles(data):
        items.append(
            {
                "section": classify(" ".join([article.get("title", ""), article.get("summary", ""), article.get("description", "")])),
                "evidence": evidence_label(article),
                "source": article.get("source_name") or article.get("source") or "Web",
                "title": article.get("title") or "未命名文章",
                "body": short_text(article.get("summary") or article.get("description", "")),
                "url": article.get("url", ""),
                "time": article.get("published"),
                "time_label": "来源时间",
            }
        )
    for account, tweet in selected_tweets(data):
        items.append(
            {
                "section": classify(tweet.get("text", "")),
                "evidence": evidence_label(tweet, account),
                "source": account.get("name") or account.get("handle") or "X",
                "title": "X 信号",
                "body": short_text(tweet.get("text", "")),
                "url": tweet.get("url", ""),
                "time": tweet.get("created_at"),
                "time_label": "发布时间",
            }
        )
    return items


def render_consumer_sections(data, lines):
    timezone_name = digest_timezone(data)
    grouped = {section_id: [] for section_id, _heading, _keywords in SECTIONS}
    for item in consumer_items(data):
        grouped[item["section"]].append(item)

    for section_id, heading, _keywords in SECTIONS:
        items = grouped[section_id]
        if not items:
            continue
        lines.append(f"## {heading}")
        for item in items:
            lines.append(f"### [{item['evidence']}] {item['source']}｜{item['title']}")
            lines.append(f"{item['time_label']}：{format_source_time(item['time'], timezone_name)}")
            if item["body"]:
                lines.append(item["body"])
            if item["url"]:
                lines.append(f"来源：{item['url']}")
            supporting_sources = item.get("supporting_sources") or []
            if supporting_sources:
                links = []
                for source in supporting_sources:
                    label = source.get("source") or "佐证来源"
                    url = source.get("url") or ""
                    links.append(f"{label}：{url}" if url else label)
                lines.append("佐证：" + "；".join(links))
            lines.append("")


def render_tweets(data, lines):
    """Compatibility helper for direct rendering/tests of X items."""
    timezone_name = digest_timezone(data)
    tweets = selected_tweets(data)
    if not tweets:
        return
    lines.append("## X 信号")
    for account, tweet in tweets:
        source = account.get("name") or account.get("handle") or "X"
        lines.append(f"- [{evidence_label(tweet, account)}] {source}：{short_text(tweet.get('text', ''))}")
        lines.append(f"  发布时间：{format_source_time(tweet.get('created_at'), timezone_name)}")
        if tweet.get("url"):
            lines.append(f"  来源：{tweet['url']}")
    lines.append("")


def render_podcasts(data, lines):
    """Optional future source type; disabled in the default source profile."""
    timezone_name = digest_timezone(data)
    podcasts = data.get("podcasts") or []
    if not podcasts:
        return
    lines.append("## 补充音频")
    for item in podcasts:
        lines.append(f"- {item.get('channel', '')}｜{item.get('title', 'Untitled')}")
        lines.append(f"  发布时间：{format_source_time(item.get('pub_date'), timezone_name)}")
    lines.append("")


def render_papers(data, lines):
    """Optional future research source type; disabled in the default profile."""
    timezone_name = digest_timezone(data)
    papers = data.get("papers") or []
    if not papers:
        return
    lines.append("## 补充研究")
    for item in papers:
        lines.append(f"- {item.get('title', 'Untitled')}")
        lines.append(f"  首次提交：{format_source_time(item.get('published'), timezone_name)}")
    lines.append("")


def main():
    configure_stdio()
    raw = sys.stdin.read()
    if not raw.strip():
        raise SystemExit("No input JSON")
    data = json.loads(raw)
    cfg = data.get("config") or {}
    now = datetime.now().strftime("%Y-%m-%d")
    articles = len(selected_articles(data))
    tweets = len(selected_tweets(data))
    events = len(data.get("event_groups") or [])
    lines = [
        f"# Consumer Signal 日报 - {now}",
        "",
        f"语言：{cfg.get('language', 'zh')} | 详细度：{cfg.get('granularity', 'summary')}",
        "",
        (
            f"本次候选：{events} 个行业事件（来自网页文章 {articles} 条，X 信号 {tweets} 条）。"
            if data.get("event_groups") is not None
            else f"本次候选：网页文章 {articles} 条，X 信号 {tweets} 条。"
        ),
        "",
    ]
    if data.get("errors"):
        lines.extend(["> 非致命提示：" + "; ".join(data["errors"]), ""])
    render_consumer_sections(data, lines)
    if len(lines) == 6 or (len(lines) == 8 and data.get("errors")):
        lines.append("今天暂时没有可展示的新消费电子信号。")
    sys.stdout.write(clean_text("\n".join(line.rstrip() for line in lines)).strip() + "\n")


if __name__ == "__main__":
    main()
