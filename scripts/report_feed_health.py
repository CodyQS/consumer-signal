"""Emit GitHub Actions annotations for degraded Consumer Signal feeds."""

import json
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parent.parent
FEEDS_DIR = ROOT_DIR / "feeds"
FEED_FILES = {
    "X": "feed-x.json",
    "网页": "feed-blogs.json",
    "播客": "feed-podcasts.json",
    "研究": "feed-arxiv.json",
}


def degraded_feeds(feeds_dir=FEEDS_DIR):
    issues = []
    for label, filename in FEED_FILES.items():
        path = Path(feeds_dir) / filename
        try:
            feed = json.loads(path.read_text("utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            issues.append((label, f"无法读取 {filename}: {exc}"))
            continue
        errors = [str(error) for error in (feed.get("errors") or [])]
        if feed.get("degraded") or errors:
            detail = " | ".join(errors[:3]) or "采集结果已降级；请检查 attempted_at 和 source 状态"
            issues.append((label, detail))
    return issues


def github_escape(value):
    return str(value).replace("%", "%25").replace("\r", "%0D").replace("\n", "%0A")


def main():
    issues = degraded_feeds()
    if not issues:
        print("Consumer Signal source health: all feed types healthy")
        return
    for label, detail in issues:
        print(f"::warning title=Consumer Signal {github_escape(label)} 来源降级::{github_escape(detail)}")
    print(f"Consumer Signal source health: {len(issues)} degraded feed type(s)")


if __name__ == "__main__":
    main()
