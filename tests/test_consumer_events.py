import sys
import unittest
import json
from datetime import datetime, timezone
from pathlib import Path
from unittest import mock


ROOT_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT_DIR / "scripts"))

import event_dedup
import generate_feed
import prepare_digest
import render_digest


class ConsumerEventDedupTests(unittest.TestCase):
    def test_merges_x_and_web_coverage_and_prefers_stronger_evidence(self):
        accounts = [
            {
                "handle": "source",
                "name": "Supply-chain source",
                "evidence_class": "early_signal",
                "tweets": [
                    {
                        "id": "x-1",
                        "text": "Apple to launch iPhone 17 Pro next week",
                        "url": "https://x.com/source/status/1",
                        "created_at": "2026-08-01T01:00:00+00:00",
                    }
                ],
            }
        ]
        articles = [
            {
                "id": "a-1",
                "source_name": "Apple Newsroom",
                "evidence_class": "primary_disclosure",
                "title": "Apple announces iPhone 17 Pro",
                "summary": "The new iPhone is available next week.",
                "url": "https://example.com/iphone-17-pro",
                "published": "2026-08-01T02:00:00+00:00",
            }
        ]

        deduped_accounts, deduped_articles, events, delivered_ids = event_dedup.dedupe_digest_events(
            accounts, articles
        )

        self.assertEqual(deduped_accounts[0]["tweets"], [])
        self.assertEqual(len(deduped_articles), 1)
        self.assertEqual(events[0]["primary"]["source"], "Apple Newsroom")
        self.assertEqual(events[0]["source_count"], 2)
        self.assertEqual(delivered_ids["tweets"], {"x-1"})
        self.assertEqual(delivered_ids["articles"], {"a-1"})

    def test_does_not_merge_a_launch_and_an_independent_review(self):
        articles = [
            {
                "id": "launch",
                "source_name": "Apple Newsroom",
                "evidence_class": "primary_disclosure",
                "title": "Apple announces iPhone 17 Pro",
                "url": "https://example.com/launch",
                "published": "2026-08-01T02:00:00+00:00",
            },
            {
                "id": "review",
                "source_name": "Reviewer",
                "evidence_class": "review_feedback",
                "title": "iPhone 17 Pro camera review",
                "url": "https://example.com/review",
                "published": "2026-08-01T03:00:00+00:00",
            },
        ]

        _accounts, deduped_articles, events, _ids = event_dedup.dedupe_digest_events([], articles)

        self.assertEqual(len(deduped_articles), 2)
        self.assertEqual(len(events), 2)

    def test_fallback_renderer_uses_one_event_and_shows_corroboration(self):
        data = {
            "config": {"timezone": "Asia/Shanghai"},
            "articles": [
                {
                    "event_id": "event-1",
                    "title": "Apple announces iPhone 17 Pro",
                    "summary": "The new phone is available next week.",
                }
            ],
            "x": [],
            "event_groups": [
                {
                    "id": "event-1",
                    "title": "Apple announces iPhone 17 Pro",
                    "primary": {
                        "source": "Apple Newsroom",
                        "url": "https://example.com/launch",
                        "timestamp": "2026-08-01T02:00:00+00:00",
                        "evidence_class": "primary_disclosure",
                    },
                    "supporting_sources": [
                        {"source": "Supply-chain source", "url": "https://x.com/source/status/1"}
                    ],
                }
            ],
        }

        lines = []
        render_digest.render_consumer_sections(data, lines)
        rendered = "\n".join(lines)

        self.assertEqual(rendered.count("Apple announces iPhone 17 Pro"), 1)
        self.assertIn("佐证：Supply-chain source：https://x.com/source/status/1", rendered)


class FeedResilienceTests(unittest.TestCase):
    def test_keeps_last_successful_feed_and_marks_it_degraded(self):
        attempted_at = datetime(2026, 8, 1, 8, tzinfo=timezone.utc)
        existing = {
            "profile": "consumer_electronics",
            "generated_at": "2026-08-01T01:00:00+00:00",
            "x": [{"handle": "source", "tweets": [{"id": "x-1"}]}],
        }

        with mock.patch.object(generate_feed, "load_feed", return_value=existing):
            feed, used_cache = generate_feed.cached_feed_on_failure(
                "feed-x.json", "x", "consumer_electronics", "Twitter/X: session expired", attempted_at
            )

        feed = generate_feed.finalize_feed(feed, "consumer_electronics", attempted_at, used_cache)
        self.assertTrue(used_cache)
        self.assertEqual(feed["generated_at"], "2026-08-01T01:00:00+00:00")
        self.assertTrue(feed["degraded"])
        self.assertEqual(feed["attempted_at"], "2026-08-01T08:00:00+00:00")
        self.assertIn("session expired", feed["errors"][0])

    def test_empty_error_result_uses_compatible_cache(self):
        attempted_at = datetime(2026, 8, 1, 8, tzinfo=timezone.utc)
        existing = {
            "profile": "consumer_electronics",
            "generated_at": "2026-08-01T01:00:00+00:00",
            "articles": [{"id": "a-1"}],
        }
        result = {"articles": [], "errors": ["all public sources timed out"]}

        with mock.patch.object(generate_feed, "load_feed", return_value=existing):
            feed, used_cache = generate_feed.preserve_on_empty_error(
                result, "feed-blogs.json", "articles", "consumer_electronics", attempted_at
            )

        self.assertTrue(used_cache)
        self.assertEqual(feed["articles"], [{"id": "a-1"}])
        self.assertTrue(feed["degraded"])

    def test_client_surfaces_degraded_source_errors(self):
        sources, warnings = prepare_digest.annotate_feed_sources(
            {"blogs": {"source": "remote", "filename": "feed-blogs.json", "url": "", "generated_at": "now"}},
            {"blogs": {"generated_at": "2026-08-01T01:00:00+00:00", "errors": ["CINNO: timed out"]}},
        )

        self.assertTrue(sources["blogs"]["degraded"])
        self.assertIn("CINNO: timed out", warnings[0])


class SourceCatalogIntegrityTests(unittest.TestCase):
    def test_every_enabled_web_source_is_marked_active_and_verified_in_catalog(self):
        sources = json.loads((ROOT_DIR / "config" / "sources.json").read_text("utf-8"))
        catalog = json.loads(
            (ROOT_DIR / "config" / "source-catalog.consumer-electronics.json").read_text("utf-8")
        )
        catalog_sources = {
            entry["active_source_id"]: entry
            for layer in catalog["source_layers"]
            for entry in layer["sources"]
            if entry.get("active_source_id")
        }
        enabled_ids = {
            entry["id"] for entry in sources["blogs"]["sources"]
            if entry.get("enabled", True) is not False
        }

        self.assertTrue(enabled_ids <= set(catalog_sources))
        for source_id in enabled_ids:
            self.assertTrue(catalog_sources[source_id]["ingestion"]["status"].startswith("verified_"))


if __name__ == "__main__":
    unittest.main()
