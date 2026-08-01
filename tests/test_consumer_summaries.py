import json
import sys
import tempfile
import unittest
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT_DIR / "scripts"))

import generate_summaries


class ConsumerSummaryTaskTests(unittest.TestCase):
    def setUp(self):
        self.tmp_dir = tempfile.TemporaryDirectory()
        self.tmp_path = Path(self.tmp_dir.name)
        self.profile = {
            "language": "zh",
            "domains": ["consumer_electronics"],
            "include_x": True,
            "include_articles": True,
        }
        self.cfg = {
            "llm": {"model": "test-model"},
            "output": {"dir": "content/summaries"},
            "x": {"enabled": True, "input_path": str(self.tmp_path / "feed-x.json")},
            "articles": {"enabled": True, "input_path": str(self.tmp_path / "feed-blogs.json")},
        }

    def tearDown(self):
        self.tmp_dir.cleanup()

    def write_feed(self, filename, data):
        (self.tmp_path / filename).write_text(json.dumps(data), encoding="utf-8")

    def test_only_consumer_profile_feeds_produce_x_and_article_tasks(self):
        self.write_feed(
            "feed-x.json",
            {
                "profile": "consumer_electronics",
                "x": [{"handle": "DSCCRoss", "domain": "consumer_electronics", "evidence_class": "early_signal", "tweets": [{"id": "x1", "text": "OLED panel orders for AI glasses are ramping.", "url": "https://x.com/example/status/1"}]}],
            },
        )
        self.write_feed(
            "feed-blogs.json",
            {
                "profile": "consumer_electronics",
                "articles": [{"id": "a1", "domain": "consumer_electronics", "source_name": "IDC", "evidence_class": "market_data", "title": "Smartphone shipments recover", "url": "https://example.com/a1"}],
            },
        )

        x_tasks = generate_summaries.x_tasks(self.cfg, "zh_standard", self.profile, {"profiles": {}}, None)
        article_tasks = generate_summaries.article_tasks(self.cfg, "zh_standard", self.profile, {"profiles": {}}, None)

        self.assertEqual(len(x_tasks), 1)
        self.assertEqual(len(article_tasks), 1)
        self.assertIn("[线索/传闻]", generate_summaries.build_x_prompt(x_tasks[0]["item"], self.profile))
        self.assertIn("[数据/研究]", generate_summaries.build_article_prompt(article_tasks[0]["item"], self.profile))

    def test_legacy_feed_without_profile_is_not_summarized(self):
        self.write_feed("feed-x.json", {"x": [{"tweets": [{"id": "old", "text": "old cached item"}]}]})
        self.write_feed("feed-blogs.json", {"articles": [{"id": "old", "title": "old cached item"}]})

        self.assertEqual(
            generate_summaries.x_tasks(self.cfg, "zh_standard", self.profile, {"profiles": {}}, None), []
        )
        self.assertEqual(
            generate_summaries.article_tasks(self.cfg, "zh_standard", self.profile, {"profiles": {}}, None), []
        )


if __name__ == "__main__":
    unittest.main()
