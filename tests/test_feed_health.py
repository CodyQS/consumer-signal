import json
import sys
import tempfile
import unittest
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT_DIR / "scripts"))

import report_feed_health


class FeedHealthTests(unittest.TestCase):
    def test_reports_only_degraded_feed_types(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            for filename in report_feed_health.FEED_FILES.values():
                (root / filename).write_text(json.dumps({"profile": "consumer_electronics"}), "utf-8")
            (root / "feed-x.json").write_text(
                json.dumps({"degraded": True, "errors": ["Twitter/X: session expired"]}), "utf-8"
            )

            issues = report_feed_health.degraded_feeds(root)

        self.assertEqual(issues, [("X", "Twitter/X: session expired")])

    def test_escapes_github_annotation_control_characters(self):
        self.assertEqual(report_feed_health.github_escape("a%\nb\r"), "a%25%0Ab%0D")


if __name__ == "__main__":
    unittest.main()
