import json
import sys
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest import mock


ROOT_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT_DIR / "scripts"))

import generate_feed
import prepare_digest


def load_json_without_duplicate_keys(path):
    def reject_duplicate_keys(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise ValueError(f"duplicate JSON key: {key}")
            result[key] = value
        return result

    return json.loads(path.read_text("utf-8"), object_pairs_hook=reject_duplicate_keys)


class ConsumerConfigurationTests(unittest.TestCase):
    def test_active_source_configuration_matches_verified_catalog_entries(self):
        sources = load_json_without_duplicate_keys(ROOT_DIR / "config" / "sources.json")
        catalog = load_json_without_duplicate_keys(
            ROOT_DIR / "config" / "source-catalog.consumer-electronics.json"
        )
        catalog_sources = [
            source
            for layer in catalog["source_layers"]
            for source in layer.get("sources", [])
        ]
        by_id = {source["id"]: source for source in catalog_sources}
        by_handle = {
            source["ingestion"]["handle"].casefold(): source
            for source in catalog_sources
            if source.get("ingestion", {}).get("handle")
        }

        self.assertEqual(sources["twitter"]["max_tweets_per_user"], None)
        self.assertEqual(sources["twitter"]["min_engagement"], None)
        cninfo_source = next(
            source for source in sources["blogs"]["sources"]
            if source["id"] == "cninfo_consumer_supply_chain"
        )
        self.assertTrue(
            {
                "立讯精密", "歌尔股份", "欧菲光", "环旭电子", "瑞芯微", "欣旺达", "传音控股"
            }.issubset(cninfo_source["companies"])
        )
        for account in sources["twitter"]["accounts"]:
            catalog_source = by_handle.get(account["handle"].casefold())
            self.assertIsNotNone(catalog_source, account["handle"])
            self.assertTrue(
                catalog_source["ingestion"]["status"].startswith("verified_"),
                account["handle"],
            )

        for source in sources["blogs"]["sources"]:
            if source.get("enabled", True) is False:
                continue
            catalog_source = by_id.get(source["id"])
            if catalog_source is None:
                catalog_source = next(
                    (
                        item
                        for item in catalog_sources
                        if item.get("active_source_id") == source["id"]
                    ),
                    None,
                )
            self.assertIsNotNone(catalog_source, source["id"])
            self.assertTrue(
                catalog_source["ingestion"]["status"].startswith("verified_"),
                source["id"],
            )


class ConsumerFilterTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.content_filter = generate_feed.load_content_filter(
            {"filtering": {"config_path": "filter-terms.consumer-electronics.json"}},
            "twitter",
        )

    def assertMatches(self, text):
        self.assertTrue(generate_feed.is_relevant_content(text, self.content_filter), text)

    def assertDoesNotMatch(self, text):
        self.assertFalse(generate_feed.is_relevant_content(text, self.content_filter), text)

    def test_keeps_terminal_product_launch(self):
        self.assertMatches(
            "Apple will launch the next iPhone with a revised price and new camera system this fall."
        )

    def test_keeps_china_supply_chain_signal(self):
        self.assertMatches(
            "立讯精密开始为苹果新机备货，供应链预计在下季度进入量产爬坡阶段。"
        )

    def test_keeps_traditional_chinese_product_and_supply_signal(self):
        self.assertMatches(
            "折疊 iPhone 可能同場發布、較晚開賣，供應吃緊將延續至年底。"
        )

    def test_keeps_priority_ai_terminal_phrase(self):
        self.assertMatches(
            "The AI glasses category is moving from prototypes to retail products with a clearer use case."
        )

    def test_drops_brand_only_market_chatter(self):
        self.assertDoesNotMatch(
            "Apple is one of the most discussed companies in markets today, according to traders."
        )

    def test_drops_stock_target_even_when_a_terminal_brand_is_named(self):
        self.assertDoesNotMatch(
            "JP Morgan cuts its Apple price target after reviewing the latest iPhone demand data."
        )

    def test_drops_generic_ai_and_cloud(self):
        self.assertDoesNotMatch(
            "The AI agent improves enterprise software workflows on a large cloud computing platform."
        )

    def test_drops_cloud_capex_that_only_uses_hinge_as_a_verb(self):
        self.assertDoesNotMatch(
            "Cloud service provider capex hinges on AI monetization and data center demand."
        )

    def test_drops_giveaway_even_when_a_device_is_named(self):
        self.assertDoesNotMatch(
            "Enter our giveaway to win a new iPhone; use the affiliate link and discount code today."
        )

    def test_drops_automotive_even_when_a_terminal_brand_is_named(self):
        self.assertDoesNotMatch(
            "小米新能源汽车开启预售，这款 SUV 面向家庭用户并公布了续航和定价。"
        )

    def test_source_level_exclusion_handles_ambiguous_auto_model_names(self):
        article = {"title": "小米澎程 N70 Max 开始预售", "summary": "面向家庭用户"}
        source = {"exclude_keywords": ["澎程", "理想", "问界"]}
        self.assertTrue(generate_feed.source_excludes_content(article, source))

    def test_source_priority_signal_admits_vetted_teardown_analysis(self):
        article = {
            "title": "The Secrets Behind Samsung's Privacy Screen",
            "summary": "We tear down the Galaxy S26 Ultra to inspect its OLED privacy screen.",
        }
        source = {"priority_keywords": ["teardown", "tear down", "repairability"]}
        self.assertTrue(generate_feed.source_has_priority_signal(article, source))

    def test_can_disable_filter_for_a_content_type(self):
        disabled = generate_feed.load_content_filter(
            {
                "filtering": {
                    "config_path": "filter-terms.consumer-electronics.json",
                    "apply_to": ["twitter"],
                }
            },
            "blogs",
        )
        self.assertIsNone(disabled)

    def test_legacy_twitter_filter_still_works_without_profile(self):
        self.assertTrue(generate_feed.is_relevant_tweet("AI product announcement", {}))


class JsonLdListingTests(unittest.TestCase):
    def test_parses_recent_newsarticle_and_preserves_coverage_metadata(self):
        published = datetime.now(timezone.utc).isoformat()
        html = f'''<script type="application/ld+json">{{
          "@context": "https://schema.org",
          "@graph": [{{
            "@type": "ItemList",
            "itemListElement": [{{"@type": "ListItem", "item": {{
              "@type": "NewsArticle",
              "url": "https://example.com/article",
              "headline": "联发科公布手机芯片出货与下季指引",
              "description": "手机芯片供应链的订单和出货量出现边际变化。",
              "datePublished": "{published}"
            }}}}]
          }}]
        }}</script>'''
        source = {
            "id": "economic_daily_industry",
            "name": "经济日报（台湾）产业",
            "region": "taiwan_asia",
            "evidence_class": "industry_reporting",
            "source_layer": "supply_chain_reporting",
        }

        items = generate_feed.blog_items_from_json_ld_listing(
            html, source, datetime.now(timezone.utc) - timedelta(hours=48)
        )

        self.assertEqual(len(items), 1)
        self.assertEqual(items[0]["region"], "taiwan_asia")

    def test_can_disable_feed_types(self):
        self.assertFalse(generate_feed.source_enabled({"arxiv": {"enabled": False}}, "arxiv"))
        self.assertTrue(generate_feed.source_enabled({}, "blogs"))

    def test_parses_atom_video_feed_with_source_metadata(self):
        published = datetime.now(timezone.utc).isoformat()
        atom = f'''<feed xmlns="http://www.w3.org/2005/Atom">
          <entry>
            <title>极客湾手机续航大横评</title>
            <link rel="alternate" href="https://www.youtube.com/watch?v=example" />
            <published>{published}</published>
            <summary>手机芯片性能、功耗和续航分析。</summary>
          </entry>
        </feed>'''
        source = {
            "id": "geekerwan",
            "name": "极客湾 Geekerwan",
            "region": "china",
            "evidence_class": "review_feedback",
            "source_layer": "engineering_reviews",
        }

        items = generate_feed.blog_items_from_rss(
            atom, source, datetime.now(timezone.utc) - timedelta(hours=48)
        )

        self.assertEqual(len(items), 1)
        self.assertEqual(items[0]["url"], "https://www.youtube.com/watch?v=example")
        self.assertEqual(items[0]["source_layer"], "engineering_reviews")

    def test_parses_public_cinno_listing_with_dates_and_metadata(self):
        today = datetime.now(timezone.utc)
        html = f'''<div class="news_list"><ul><li>
          <a href="/industry/news/q1-ai-ar"><h2>{today.strftime("%d %B %Y")}</h2>
          <p class="wx_num">Q1’26消费级 AI/AR 市场销量持续增长</p></a>
        </li></ul></div>'''
        source = {
            "id": "cinno_research",
            "name": "CINNO Research",
            "url": "https://www.cinno.com.cn/industry/insights",
            "region": "china",
            "evidence_class": "market_data",
            "source_layer": "market_data",
        }

        source["fetch_article_summary"] = True
        with mock.patch(
            "generate_feed.cinno_article_summary",
            return_value="AI/AR 终端市场销量与供应链需求持续增长。",
        ):
            items = generate_feed.blog_items_from_cinno_listing(
                html, source, datetime.now(timezone.utc) - timedelta(hours=48)
            )

        self.assertEqual(len(items), 1)
        self.assertEqual(items[0]["url"], "https://www.cinno.com.cn/industry/news/q1-ai-ar")
        self.assertEqual(items[0]["evidence_class"], "market_data")
        self.assertEqual(items[0]["summary"], "AI/AR 终端市场销量与供应链需求持续增长。")

    def test_parses_google_devices_listing_with_public_card_metadata(self):
        today = datetime.now(timezone.utc)
        html = f'''<a href="https://blog.google/products-and-platforms/devices/pixel/example/"
          class="uni-nup__article"
          data-ga4-analytics-lead-click='{{"publish_date":"{today:%Y-%m-%d|%H:%M}","primary_tag":"Products - Pixel"}}'>
          <h3 class="uni-nup__header uni-title-1">Google announces a new Pixel device</h3>
        </a>'''
        source = {
            "id": "google_devices_blog",
            "name": "Google Blog: Devices",
            "url": "https://blog.google/products-and-platforms/devices/",
            "region": "global",
            "evidence_class": "primary_disclosure",
            "source_layer": "official_brand",
        }

        items = generate_feed.blog_items_from_google_devices_listing(
            html, source, today - timedelta(hours=48)
        )

        self.assertEqual(len(items), 1)
        self.assertEqual(items[0]["url"], "https://blog.google/products-and-platforms/devices/pixel/example/")
        self.assertEqual(items[0]["title"], "Google announces a new Pixel device")
        self.assertIn("Products - Pixel", items[0]["summary"])

    def test_parses_public_counterpoint_listing_with_dates_and_metadata(self):
        today = datetime.now(timezone.utc)
        html = f'''<a class="block" href="/en/insights/global-smartphone-shipments">
          <div><h3>Global Smartphone Shipments Fall as Memory Costs Rise</h3>
          <p>{today.strftime("%B %d, %Y")}</p></div></a>'''
        source = {
            "id": "counterpoint_insights",
            "name": "Counterpoint Research Insights",
            "url": "https://counterpointresearch.com/en/insights",
            "region": "global",
            "evidence_class": "market_data",
            "source_layer": "market_data",
        }

        items = generate_feed.blog_items_from_counterpoint_listing(
            html, source, datetime.now(timezone.utc) - timedelta(hours=48)
        )

        self.assertEqual(len(items), 1)
        self.assertEqual(
            items[0]["url"],
            "https://counterpointresearch.com/en/insights/global-smartphone-shipments",
        )
        self.assertEqual(items[0]["source_layer"], "market_data")

    def test_parses_vivo_server_rendered_listing_with_location_prefixed_date(self):
        today = datetime.now(timezone.utc)
        html = f'''<ul><li class="list-container-item delay-list">
          <a class="list-container-link" href="/en/about-vivo/news/vivo-x200-launch">
            <p class="list-container-title">vivo Launches X200 with New Imaging System</p>
            <span class="list-container-time">SHENZHEN, China, {today.strftime("%B %-d, %Y")}</span>
          </a>
        </li></ul>'''
        source = {
            "id": "vivo_newsroom",
            "name": "vivo Global Newsroom",
            "url": "https://www.vivo.com/en/about-vivo/news",
            "region": "china",
            "evidence_class": "primary_disclosure",
            "source_layer": "official_brand",
        }

        items = generate_feed.blog_items_from_vivo_listing(
            html, source, datetime.now(timezone.utc) - timedelta(hours=48)
        )

        self.assertEqual(len(items), 1)
        self.assertEqual(
            items[0]["url"], "https://www.vivo.com/en/about-vivo/news/vivo-x200-launch"
        )
        self.assertEqual(items[0]["region"], "china")

    def test_parses_oppo_public_listing_api_response(self):
        published = datetime.now(timezone.utc)
        payload = {
            "code": "0",
            "data": {
                "rows": [
                    {
                        "pageUrl": "https://www.oppo.com/en/newsroom/press/oppo-find-x-launch/",
                        "title": "OPPO Launches Find X with New AI Imaging Platform",
                        "publishTime": int(published.timestamp()),
                        "description": "The new smartphone brings on-device AI imaging to the flagship series.",
                    }
                ]
            },
        }
        source = {
            "id": "oppo_newsroom",
            "name": "OPPO Global Press Releases",
            "region": "china",
            "evidence_class": "primary_disclosure",
            "source_layer": "official_brand",
        }

        items = generate_feed.blog_items_from_oppo_listing(
            payload, source, datetime.now(timezone.utc) - timedelta(hours=48)
        )

        self.assertEqual(len(items), 1)
        self.assertEqual(items[0]["title"], "OPPO Launches Find X with New AI Imaging Platform")
        self.assertEqual(items[0]["evidence_class"], "primary_disclosure")

    def test_parses_xiaomi_public_listing_api_response(self):
        published = datetime.now(timezone.utc)
        payload = {
            "error": 0,
            "data": {
                "total_pages": 1,
                "list": [
                    {
                        "material_id": "6201",
                        "material_type": "newsroom",
                        "title": "Xiaomi Unveils New AI Tablet and Smart Wearable Devices",
                        "desc": "The new devices bring on-device AI and upgraded displays to consumers.",
                        "add_time": int(published.timestamp()),
                    },
                    {
                        "material_id": "video-1",
                        "material_type": "video",
                        "title": "Xiaomi short video",
                        "add_time": int(published.timestamp()),
                    },
                ],
            },
        }
        source = {
            "id": "xiaomi_global_discover",
            "name": "Xiaomi Global Discover / Newsroom",
            "region": "china",
            "evidence_class": "primary_disclosure",
            "source_layer": "official_brand",
            "material_types": ["newsroom"],
        }

        items = generate_feed.blog_items_from_xiaomi_listing(
            payload, source, datetime.now(timezone.utc) - timedelta(hours=48)
        )

        self.assertEqual(len(items), 1)
        self.assertEqual(
            items[0]["url"], "https://www.mi.com/global/discover/article?id=6201"
        )
        self.assertIn("on-device AI", items[0]["summary"])

    def test_xiaomi_listing_fetches_pages_until_an_entire_page_is_old(self):
        now = datetime.now(timezone.utc)
        recent_payload = {
            "data": {
                "total_pages": 3,
                "list": [
                    {
                        "material_id": "recent",
                        "material_type": "newsroom",
                        "title": "Xiaomi Launches New AI Glasses",
                        "desc": "A new wearable product launch.",
                        "add_time": int(now.timestamp()),
                    }
                ],
            }
        }
        old_payload = {
            "data": {
                "total_pages": 3,
                "list": [
                    {
                        "material_id": "old",
                        "material_type": "newsroom",
                        "title": "Xiaomi Announces an Earlier Smartphone",
                        "desc": "An older product launch.",
                        "add_time": int((now - timedelta(days=3)).timestamp()),
                    }
                ],
            }
        }
        recent_response = mock.Mock()
        recent_response.json.return_value = recent_payload
        recent_response.raise_for_status.return_value = None
        old_response = mock.Mock()
        old_response.json.return_value = old_payload
        old_response.raise_for_status.return_value = None
        source = {
            "id": "xiaomi_global_discover",
            "api_url": "https://example.com/xiaomi-api",
            "api_params": {"from": "pc"},
            "material_types": ["newsroom"],
        }

        with mock.patch("generate_feed.httpx.get", side_effect=[recent_response, old_response]) as get:
            items = generate_feed.fetch_xiaomi_listing(source, now - timedelta(hours=48))

        self.assertEqual(len(items), 1)
        self.assertEqual(get.call_count, 2)
        self.assertEqual(get.call_args_list[0].kwargs["params"], {"from": "pc", "page_num": 1})
        self.assertEqual(get.call_args_list[1].kwargs["params"], {"from": "pc", "page_num": 2})

    def test_xiaomi_listing_stops_when_api_repeats_a_page(self):
        now = datetime.now(timezone.utc)
        payload = {
            "data": {
                "total_pages": 68,
                "list": [
                    {
                        "material_id": "recent",
                        "material_type": "newsroom",
                        "title": "Xiaomi Launches New AI Glasses",
                        "desc": "A new wearable product launch.",
                        "add_time": int(now.timestamp()),
                    }
                ],
            }
        }
        response = mock.Mock()
        response.json.return_value = payload
        response.raise_for_status.return_value = None
        source = {
            "id": "xiaomi_global_discover",
            "api_url": "https://example.com/xiaomi-api",
            "material_types": ["newsroom"],
        }

        with mock.patch("generate_feed.httpx.get", return_value=response) as get:
            items = generate_feed.fetch_xiaomi_listing(source, now - timedelta(hours=48))

        self.assertEqual(len(items), 1)
        self.assertEqual(get.call_count, 2)

    def test_parses_qualcomm_public_graphql_listing_response(self):
        published = datetime.now(timezone.utc)
        payload = {
            "data": {
                "newsFinder": {
                    "resources": [
                        {
                            "id": "qualcomm-ai-glasses",
                            "title": "Qualcomm Launches Snapdragon Platform for AI Glasses",
                            "link": "/news/releases/2026/08/qualcomm-ai-glasses",
                            "field_publish_date": [str(int(published.timestamp()))],
                            "field_content_tags_name": ["Snapdragon", "XR", "AI"],
                        }
                    ]
                }
            }
        }
        source = {
            "id": "qualcomm_releases",
            "name": "Qualcomm Press Announcements",
            "url": "https://www.qualcomm.com/news/releases",
            "region": "global",
            "evidence_class": "primary_disclosure",
            "source_layer": "component_supplier",
        }

        items = generate_feed.blog_items_from_qualcomm_listing(
            payload, source, datetime.now(timezone.utc) - timedelta(hours=48)
        )

        self.assertEqual(len(items), 1)
        self.assertEqual(
            items[0]["url"],
            "https://www.qualcomm.com/news/releases/2026/08/qualcomm-ai-glasses",
        )
        self.assertIn("Official tags: Snapdragon, XR, AI.", items[0]["summary"])

    def test_qualcomm_listing_stops_at_the_first_page_that_crosses_lookback(self):
        now = datetime.now(timezone.utc)
        payload = {
            "data": {
                "newsFinder": {
                    "resources": [
                        {
                            "id": "fresh",
                            "title": "Qualcomm Announces Snapdragon Platform for AI PCs",
                            "link": "/news/fresh",
                            "field_publish_date": [str(int(now.timestamp()))],
                        },
                        {
                            "id": "old",
                            "title": "Qualcomm Announces Earlier Mobile Platform",
                            "link": "/news/old",
                            "field_publish_date": [str(int((now - timedelta(days=3)).timestamp()))],
                        },
                    ]
                }
            }
        }
        response = mock.Mock()
        response.json.return_value = payload
        response.raise_for_status.return_value = None
        source = {
            "id": "qualcomm_releases",
            "url": "https://www.qualcomm.com/news/releases",
            "graphql_url": "https://example.com/qualcomm-graphql",
            "page_size": 2,
        }

        with mock.patch("generate_feed.httpx.post", return_value=response) as post:
            items = generate_feed.fetch_qualcomm_listing(source, now - timedelta(hours=48))

        self.assertEqual(len(items), 1)
        post.assert_called_once()
        self.assertEqual(
            post.call_args.kwargs["json"]["variables"]["searchInput"]["start"], 0
        )

    def test_parses_cninfo_title_search_and_keeps_official_pdf_link(self):
        published = datetime.now(timezone.utc)
        payload = {
            "announcements": [
                {
                    "announcementId": "1225000001",
                    "secCode": "002475",
                    "secName": "立讯精密",
                    "announcementTitle": "<em>立讯精密</em>：2026年半年度报告",
                    "announcementTime": int(published.timestamp() * 1000),
                    "adjunctUrl": "finalpage/2026-08-01/1225000001.PDF",
                }
            ]
        }
        source = {
            "id": "cninfo_consumer_supply_chain",
            "name": "巨潮资讯：消费电子供应链法定披露",
            "region": "china",
            "evidence_class": "primary_disclosure",
            "source_layer": "component_supplier",
        }

        items = generate_feed.blog_items_from_cninfo_listing(
            payload, source, datetime.now(timezone.utc) - timedelta(hours=48)
        )

        self.assertEqual(len(items), 1)
        self.assertEqual(items[0]["title"], "立讯精密：2026年半年度报告")
        self.assertEqual(
            items[0]["url"], "https://static.cninfo.com.cn/finalpage/2026-08-01/1225000001.PDF"
        )
        self.assertIn("立讯精密 002475", items[0]["summary"])

    def test_cninfo_listing_queries_each_configured_company(self):
        now = datetime.now(timezone.utc)
        payload = {
            "announcements": [
                {
                    "announcementId": "fresh",
                    "secCode": "002475",
                    "secName": "立讯精密",
                    "announcementTitle": "立讯精密：2026年半年度报告",
                    "announcementTime": int(now.timestamp() * 1000),
                    "adjunctUrl": "finalpage/2026-08-01/fresh.PDF",
                },
                {
                    "announcementId": "old",
                    "secCode": "002475",
                    "secName": "立讯精密",
                    "announcementTitle": "立讯精密：更早的年度报告",
                    "announcementTime": int((now - timedelta(days=3)).timestamp() * 1000),
                    "adjunctUrl": "finalpage/2026-07-29/old.PDF",
                },
            ]
        }
        response = mock.Mock()
        response.json.return_value = payload
        response.raise_for_status.return_value = None
        source = {
            "id": "cninfo_consumer_supply_chain",
            "url": "https://example.com/cninfo",
            "companies": ["立讯精密"],
            "page_size": 2,
        }

        with mock.patch("generate_feed.httpx.get", return_value=response) as get:
            items = generate_feed.fetch_cninfo_listing(source, now - timedelta(hours=48))

        self.assertEqual(len(items), 1)
        get.assert_called_once()
        self.assertEqual(get.call_args.kwargs["params"]["searchkey"], "立讯精密")
        self.assertEqual(get.call_args.kwargs["params"]["isfulltext"], "false")

    def test_parses_mediatek_server_rendered_press_room(self):
        today = datetime.now(timezone.utc)
        html = f'''<div class="pr-item pr-category-item no_image">
          <h3 class="pr-item-title"><a href="https://www.mediatek.com/press-room/dimensity-launch">
            MediaTek Launches Dimensity Platform for AI Smartphones
          </a></h3>
          <div class="pr_item_date"><p>{today.strftime("%d %b %Y - %H:%M")}</p></div>
          <div class="pr-item-excerpt">The new mobile SoC improves on-device AI, imaging and efficiency.</div>
        </div>'''
        source = {
            "id": "mediatek_press_room",
            "name": "MediaTek Press Room",
            "url": "https://www.mediatek.com/press-room",
            "region": "taiwan_asia",
            "evidence_class": "primary_disclosure",
            "source_layer": "component_supplier",
        }

        items = generate_feed.blog_items_from_mediatek_listing(
            html, source, datetime.now(timezone.utc) - timedelta(hours=48)
        )

        self.assertEqual(len(items), 1)
        self.assertEqual(items[0]["url"], "https://www.mediatek.com/press-room/dimensity-launch")
        self.assertIn("on-device AI", items[0]["summary"])
        self.assertEqual(items[0]["source_layer"], "component_supplier")

    def test_fetch_blogs_has_no_implicit_per_source_cap(self):
        now = datetime.now(timezone.utc)
        rss_items = "".join(
            f'''<item><title>Apple launches iPhone {index} with new camera</title>
              <link>https://example.com/iphone-{index}</link>
              <pubDate>{now.strftime("%a, %d %b %Y %H:%M:%S +0000")}</pubDate>
              <description>Apple announces the iPhone {index} product launch and camera upgrade.</description>
            </item>'''
            for index in range(6)
        )
        response = mock.Mock()
        response.text = f"<rss><channel>{rss_items}</channel></rss>"
        response.raise_for_status.return_value = None
        sources = {
            "filtering": {"config_path": "filter-terms.consumer-electronics.json"},
            "blogs": {
                "lookback_hours": 48,
                "sources": [
                    {
                        "id": "uncapped_test_source",
                        "name": "Uncapped test source",
                        "type": "rss",
                        "url": "https://example.com/feed.xml",
                    }
                ],
            },
        }

        with mock.patch("generate_feed.httpx.get", return_value=response):
            result = generate_feed.fetch_blogs(sources)

        self.assertIsNone(result["errors"])
        self.assertEqual(len(result["articles"]), 6)

    def test_fetch_blogs_skips_a_disabled_source_without_requesting_it(self):
        sources = {
            "filtering": {"config_path": "filter-terms.consumer-electronics.json"},
            "blogs": {
                "sources": [
                    {
                        "id": "disabled_source",
                        "name": "Disabled source",
                        "type": "rss",
                        "url": "https://example.com/feed.xml",
                        "enabled": False,
                    }
                ],
            },
        }
        with mock.patch("generate_feed.httpx.get") as get:
            result = generate_feed.fetch_blogs(sources)

        get.assert_not_called()
        self.assertEqual(result["articles"], [])
        self.assertIsNone(result["errors"])

    def test_consumer_client_rejects_legacy_feed_cache(self):
        self.assertFalse(prepare_digest.is_compatible_feed({"generated_at": "now", "x": []}))
        self.assertTrue(
            prepare_digest.is_compatible_feed(
                {"generated_at": "now", "profile": "consumer_electronics", "x": []}
            )
        )


if __name__ == "__main__":
    unittest.main()
