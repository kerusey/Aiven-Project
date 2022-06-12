from producer import DataExporter
from unittest import IsolatedAsyncioTestCase


class ProducerTests(IsolatedAsyncioTestCase):
    async def test_status_codes(self):
        self.valid_status_codes = {
            "https://kerusey.com": 200,
            "https://google.com": 200,
            "https://aiven.io": 200,
            "https://eqewqeqweqweqwe.com": 404,
            "https://intra.epitech.eu/planning/#": 403
        }
        data_exporter = DataExporter(
            domain_name_list=[
                domain_name for domain_name in self.valid_status_codes
            ]
        )

        async for stats in data_exporter.collect_domains_stats():
            self.assertEqual(self.valid_status_codes[stats['host']], stats['status'])

    async def test_request_time(self):
        data_exporter = DataExporter(
            domain_name_list=[
                "https://kerusey.com",
                "https://google.com",
                "https://aiven.io",
                "https://eqewqeqweqweqwe.com",  # 404
                "https://intra.epitech.eu/planning/#"  # 401
            ]
        )

        async for stats in data_exporter.collect_domains_stats():
            self.assertTrue(0 <= stats['request_time'] <= 1)

    async def test_regex_in_html_body(self):
        self.valid_regex_results = {
            "https://kerusey.com": (492, 503),
            "https://google.com": None,
            "https://aiven.io": None,
            "https://eqewqeqweqweqwe.com": None,
            "https://intra.epitech.eu/planning/#": None
        }
        self.regex = 'Danila Likh'
        data_exporter = DataExporter(
            domain_name_list=[
                domain_name for domain_name in self.valid_regex_results
            ],
            regular_expression_pattern=self.regex
        )

        async for stats in data_exporter.collect_domains_stats():
            self.assertEqual(self.valid_regex_results[stats['host']], stats.get('regex'))