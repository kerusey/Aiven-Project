import aiohttp
from time import time
from collections.abc import AsyncIterable
from re import search


class WebHandler:
    def __init__(self, domain_name_list: list, regular_expression_pattern: str = ""):
        self.site_domains = domain_name_list
        self.regular_expression_pattern = regular_expression_pattern

    async def collect_domains_stats(self) -> AsyncIterable:
        """
        Runs asynchronous request to external domains in order to get site upstream information (request time, status code etc.)
        :return: AsyncIterable (de facto iterable collection of dicts that contains all the info required)
        """
        async with aiohttp.ClientSession() as session:
            for domain_name in self.site_domains:
                start_time = time()
                try:
                    async with session.get(domain_name, verify_ssl=False) as response:
                        body = await response.text()
                        kafka_message = {'host': domain_name, 'status': response.status, 'request_time': time() - start_time}
                        if self.regular_expression_pattern:
                            search_result = search(self.regular_expression_pattern, body)
                            kafka_message['regex'] = search_result.span() if search_result else None
                        yield kafka_message
                except aiohttp.client_exceptions.ClientConnectorError:
                    yield {'host': domain_name, 'status': 404, 'request_time': 0}