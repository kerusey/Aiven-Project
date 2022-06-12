import aiohttp
import asyncio
import time
import json
import logging
from kafka import KafkaProducer
from collections.abc import AsyncIterable
from os import environ
from re import search


logging.basicConfig(level=logging.INFO)


class DataExporter:
    def __init__(self, domain_name_list: list, regular_expression_pattern: str = ""):
        self.site_domains = domain_name_list
        self.regular_expression_pattern = regular_expression_pattern

        self.producer = KafkaProducer(
            bootstrap_servers=environ['kafka-bootstrap_servers'],
            security_protocol="SSL",
            ssl_cafile="../service-secrets/ca.pem",
            ssl_certfile="../service-secrets/service.cert",
            ssl_keyfile="../service-secrets/service.key",
        )

    async def collect_domains_stats(self) -> AsyncIterable:
        """
        Runs asynchronous request to external domains in order to get site upstream information (request time, status code etc.)
        :return: AsyncIterable (de facto iterable collection of dicts that contains all the info required)
        """
        async with aiohttp.ClientSession() as session:
            for domain_name in self.site_domains:
                start_time = time.time()
                try:
                    async with session.get(domain_name) as response:
                        body = await response.text()
                        message_body = {'host': domain_name, 'status': response.status, 'request_time': time.time() - start_time}
                        if self.regular_expression_pattern:
                            search_result = search(self.regular_expression_pattern, body)
                            message_body['regex'] = search_result.span() if search_result else None
                        yield message_body
                except aiohttp.client_exceptions.ClientConnectorError:
                    yield {'host': domain_name, 'status': 404, 'request_time': 0}

    async def send_statistics_to_kafka(self) -> None:
        async for stats in self.collect_domains_stats():
            self.producer.send('host', json.dumps(stats).encode('utf-8'))
            logging.info(f"Statistics {stats} have been successfully sent to Kafka!")
        self.producer.flush()


if __name__ == "__main__":
    while True:
        loop = asyncio.get_event_loop()
        try:
            loop.run_until_complete(DataExporter(
                domain_name_list=[
                    "https://kerusey.com",
                    "https://google.com",
                    "https://aiven.io",
                    "https://eqewqeqweqweqwe.com",  # 404
                    "https://intra.epitech.eu/planning/#"  # 401
                ],
                regular_expression_pattern="Danila Likh"
            ).send_statistics_to_kafka())
        finally:
            loop.run_until_complete(loop.shutdown_asyncgens())
            loop.close()
        time.sleep(10)  # change the frequency of the worker runs here
