import aiohttp
import asyncio
import time
import json
from kafka import KafkaProducer
from collections.abc import AsyncIterable
from os import environ


class DataExporter:
    def __init__(self):
        self.site_domains = [
            "https://kerusey.com",
            "https://google.com",
            "https://aiven.io",
            "https://eqewqeqweqweqwe.com"
        ]

        self.producer = KafkaProducer(
            bootstrap_servers=environ['kafka-bootstrap_servers'],
            security_protocol="SSL",
            ssl_cafile="/service-secrets/ca.pem",
            ssl_certfile="/service-secrets/service.cert",
            ssl_keyfile="/service-secrets/service.key",
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
                        await response.text()
                        yield {'host': domain_name, 'status': response.status, 'request_time': time.time() - start_time}
                except aiohttp.client_exceptions.ClientConnectorError:
                    yield {'host': domain_name, 'status': 404, 'request_time': 0}

    async def send_statistics_to_kafka(self) -> None:
        async for stats in self.collect_domains_stats():
            self.producer.send('host', json.dumps(stats).encode('utf-8'))
        self.producer.flush()


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(DataExporter().send_statistics_to_kafka())
    finally:
        loop.run_until_complete(loop.shutdown_asyncgens())
        loop.close()
