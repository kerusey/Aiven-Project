import asyncio
import time
import json
import logging
from kafka import KafkaProducer
from os import environ
import sys

from WebHandler import WebHandler

# https://github.com/encode/httpx/issues/914#issuecomment-622586610
# this is a bug of python async package in windows causing a runtime error

if sys.version_info[0] == 3 and sys.version_info[1] >= 8 and sys.platform.startswith('win'):
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())


logging.basicConfig(level=logging.INFO)


class DataExporter:
    def __init__(self, domain_name_list: list, regular_expression_pattern: str = ""):
        self.web_handler = WebHandler(domain_name_list, regular_expression_pattern)

        self.producer = KafkaProducer(
            bootstrap_servers=environ['kafka-bootstrap_servers'],
            security_protocol="SSL",
            ssl_cafile="../service-secrets/ca.pem",
            ssl_certfile="../service-secrets/service.cert",
            ssl_keyfile="../service-secrets/service.key",
        )

    async def send_statistics_to_kafka(self) -> None:
        """
        Main function that sends requested statisctics to kafka
        :return:
        """
        async for stats in self.web_handler.collect_domains_stats():
            self.producer.send('host', json.dumps(stats).encode('utf-8'))
            logging.info(f"Statistics {stats} have been successfully sent to Kafka!")
        self.producer.flush()


if __name__ == "__main__":
    while True:
        loop = asyncio.new_event_loop()
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
