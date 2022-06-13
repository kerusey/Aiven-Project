import logging
from kafka import KafkaConsumer
import json
from os import environ

from consumer.DatabaseHandler import DatabaseHandler

logging.basicConfig(level=logging.INFO)


class DataImporter:
    def __init__(self, topics: set = ('host',)):
        self.consumer = KafkaConsumer(
            bootstrap_servers=environ['kafka-bootstrap_servers'],
            security_protocol="SSL",
            group_id='group-1',
            ssl_cafile='../service-secrets/ca.pem',
            ssl_certfile='../service-secrets/service.cert',
            ssl_keyfile='../service-secrets/service.key',
            auto_offset_reset='earliest',
            enable_auto_commit=False
        )
        self.consumer.subscribe(topics=topics)

        self.database = DatabaseHandler()

    def proceed_the_report(self) -> None:
        """
        Reads external kafka queue (uses At Most Once Consumer semantics) and writes data to the PostgreSQL
        :return: None
        """
        message_batch = self.consumer.poll()
        self.consumer.commit()
        for partition_batch in message_batch.values():
            sql_sequence = self.database.compose_sql_sequence([json.loads(message.value.decode('utf-8')) | {'timestamp': message.timestamp} for message in partition_batch])
            self.database.execute_message_to_target_table(sql_sequence)


if __name__ == '__main__':
    data_importer_object = DataImporter()
    while True:
        data_importer_object.proceed_the_report()
