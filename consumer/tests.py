import unittest
import psycopg2

from consumer.Consumer import DataImporter
from consumer.DatabaseHandler import DatabaseHandler
from producer.Producer import DataExporter


class ConsumerTests(unittest.TestCase):
    def test_kafka_read_and_write(self):
        """
        Tests read and write to kafka possibility
        :return:
        """
        testing_values = [
            'testing-kafka', 'testing-kafka123', '123testing-kafka123'
        ]

        data_exporter_object = DataExporter(None, None)
        for testing_value in testing_values:  # process of sending messages to kafka
            data_exporter_object.producer.send('test', testing_value.encode('utf-8'))

        data_importer = DataImporter(topics=('test',))
        message_batch = data_importer.consumer.poll()

        data_importer.consumer.commit()
        for partition_batch in message_batch.values():
            for index, message in enumerate(partition_batch):
                self.assertEqual(message.value, testing_values[index])

    def test_postgresql_read_and_write(self):
        """
        Tests read and write to postgresql possibility
        :return:
        """
        insertion_values = [
            (1, 'DOMAIN1', 200, 2.2, 'NONE'),
            (2, 'DOMAIN2', 300, 2.1, 'NONE'),
            (3, 'DOMAIN3', 400, 2.3, 'NONE')
        ]
        sql_insertion_sequence = f"""
            INSERT INTO TESTS (TIMESTAMP, DOMAIN, STATUS, REQUEST_TIME, REGEX) \
            VALUES {str([value for value in insertion_values])[1: -1]};
        """

        database_handler = DatabaseHandler()
        database = database_handler.open_database()
        cursor = database.cursor()
        try:
            cursor.execute(
                '''
                    CREATE TABLE TESTS(
                        ID INT PRIMARY KEY NOT NULL,
                        TIMESTAMP BIGINT NOT NULL,
                        DOMAIN VARCHAR(255) NOT NULL,
                        STATUS INT NOT NULL,
                        REQUEST_TIME FLOAT NOT NULL,
                        REGEX VARCHAR(255)
                    );

                    CREATE SEQUENCE tests_id_seq AS INTEGER;
                    ALTER TABLE TESTS
                        ALTER COLUMN id SET DEFAULT nextval('public.tests_id_seq'::regclass);
                    ALTER SEQUENCE tests_id_seq owned BY tests.id;
                '''
            )
            database.commit()
        except psycopg2.errors.DuplicateTable:
            pass

        database_handler.execute_message_to_target_table(sql_insertion_sequence)
        for index, instance in enumerate(database_handler.request_data_from_table("TESTS")):
            self.assertEqual(instance[1:], insertion_values[index])

        database_handler.execute_message_to_target_table(f'TRUNCATE TABLE TESTS')
