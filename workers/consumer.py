from kafka import KafkaConsumer
import psycopg2
import json


class DatabaseHandler:
    def __init__(self):
        self.create_database_table()

    @staticmethod
    def open_database():
        return psycopg2.connect(
            database="host-statistics",
            user="avnadmin",
            password="AVNS_EAB_pkbi6QBQR882ehD",
            host="pg-3b73cfc1-keruseyoff-1ead.aivencloud.com",
            port="26981"
        )

    def create_database_table(self) -> None:
        """
        Creates target table for the reports and sequence for auto increment id pk in the database
        :return:
        """
        database = self.open_database()
        cursor = database.cursor()
        try:
            cursor.execute(
                '''
                    CREATE TABLE HOSTS(
                        ID INT PRIMARY KEY NOT NULL,
                        TIMESTAMP BIGINT NOT NULL,
                        DOMAIN VARCHAR(255) NOT NULL,
                        STATUS INT NOT NULL,
                        REQUEST_TIME FLOAT NOT NULL
                    );

                    CREATE SEQUENCE hosts_id_seq
                        AS INTEGER;
                    ALTER TABLE HOSTS
                        ALTER COLUMN id SET DEFAULT nextval('public.hosts_id_seq'::regclass);
                    ALTER SEQUENCE hosts_id_seq owned BY hosts.id;
                '''
            )
            database.commit()
        except psycopg2.errors.DuplicateTable:
            pass
            # might be logged buu whatever
        finally:
            database.close()

    @staticmethod
    def compose_sql_sequence(messages_list: list[dict]) -> str:
        sql_sequence = '''
            INSERT INTO HOSTS (TIMESTAMP, DOMAIN, STATUS, REQUEST_TIME) VALUES
        '''
        for message in messages_list:
            sql_sequence += f"({message['timestamp']}, '{message['host']}', {message['status']}, {message['request_time']}),"
        return sql_sequence[:-1] + ';'

    def execute_message_to_target_table(self, sql_sequence: str) -> None:
        """
        Writes report message (host name, status code and so on) to the target table (hosts) in the database
        :param sql_sequence:
        :return:
        """
        database = self.open_database()
        cursor = database.cursor()
        cursor.execute(sql_sequence)
        database.commit()
        database.close()


class DataImporter:
    def __init__(self):
        self.consumer = KafkaConsumer(
            bootstrap_servers="kafka-2859ed46-keruseyoff-1ead.aivencloud.com:26983",
            security_protocol="SSL",
            group_id='group-1',
            ssl_cafile="ca.pem",
            ssl_certfile="service.cert",
            ssl_keyfile="service.key",
            auto_offset_reset='earliest',
            enable_auto_commit=False
        )
        self.consumer.subscribe(topics='host')

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