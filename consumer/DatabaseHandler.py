import psycopg2
from os import environ
import logging


class DatabaseHandler:
    def __init__(self):
        self.create_database_table()

    @staticmethod
    def open_database():
        return psycopg2.connect(
            database=environ['database-name'],
            user=environ['database-username'],
            password=environ['database-password'],
            host=environ['database-host'],
            port=environ['database-port']
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
                        REQUEST_TIME FLOAT NOT NULL,
                        REGEX VARCHAR(255)
                    );

                    CREATE SEQUENCE hosts_id_seq AS INTEGER;
                    ALTER TABLE HOSTS
                        ALTER COLUMN id SET DEFAULT nextval('public.hosts_id_seq'::regclass);
                    ALTER SEQUENCE hosts_id_seq owned BY hosts.id;
                '''
            )
            database.commit()
            logging.info("Hosts table has been successfully created!")
        except psycopg2.errors.DuplicateTable:
            logging.info("Hosts table is already excepts")
        finally:
            database.close()

    @staticmethod
    def compose_sql_sequence(messages_list: list[dict]) -> str:
        """
        Creates sql insertion sequence to the database table
        :param messages_list: message instances that have to be sent to the database
        :return: sql sequence ready to be executed
        """
        sql_sequence = '''
            INSERT INTO HOSTS (TIMESTAMP, DOMAIN, STATUS, REQUEST_TIME, REGEX) VALUES
        '''
        for message in messages_list:
            sql_sequence += f"({message['timestamp']}, '{message['host']}', {message['status']}, {message['request_time']}, {tuple(message['regex']) if message.get('regex') else 'NULL'}),"
        logging.info(f"{sql_sequence[:-1] + ';'}\nhas been executed!")
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

    def request_data_from_table(self, table_name: str = "HOSTS"):
        database = self.open_database()
        cursor = database.cursor()
        cursor.execute(f"SELECT * FROM {table_name}")
        return cursor.fetchall()
