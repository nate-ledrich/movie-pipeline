import json
from cassandra.cluster import Cluster
from config.config import config
from decimal import Decimal
from datetime import date


class CustomEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        if isinstance(obj, date):
            return obj.isoformat()
        return super().default(obj)


class CassandraLoader:
    def __init__(self):
        self.cluster = Cluster([config.CASSANDRA_HOST], port=config.CASSANDRA_PORT)
        self.session = self.cluster.connect(config.CASSANDRA_KEYSPACE)

    def load_data(self, table_name, data):
        print(f"Loading data into table: {table_name}")
        insert_query = f"INSERT INTO {table_name} JSON ?"
        prepared_query = self.session.prepare(insert_query)
        for row in data:
            encoded_row = json.dumps(row, cls=CustomEncoder)
            print(f"Inserting row: {encoded_row}")
            self.session.execute(prepared_query, (encoded_row,))
        print(f"Data loaded into table: {table_name}")

    def load_all_data(self, transformed_data):
        try:
            for table_name, data in transformed_data.items():
                self.load_data(table_name, data)
        finally:
            self.close_connection()

    def close_connection(self):
        self.session.shutdown()
        self.cluster.shutdown()
