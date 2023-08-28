from cassandra.cluster import Cluster
from config.config import Config


class CassandraDataProvider:
    table_names = [
        "movies_by_country",
        "movie_cast_and_crew",
        "production_company_movies_count",
        "most_popular_genres_by_language"
    ]

    def __init__(self):
        self.cluster = Cluster([Config.CASSANDRA_HOST], port=Config.CASSANDRA_PORT)
        self.session = self.cluster.connect()

    def connect_to_keyspace(self, keyspace_name):
        self.session.set_keyspace(keyspace_name)

    def fetch_data(self, table_name):
        query = f"SELECT * FROM {table_name};"
        rows = self.session.execute(query)
        return [row for row in rows]

    def fetch_all_data(self):
        try:
            self.connect_to_keyspace(Config.CASSANDRA_KEYSPACE)
            fetched_data = {}
            for table_name in self.table_names:
                fetched_data[table_name] = self.fetch_data(table_name)
                print(f"Raw data from {table_name}: {fetched_data[table_name]}")
            return fetched_data
        finally:
            self.close_connection()

    def close_connection(self):
        self.session.shutdown()
        self.cluster.shutdown()
