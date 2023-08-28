from data_extraction.mysql_extractor import MySQLExtractor
from data_fetching.cassandra_data_fetcher import CassandraDataProvider
from data_loading.cassandra_loader import CassandraLoader
from data_transformation.transformer import DataTransformer


class DataProcessor:
    def __init__(self):
        self.mysql_extractor = MySQLExtractor()
        self.cassandra_provider = CassandraDataProvider()

    def extract_transform_load_from_mysql_to_cassandra(self):
        data_by_table = self.mysql_extractor.extract_all_data()

        transformed_data = DataTransformer.transform_all_data(data_by_table)

        cassandra_loader = CassandraLoader()
        cassandra_loader.load_all_data(transformed_data)

    def fetch_transform_from_cassandra(self):
        cassandra_data = self.cassandra_provider.fetch_all_data()

        transformed_data = DataTransformer.transform_for_elasticsearch(cassandra_data)

        return transformed_data


def main():
    try:
        data_processor = DataProcessor()
        data_processor.extract_transform_load_from_mysql_to_cassandra()
        transformed_data_for_elasticsearch = data_processor.fetch_transform_from_cassandra()

    except Exception as e:
        print("An error occurred:", e)


if __name__ == "__main__":
    main()
