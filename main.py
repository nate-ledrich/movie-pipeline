from data_extraction.mysql_extractor import MySQLExtractor
from data_fetching.cassandra_data_fetcher import CassandraDataProvider
from data_indexing.elasticsearch_indexer import ElasticsearchIndexer
from data_loading.cassandra_loader import CassandraLoader
from data_transformation.transformer import DataTransformer


class DataProcessor:
    @staticmethod
    def extract_transform_load_from_mysql_to_cassandra():
        mysql_extractor = MySQLExtractor()
        data_by_table = mysql_extractor.extract_all_data()

        transformed_data = DataTransformer.transform_all_data(data_by_table)

        cassandra_loader = CassandraLoader()
        cassandra_loader.load_all_data(transformed_data)

    @staticmethod
    def fetch_transform_index_from_cassandra_to_elastic():
        cassandra_provider = CassandraDataProvider()
        cassandra_data = cassandra_provider.fetch_all_data()
        print("Data fetched")

        transformed_data = DataTransformer.transform_for_elasticsearch(cassandra_data)
        print("Data formatted")

        indexer = ElasticsearchIndexer()
        indexer.index_data_into_elasticsearch(transformed_data)
        print("Data indexed")


def main():
    try:
        data_processor = DataProcessor()
        data_processor.extract_transform_load_from_mysql_to_cassandra()
        data_processor.fetch_transform_index_from_cassandra_to_elastic()

    except Exception as e:
        print("An error occurred:", e)


if __name__ == "__main__":
    main()
