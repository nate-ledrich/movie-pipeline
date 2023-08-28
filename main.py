from data_extraction.mysql_extractor import MySQLExtractor
from data_loading.cassandra_loader import CassandraLoader
from data_transformation.transformer import DataTransformer


def main():
    extractor = MySQLExtractor()
    data_by_table = extractor.extract_all_data()

    transformer = DataTransformer()
    transformed_data = transformer.transform_all_data(data_by_table)

    cassandra_loader = CassandraLoader()
    cassandra_loader.load_all_data(transformed_data)


if __name__ == "__main__":
    main()
