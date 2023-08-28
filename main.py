from data_extraction import mysql_extractor
from data_loading.cassandra_loader import CassandraLoader
from data_transformation import transformer


def main():
    data_by_table = mysql_extractor.extract_all_data()

    transformed_data = transformer.transform_all_data(data_by_table)

    cassandra_loader = CassandraLoader()
    cassandra_loader.load_all_data(transformed_data)


if __name__ == "__main__":
    main()
