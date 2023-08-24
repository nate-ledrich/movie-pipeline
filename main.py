from data_extraction import mysql_extractor
from data_transformation import transformer


def main():
    data_by_table = mysql_extractor.extract_all_data()

    for table_name, data in data_by_table.items():
        print(f"Data extracted for table '{table_name}':", data)

    transformed_data = transformer.transform_all_data(data_by_table)
    print(transformed_data)


if __name__ == "__main__":
    main()
