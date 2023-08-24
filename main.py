from data_extraction import mysql_extractor


def main():
    # Extract data from MySQL
    data_by_table = mysql_extractor.extract_all_data()

    for table_name, data in data_by_table.items():
        print(f"Data extracted for table '{table_name}':", data)


if __name__ == "__main__":
    main()
