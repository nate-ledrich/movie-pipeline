import datetime
import uuid

from cassandra.util import Date


class DataTransformer:
    movies_by_country_mapping = {
        "country_id": 0,
        "movie_id": 1,
        "title": 2,
        "country_iso_code": 3,
        "country_name": 4,
        "budget": 5,
        "homepage": 6,
        "overview": 7,
        "popularity": 8,
        "release_date": 9,
        "revenue": 10,
        "runtime": 11,
        "movie_status": 12,
        "tagline": 13,
        "vote_average": 14,
        "vote_count": 15
    }

    movie_cast_and_crew_mapping = {
        "movie_id": 0,
        "person_id": 1,
        "person_name": 2,
        "character_name": 3,
        "title": 4,
        "cast_order": 5,
        "gender": 6,
        "department_id": 7,
        "job": 8,
        "department_name": 9,
        "member_type": 10
    }

    production_company_movies_count_mapping = {
        "company_id": 0,
        "company_name": 1,
        "movie_count": 2
    }

    most_popular_genres_by_language_mapping = {
        "language_id": 0,
        "genre_id": 1,
        "language_code": 2,
        "language_name": 3,
        "genre_name": 4,
        "popularity": 5
    }

    mappings = {
        "movies_by_country": movies_by_country_mapping,
        "movie_cast_and_crew": movie_cast_and_crew_mapping,
        "production_company_movies_count": production_company_movies_count_mapping,
        "most_popular_genres_by_language": most_popular_genres_by_language_mapping
    }

    @staticmethod
    def transform_data(data, table_name):
        transformed_data = []
        for row in data:
            transformed_row = {}
            for target_col, src_col in DataTransformer.mappings[table_name].items():
                if target_col in ['movie_id', 'person_id', 'department_id', 'company_id', 'language_id', 'genre_id',
                                  'country_id']:
                    if row[src_col] is not None:
                        transformed_row[target_col] = str(uuid.UUID(bytes=row[src_col].to_bytes(16, 'big')))
                        print(f"Transformed UUID: {transformed_row[target_col]}")
                else:
                    transformed_row[target_col] = row[src_col]
            transformed_data.append(transformed_row)
        return transformed_data

    @staticmethod
    def transform_all_data(data_by_table):
        transformed_data = {}
        for table_name, data in data_by_table.items():
            transformed_data[table_name] = DataTransformer.transform_data(data, table_name)
        return transformed_data

    @staticmethod
    def format_date_for_elasticsearch(date_obj):
        if isinstance(date_obj, Date):
            formatted_date = DataTransformer.convert_cassandra_date(date_obj)
            return formatted_date
        else:
            raise ValueError("Invalid date object provided")

    @staticmethod
    def convert_cassandra_date(cassandra_date):
        if cassandra_date is None:
            return None

        year = cassandra_date.date().year
        month = cassandra_date.date().month
        day = cassandra_date.date().day

        python_date = datetime.date(year, month, day)
        return python_date.isoformat()

    @staticmethod
    def transform_for_elasticsearch(data):
        transformed_data = {}

        for table_name, table_data in data.items():
            table_transformed_data = []

            print(f"Table: {table_name}")
            print("Original Data:")

            if table_name == "movies_by_country":
                for row in table_data:
                    print(row)
                    transformed_row = {"country_id": str(row[0]),
                                       "movie_id": str(row[1]),
                                       "budget": float(row[2]),
                                       "country_iso_code": row[3],
                                       "country_name": row[4],
                                       "homepage": row[5],
                                       "movie_status": row[6],
                                       "overview": row[7],
                                       "popularity": float(row[8]),
                                       "release_date": DataTransformer.format_date_for_elasticsearch(row[9]),
                                       "revenue": float(row[10]),
                                       "runtime": row[11],
                                       "tagline": row[12],
                                       "title": row[13],
                                       "vote_average": float(row[14]),
                                       "vote_count": row[15]}

                    table_transformed_data.append(transformed_row)

                print("Transformed Data:")
                for transformed_row in table_transformed_data:
                    print(transformed_row)

            elif table_name == "movie_cast_and_crew":
                for row in table_data:
                    print(row)
                    transformed_row = {"movie_id": str(row[0]),
                                       "person_id": str(row[1]),
                                       "cast_order": row[2],
                                       "character_name": row[3],
                                       "department_id": str(row[4]),
                                       "department_name": row[5],
                                       "gender": row[6],
                                       "job": row[7],
                                       "member_type": row[8],
                                       "person_name": row[9],
                                       "title": row[10]}

                    table_transformed_data.append(transformed_row)

                print("Transformed Data:")
                for transformed_row in table_transformed_data:
                    print(transformed_row)

            elif table_name == "production_company_movies_count":
                for row in table_data:
                    print(row)
                    transformed_row = {"company_id": str(row[0]),
                                       "company_name": row[1],
                                       "movie_count": row[2]}

                    table_transformed_data.append(transformed_row)

                print("Transformed Data:")
                for transformed_row in table_transformed_data:
                    print(transformed_row)

            elif table_name == "most_popular_genres_by_language":
                for row in table_data:
                    print(row)
                    transformed_row = {"language_id": str(row[0]),
                                       "popularity": float(row[1]),
                                       "genre_id": str(row[2]),
                                       "genre_name": row[3],
                                       "language_code": row[4],
                                       "language_name": row[5]}

                    table_transformed_data.append(transformed_row)

                print("Transformed Data:")
                for transformed_row in table_transformed_data:
                    print(transformed_row)

            transformed_data[table_name] = table_transformed_data

        return transformed_data
