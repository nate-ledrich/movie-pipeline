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
    "company_name": 0,
    "movie_count": 1
}

most_popular_genre_by_language_mapping = {
    "language_id": 0,
    "genre_id": 1,
    "language_code": 2,
    "language_name": 3,
    "genre_name": 4
}

mappings = {
    "movies_by_country": movies_by_country_mapping,
    "movie_cast_and_crew": movie_cast_and_crew_mapping,
    "production_company_movies_count": production_company_movies_count_mapping,
    "most_popular_genre_by_language": most_popular_genre_by_language_mapping
}


def transform_data(data, table_name):
    transformed_data = []
    for row in data:
        transformed_row = {target_col: row[src_col] for target_col, src_col in mappings[table_name].items()}
        transformed_data.append(transformed_row)
    return transformed_data


def transform_all_data(data_by_table):
    transformed_data = {}
    for table_name, data in data_by_table.items():
        transformed_data[table_name] = transform_data(data, table_name)
    return transformed_data
