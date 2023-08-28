import mysql.connector
from config.config import Config


class MySQLExtractor:
    movies_by_country_query = ("SELECT country.country_id,"
                               "movie.movie_id,"
                               "movie.title, "
                               "country.country_iso_code ,"
                               "country.country_name, "
                               "movie.budget, "
                               "movie.homepage , "
                               "movie.overview, "
                               "movie.popularity, "
                               "movie.release_date, "
                               "movie.revenue, "
                               "movie.runtime, "
                               "movie.movie_status, "
                               "movie.tagline, "
                               "movie.vote_average, "
                               "movie.vote_count "
                               "FROM movies.movie "
                               "JOIN movies.production_country "
                               "ON movie.movie_id = production_country.movie_id "
                               "JOIN movies.country "
                               "ON country.country_id = production_country.country_id;")

    movie_cast_and_crew_query = ("SELECT m.movie_id, "
                                 "p.person_id, "
                                 "p.person_name, "
                                 "mc.character_name, "
                                 "m.title, "
                                 "mc.cast_order, "
                                 "g.gender, "
                                 "NULL AS department_id, "
                                 "NULL AS job, "
                                 "NULL AS department_name, "
                                 "'Cast' AS member_type "
                                 "FROM movie m "
                                 "LEFT JOIN movie_cast mc ON m.movie_id = mc.movie_id "
                                 "LEFT JOIN person p ON mc.person_id = p.person_id "
                                 "LEFT JOIN gender g ON mc.gender_id = g.gender_id "
                                 "WHERE p.person_id IS NOT NULL "
                                 "UNION "
                                 "SELECT m.movie_id,"
                                 "p.person_id, "
                                 "p.person_name, "
                                 "NULL AS character_name, "
                                 "m.title, "
                                 "NULL AS cast_order, "
                                 "NULL AS gender, "
                                 "d.department_id, "
                                 "mcr.job, "
                                 "d.department_name, "
                                 "'Crew' AS member_type "
                                 "FROM movie m "
                                 "LEFT JOIN movie_crew mcr ON m.movie_id = mcr.movie_id "
                                 "LEFT JOIN person p ON mcr.person_id = p.person_id "
                                 "LEFT JOIN department d ON mcr.department_id = d.department_id "
                                 "WHERE p.person_id IS NOT NULL;")

    production_company_movies_count_query = ("SELECT pc.company_id, "
                                             "pc.company_name, "
                                             "COUNT(DISTINCT mc.movie_id) AS movie_count "
                                             "FROM production_company pc "
                                             "JOIN movie_company mc ON pc.company_id = mc.company_id "
                                             "GROUP BY pc.company_id "
                                             "ORDER BY movie_count DESC;")

    most_popular_genres_by_language_query = ("SELECT l.language_id, "
                                             "g.genre_id, "
                                             "l.language_code, "
                                             "l.language_name, "
                                             "g.genre_name, "
                                             "AVG(m.popularity) AS average_popularity "
                                             "FROM language l "
                                             "JOIN movie_languages ml ON l.language_id = ml.language_id "
                                             "JOIN language_role lr ON ml.language_role_id = lr.role_id "
                                             "JOIN movie m ON ml.movie_id = m.movie_id "
                                             "JOIN movie_genres mg ON m.movie_id = mg.movie_id "
                                             "JOIN genre g ON mg.genre_id = g.genre_id "
                                             "WHERE lr.language_role = 'Original' "
                                             "GROUP BY l.language_id, "
                                             "g.genre_id, "
                                             "l.language_code, "
                                             "l.language_name, "
                                             "g.genre_name "
                                             "ORDER BY l.language_name, average_popularity DESC;")

    TABLE_QUERIES = {
        "movies_by_country": movies_by_country_query,
        "movie_cast_and_crew": movie_cast_and_crew_query,
        "production_company_movies_count": production_company_movies_count_query,
        "most_popular_genres_by_language": most_popular_genres_by_language_query
    }

    def __init__(self):
        self.config = Config()
        self.connection = self.connect_to_mysql()
        self.cursor = self.connection.cursor()

    def connect_to_mysql(self):
        return mysql.connector.connect(
            host=self.config.MYSQL_HOST,
            user=self.config.MYSQL_USER,
            password=self.config.MYSQL_PASSWORD,
            database=self.config.MYSQL_DATABASE
        )

    def extract_data_from_one_table(self, query):
        try:
            self.cursor.execute(query)
            data = self.cursor.fetchall()
            return data
        except mysql.connector.Error as err:
            print("Error:", err)
            return []

    def extract_all_data(self):
        try:
            data_by_table = {}
            for table_name, query in self.TABLE_QUERIES.items():
                data = self.extract_data_from_one_table(query)
                data_by_table[table_name] = data
            return data_by_table
        finally:
            self.close_connection()

    def close_connection(self):
        self.cursor.close()
        self.connection.close()
