-- drop table if exists movies_keyspace.most_popular_genres_by_language;

create table if not exists movies_keyspace.most_popular_genres_by_language
(
    language_id   uuid,
    popularity    decimal,
    genre_id      uuid,
    genre_name    text,
    language_code text,
    language_name text,
    primary key (language_id, popularity, genre_id)
) with clustering order by (popularity desc, genre_id asc);