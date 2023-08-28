-- drop table if exists movies_keyspace.production_company_movies_count;

create table if not exists movies_keyspace.production_company_movies_count
(
    company_id   uuid primary key,
    company_name text,
    movie_count  int
);