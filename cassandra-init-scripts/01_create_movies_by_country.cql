-- drop table if exists movies_keyspace.movies_by_country;

create table if not exists movies_keyspace.movies_by_country
(
    country_id       uuid,
    movie_id         uuid,
    budget           decimal,
    country_iso_code text,
    country_name     text,
    homepage         text,
    movie_status     text,
    overview         text,
    popularity       decimal,
    release_date     date,
    revenue          decimal,
    runtime          int,
    tagline          text,
    title            text,
    vote_average     decimal,
    vote_count       int,
    primary key (country_id, movie_id)
) with clustering order by (movie_id DESC);