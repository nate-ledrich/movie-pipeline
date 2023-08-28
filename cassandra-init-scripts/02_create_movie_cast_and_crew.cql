-- drop table if exists movies_keyspace.movie_cast_and_crew;

create table if not exists movies_keyspace.movie_cast_and_crew
(
    movie_id        uuid,
    person_id       uuid,
    cast_order      int,
    character_name  text,
    department_id   uuid,
    department_name text,
    gender          text,
    job             text,
    member_type     text,
    person_name     text,
    title           text,
    primary key (movie_id, person_id)
) with clustering order by (person_id DESC);