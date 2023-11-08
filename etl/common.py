LOADING_SIZE = 10000
SLEEPING_TIME = 30

SELECT_MOVIES_SQL_PATTERN = """
SELECT
   fw.id,
   fw.title,
   fw.description,
   fw.rating as imdb_rating,

   case
        when
            max(fw.modified) >= max(p.modified)
            and max(fw.modified) >= max(g.modified)
        then max(fw.modified)
        when
            max(p.modified) >= max(g.modified)
        then max(p.modified)
        else max(g.modified)
    end as modified,

   COALESCE (
       json_agg(
           DISTINCT jsonb_build_object(
               'id', p.id,
               'name', p.full_name
           )
       ) FILTER (WHERE p.id is not null and pfw.role = 'actor'),
       '[]'
   ) as actors,

   COALESCE (
       json_agg(
           DISTINCT jsonb_build_object(
               'id', p.id,
               'name', p.full_name
           )
       ) FILTER (WHERE p.id is not null and pfw.role = 'writer'),
       '[]'
   ) as writers,

    COALESCE(
        array_agg(DISTINCT p.full_name)
        FILTER (
            where p.id is not null and pfw.role = 'director'),
        ARRAY[]::text[]
    ) as director,

    COALESCE(
        array_agg(DISTINCT p.full_name)
        FILTER (
            where p.id is not null and pfw.role = 'actor'), ARRAY[]::text[]
    ) as actors_names,

    COALESCE(
        array_agg(DISTINCT p.full_name)
        FILTER (
            where p.id is not null and pfw.role = 'writer'), ARRAY[]::text[]
    ) as writers_names,

   COALESCE (
       json_agg(
           DISTINCT jsonb_build_object(
               'id', g.id,
               'name', g.name
           )
       ) FILTER (WHERE g.id is not null),
       '[]'
   ) as genre
FROM content.film_work fw
LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id
LEFT JOIN content.person p ON p.id = pfw.person_id
LEFT JOIN content.genre_film_work gfw ON gfw.film_work_id = fw.id
LEFT JOIN content.genre g ON g.id = gfw.genre_id
GROUP BY fw.id
having max(fw.modified) > '{timestamp}'::timestamp
or max(p.modified) > '{timestamp}'::timestamp
or max(g.modified) > '{timestamp}'::timestamp
ORDER BY modified"""
