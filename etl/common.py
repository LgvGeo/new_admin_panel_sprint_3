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

SELECT_GENRES_SQL_PATTERN = """
SELECT DISTINCT
    g.id,
    g.name,
    g.description,
    g.modified
FROM content.genre g
JOIN content.genre_film_work gfw ON gfw.genre_id = g.id
JOIN content.film_work fw ON gfw.film_work_id = fw.id
WHERE g.modified > '{timestamp}'::timestamp
order by modified
"""

SELECT_PERSONS_SQL_PATTERN = """
with pf as (SELECT DISTINCT
    p.id as id,
    p.full_name as name,
    fw.id as film_id,
    fw.title as title,
    array_agg(pfw.role) as roles,

    case
        when
            fw.modified >= p.modified
        then fw.modified
        else p.modified
    end as modified

FROM content.person p
JOIN content.person_film_work pfw ON pfw.person_id = p.id
JOIN content.film_work fw ON pfw.film_work_id = fw.id
WHERE p.modified > '{timestamp}'::timestamp
    or fw.modified > '{timestamp}'::timestamp
group by p.id, p.full_name, fw.id, fw.title)
SELECT
    id,
    name,
    max(modified) as modified,
    json_agg(
        DISTINCT jsonb_build_object(
            'id', film_id,
            'title', title,
            'roles', roles
        )
    ) as films
    from pf
    GROUP BY id, name
    ORDER BY modified
"""
