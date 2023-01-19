SELECT_MOVIES = """
    SELECT
    fw.id,
    fw.rating as imdb_rating,
    COALESCE (
        jsonb_agg(
            DISTINCT jsonb_build_object(
                'id', g.id,
                'name', g.name
            )
        ) FILTER (WHERE g.id is not null),
        '[]'
    ) as genre,
    fw.title,
    fw.description,
    COALESCE (
        jsonb_agg(
            distinct p.full_name
        ) filter (where pfw.role = 'director'),
        '[]'
    ) as director,
    COALESCE (
        jsonb_agg(
            distinct p.full_name
        ) filter (where pfw.role = 'actor'),
        '[]'
    ) as actors_names,
    COALESCE (
        jsonb_agg(
            distinct p.full_name
        ) filter (where pfw.role = 'writer'),
        '[]'
    ) as writers_names,
    COALESCE (
        json_agg(
            DISTINCT jsonb_build_object(
                'id', p.id,
                'name', p.full_name
            )
        ) FILTER (WHERE pfw.role = 'actor'),
        '[]'
    ) as actors,
    COALESCE (
        json_agg(
            DISTINCT jsonb_build_object(
                'id', p.id,
                'name', p.full_name
            )
        ) FILTER (WHERE pfw.role = 'writer'),
        '[]'
    ) as writers,
    COALESCE (
        json_agg(
            DISTINCT jsonb_build_object(
                'id', p.id,
                'name', p.full_name
            )
        ) FILTER (WHERE pfw.role = 'director'),
        '[]'
    ) as directors
    FROM content.film_work fw
    LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id
    LEFT JOIN content.person p ON p.id = pfw.person_id
    LEFT JOIN content.genre_film_work gfw ON gfw.film_work_id = fw.id
    LEFT JOIN content.genre g ON g.id = gfw.genre_id
    WHERE fw.id IN %s
    GROUP BY fw.id
"""

SELECT_GENRES = """
    SELECT
    g.id,
    g.name,
    g.description,
    COUNT(*) as films_count
    FROM content.genre g
    INNER JOIN content.genre_film_work gfw ON g.id=gfw.genre_id
    WHERE g.id IN %s
    GROUP BY
    g.id,
    g.name,
    g.description
"""

SELECT_PERSONS = """
    SELECT DISTINCT
    p.id,
    p.full_name as name
    FROM content.person p
    INNER JOIN content.person_film_work pfw ON p.id=pfw.person_id
    WHERE p.id IN %s
"""
