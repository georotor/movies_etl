SELECT_MOVIES = """
    SELECT
    fw.id,
    fw.title,
    fw.description,
    fw.rating as imdb_rating,
    fw.type,
    COALESCE (
        json_agg(
            DISTINCT jsonb_build_object(
                'role', pfw.role,
                'id', p.id,
                'name', p.full_name
            )
        ) FILTER (WHERE p.id is not null),
        '[]'
    ) as persons,
    array_agg(DISTINCT g.name) as genres
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
    g.description
    FROM content.genre g
    WHERE g.id IN %s
"""

SELECT_PERSONS = """
    SELECT 
    p.id,
    p.full_name as name
    FROM content.person p
    WHERE p.id IN %s
"""
