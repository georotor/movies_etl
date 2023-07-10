# Movies: ETL для кинотеатра (postgresql -> elasticsearch)

## Архитектура

![Архитектура](https://github.com/georotor/movies_etl/blob/main/doc/schema.png?raw=true)

## Запуск сервиса

Для запуска потребуется два файла с переменными окружения:

- `.env` с настройками для ETL, запускаемого в контейнере `etl`:
```bash
cp .env.example .env
```

- `.env.db` с настройками Postgresql:
```bash
cp .env.db.example .env.db
```

Схема `movies_database` с данными в Postgresql и индекс `movies` в Elasticsearch разворачиваются при старте контейнеров:
```bash
docker-compose up --build
```

Перенос данных выполняется с интервалом указанным в переменной `INTERVAL` (по умолчанию 5m).
