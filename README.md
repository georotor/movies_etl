# Запуск решения проектного задания ETL

Для запуска потребуется два файла с переменными окружения:

- `.env` с настройками для ETL, запускаемого в контейнере `etl`:
```bash
cp .env.example .env
```

- `.env.db` с настройками Postgresql:
```bash
cp .env.db.example .env.db
```

База данных в Postgresql `movies_database` и индекс `movies` в Elasticsearch разворачиваются при старте контейнеров:
```bash
docker-compose up --build
```

Перенос данных выполняется с интервалом указанным в переменной `INTERVAL` (по умолчанию 5m).

