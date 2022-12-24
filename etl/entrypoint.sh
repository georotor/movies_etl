#!/bin/sh

echo -n "Wait Postgres"
while ! nc -z $POSTGRES_HOST $POSTGRES_PORT; do
    echo -n "."
    sleep 0.1
done
echo " started"

./es_schema.sh

exec "$@"

while true
do
    python load_data.py & sleep $INTERVAL
done

