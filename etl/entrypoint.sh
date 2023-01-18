#!/bin/sh

echo -n "Wait Postgres"
while ! nc -z $POSTGRES_HOST $POSTGRES_PORT; do
    echo -n "."
    sleep 0.1
done
echo " started"

./schema.sh

exec "$@"

die_func() {
    exit 1
}
trap die_func TERM

while true
do
    python load_data.py & sleep $INTERVAL & wait
done

