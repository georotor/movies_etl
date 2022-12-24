#!/bin/sh

/opt/es_schema.sh & /bin/tini -- /usr/local/bin/docker-entrypoint.sh

