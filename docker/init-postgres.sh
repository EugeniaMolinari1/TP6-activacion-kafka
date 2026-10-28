#!/bin/sh
set -eu
for service in activation billing provisioning notification crm loyalty; do
 psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname postgres -v service="$service" -v password="$POSTGRES_PASSWORD" <<'SQL'
SELECT format('CREATE ROLE %I LOGIN PASSWORD %L', :'service', :'password') \gexec
SELECT format('CREATE DATABASE %I OWNER %I', :'service', :'service') \gexec
SELECT format('REVOKE CONNECT ON DATABASE %I FROM PUBLIC', :'service') \gexec
SELECT format('GRANT CONNECT ON DATABASE %I TO %I', :'service', :'service') \gexec
SQL
done
