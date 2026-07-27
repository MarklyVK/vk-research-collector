#!/bin/sh
set -eu

if [ -z "${POSTGRES_READER_PASSWORD:-}" ]; then
  echo "POSTGRES_READER_PASSWORD is required" >&2
  exit 1
fi

psql --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" \
  --set=reader_user="$POSTGRES_READER_USER" \
  --set=reader_password="$POSTGRES_READER_PASSWORD" <<'SQL'
SELECT format('CREATE ROLE %I LOGIN PASSWORD %L', :'reader_user', :'reader_password')
WHERE NOT EXISTS (SELECT FROM pg_roles WHERE rolname = :'reader_user') \gexec
SELECT format('ALTER ROLE %I PASSWORD %L', :'reader_user', :'reader_password') \gexec
SELECT format('GRANT CONNECT ON DATABASE %I TO %I', current_database(), :'reader_user') \gexec
SELECT format('GRANT USAGE ON SCHEMA public TO %I', :'reader_user') \gexec
SELECT format('GRANT SELECT ON ALL TABLES IN SCHEMA public TO %I', :'reader_user') \gexec
SELECT format('GRANT SELECT ON ALL SEQUENCES IN SCHEMA public TO %I', :'reader_user') \gexec
SELECT format('ALTER DEFAULT PRIVILEGES FOR ROLE %I IN SCHEMA public GRANT SELECT ON TABLES TO %I', current_user, :'reader_user') \gexec
SELECT format('ALTER DEFAULT PRIVILEGES FOR ROLE %I IN SCHEMA public GRANT SELECT ON SEQUENCES TO %I', current_user, :'reader_user') \gexec
SQL
