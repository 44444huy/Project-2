#!/bin/sh
set -eu

psql --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" \
  --set=polaris_db_user="$POLARIS_DB_USER" \
  --set=polaris_db_password="$POLARIS_DB_PASSWORD" <<'SQL'
select format(
    'create role %I with login password %L',
    :'polaris_db_user',
    :'polaris_db_password'
)
where not exists (
    select 1
    from pg_roles
    where rolname = :'polaris_db_user'
)
\gexec

select format('create database polaris owner %I', :'polaris_db_user')
where not exists (
    select 1
    from pg_database
    where datname = 'polaris'
)
\gexec
SQL
