#!/bin/bash

set -o errexit

EXPECTED_ARGS=5
E_BADARGS=101

if [ $# -lt $EXPECTED_ARGS ]
then
  echo "Usage: `basename $0` and $EXPECTED_ARGS arguments"
  exit $E_BADARGS
fi

DB_NAME=$1
DB_HOST=$2
DB_PORT=$3
DB_USER=$4
DB_PASS=$5

if [ $# -eq 6 ]
then
  DB_ADMIN="-U $6"
else
  DB_ADMIN=""
fi

echo "==start to create database $1 =="
echo "Create database : psql -d postgres -h $DB_HOST -p $DB_PORT $DB_ADMIN --set ON_ERROR_STOP=1 -c \"create database $DB_NAME;\""
psql -d postgres -h $DB_HOST -p $DB_PORT $DB_ADMIN --set ON_ERROR_STOP=1 -c "create database $DB_NAME;"
echo "Create language : psql -d $DB_NAME -h $DB_HOST -p $DB_PORT $DB_ADMIN --set ON_ERROR_STOP=1 -c \"create language plpgsql;\""
psql -d $DB_NAME -h $DB_HOST -p $DB_PORT $DB_ADMIN --set ON_ERROR_STOP=1 -c "create language plpgsql;"
echo "Create user with password : user $DB_USER with login password '$DB_PASS'"  
psql -d $DB_NAME -h $DB_HOST -p $DB_PORT $DB_ADMIN --set ON_ERROR_STOP=1 -c "
CREATE OR REPLACE FUNCTION create_role()
  RETURNS void AS
\$BODY\$
  DECLARE
  user_exist text;
  BEGIN
    select rolname from pg_roles where rolname = '$DB_USER' into user_exist;
    if user_exist is null then
      create user $DB_USER with login password '$DB_PASS';
    end if;
  END;
\$BODY\$
LANGUAGE plpgsql VOLATILE;
select create_role();
DROP FUNCTION create_role();"
echo "Grant privileges : psql -d $DB_NAME -h $DB_HOST -p $DB_PORT $DB_ADMIN --set ON_ERROR_STOP=1 -c \"grant all on database $DB_NAME to $DB_USER;\""
psql -d $DB_NAME -h $DB_HOST -p $DB_PORT $DB_ADMIN --set ON_ERROR_STOP=1 -c "grant all on database $DB_NAME to $DB_USER;"
echo "==start to create tables =="
echo "Command : psql -d $DB_NAME -h $DB_HOST -p $DB_PORT $DB_ADMIN --set ON_ERROR_STOP=1 -f core_db_schema_tables.sql"
psql -d $DB_NAME -h $DB_HOST -p $DB_PORT $DB_ADMIN --set ON_ERROR_STOP=1 -f core_db_schema_tables.sql 

echo "==start to create functions =="
echo "Command : psql -d $DB_NAME -h $DB_HOST -p $DB_PORT $DB_ADMIN --set ON_ERROR_STOP=1 -f core_db_schema_functions.sql"
psql -d $DB_NAME -h $DB_HOST -p $DB_PORT $DB_ADMIN --set ON_ERROR_STOP=1 -f core_db_schema_functions.sql 

echo "==end to create basic schema=="

set `date`
echo "The date now is $@";
