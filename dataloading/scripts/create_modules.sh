#!/bin/bash

set -o errexit

EXPECTED_ARGS=3
E_BADARGS=101

if [ $# -lt $EXPECTED_ARGS ]
then
  echo "Usage: `basename $0` and $EXPECTED_ARGS arguments"
  exit $E_BADARGS
fi

DB_NAME=$1
DB_HOST=$2
DB_PORT=$3

if [ $# -eq 4 ]
then
  DB_ADMIN="-U $4"
else
  DB_ADMIN=""
fi

echo "==start to install modules into database $1 =="
echo "Create module tables : psql -d $DB_NAME -h $DB_HOST -p $DB_PORT $DB_ADMIN --set ON_ERROR_STOP=1 -f modules_db_schema_tables.sql"
psql -d $DB_NAME -h $DB_HOST -p $DB_PORT $DB_ADMIN --set ON_ERROR_STOP=1 -f modules_db_schema_tables.sql 
echo "Create module functions : psql -d $DB_NAME -h $DB_HOST -p $DB_PORT $DB_ADMIN --set ON_ERROR_STOP=1 -f modules_db_schema_functions.sql"
psql -d $DB_NAME -h $DB_HOST -p $DB_PORT $DB_ADMIN --set ON_ERROR_STOP=1 -f modules_db_schema_functions.sql 
echo "Create module views : psql -d $DB_NAME -h $DB_HOST -p $DB_PORT $DB_ADMIN --set ON_ERROR_STOP=1 -f modules_db_schema_views.sql"
psql -d $DB_NAME -h $DB_HOST -p $DB_PORT $DB_ADMIN --set ON_ERROR_STOP=1 -f modules_db_schema_views.sql 
echo "Create descriptive variable functions : psql -d $DB_NAME -h $DB_HOST -p $DB_PORT $DB_ADMIN --set ON_ERROR_STOP=1 -f descriptivevariables_db_schema_functions.sql"
psql -d $DB_NAME -h $DB_HOST -p $DB_PORT $DB_ADMIN --set ON_ERROR_STOP=1 -f descriptivevariables_db_schema_functions.sql
echo "Create template models : psql -d $DB_NAME -h $DB_HOST -p $DB_PORT $DB_ADMIN --set ON_ERROR_STOP=1 -f insert_churn_inactivity_template_model.sql"
psql -d $DB_NAME -h $DB_HOST -p $DB_PORT $DB_ADMIN --set ON_ERROR_STOP=1 -f insert_churn_inactivity_template_model.sql
echo "Create template models : psql -d $DB_NAME -h $DB_HOST -p $DB_PORT $DB_ADMIN --set ON_ERROR_STOP=1 -f insert_churn_postpaid_template_model.sql"
psql -d $DB_NAME -h $DB_HOST -p $DB_PORT $DB_ADMIN --set ON_ERROR_STOP=1 -f insert_churn_postpaid_template_model.sql


echo "==end of modules schema creation =="


set `date`
echo "The date now is $@"; 
