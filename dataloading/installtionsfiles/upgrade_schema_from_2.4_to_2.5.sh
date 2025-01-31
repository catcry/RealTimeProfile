#!/bin/bash

#nohup bash upgrade_schema_from_2.3_to_2.4.sh $DB_NAME $DB_HOST $DB_PORT &
#assumes that the gpadmin is the db admin user and has the direct connection with psql without password

set -o errexit

if [ -f /greenplum_path.sh ]; then
    source /greenplum_path.sh
fi

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
DB_ADMIN="gpadmin"


echo date
echo "Starting schema upgrade."
echo "Upgrade core tables: psql -d $DB_NAME -h $DB_HOST -p $DB_PORT -U $DB_ADMIN --set ON_ERROR_STOP=1 -f upgrade_core_from_2.4_to_2.5.sql"
psql -d $DB_NAME -h $DB_HOST -p $DB_PORT -U $DB_ADMIN --set ON_ERROR_STOP=1 -f upgrade_core_from_2.4_to_2.5.sql
echo "Upgrade core functions: psql -d $DB_NAME -h $DB_HOST -p $DB_PORT -U $DB_ADMIN --set ON_ERROR_STOP=1 -f core_db_schema_functions.sql"
psql -d $DB_NAME -h $DB_HOST -p $DB_PORT -U $DB_ADMIN --set ON_ERROR_STOP=1 -f core_db_schema_functions.sql 
echo "Upgrade core tables: psql -d $DB_NAME -h $DB_HOST -p $DB_PORT -U $DB_ADMIN --set ON_ERROR_STOP=1 -f upgrade_modules_from_2.4_to_2.5.sql"
psql -d $DB_NAME -h $DB_HOST -p $DB_PORT -U $DB_ADMIN --set ON_ERROR_STOP=1 -f upgrade_modules_from_2.4_to_2.5.sql
echo "Upgrade module functions: psql -d $DB_NAME -h $DB_HOST -p $DB_PORT -U $DB_ADMIN --set ON_ERROR_STOP=1 -f modules_db_schema_functions.sql"
psql -d $DB_NAME -h $DB_HOST -p $DB_PORT -U $DB_ADMIN --set ON_ERROR_STOP=1 -f modules_db_schema_functions.sql 
echo "Upgrade module views : psql -d $DB_NAME -h $DB_HOST -p $DB_PORT -U $DB_ADMIN --set ON_ERROR_STOP=1 -f modules_db_schema_views.sql"
psql -d $DB_NAME -h $DB_HOST -p $DB_PORT -U $DB_ADMIN --set ON_ERROR_STOP=1 -f modules_db_schema_views.sql 
echo "Upgrade descriptive variable functions: psql -d $DB_NAME -h $DB_HOST -p $DB_PORT -U $DB_ADMIN --set ON_ERROR_STOP=1 -f descriptivevariables_db_schema_functions.sql"
psql -d $DB_NAME -h $DB_HOST -p $DB_PORT -U $DB_ADMIN --set ON_ERROR_STOP=1 -f descriptivevariables_db_schema_functions.sql 
echo "Upgrade template models : psql -d $DB_NAME -h $DB_HOST -p $DB_PORT -U $DB_ADMIN --set ON_ERROR_STOP=1 -f insert_churn_inactivity_template_model.sql"
psql -d $DB_NAME -h $DB_HOST -p $DB_PORT -U $DB_ADMIN --set ON_ERROR_STOP=1 -f insert_churn_inactivity_template_model.sql
echo "Upgrade template models : psql -d $DB_NAME -h $DB_HOST -p $DB_PORT -U $DB_ADMIN --set ON_ERROR_STOP=1 -f insert_churn_postpaid_template_model.sql"
psql -d $DB_NAME -h $DB_HOST -p $DB_PORT -U $DB_ADMIN --set ON_ERROR_STOP=1 -f insert_churn_postpaid_template_model.sql
echo "Finished schema upgrade."
echo date
