#!/bin/bash


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
echo "Starting schema upgrade for greenplum 4.3.3.0."
echo "Upgrade core tables: psql -d $DB_NAME -h $DB_HOST -p $DB_PORT -U $DB_ADMIN --set ON_ERROR_STOP=1 -f upgrade_core_to_greenplum_4.3.3.0.sql"
psql -d $DB_NAME -h $DB_HOST -p $DB_PORT -U $DB_ADMIN --set ON_ERROR_STOP=1 -f upgrade_core_to_greenplum_4.3.3.0.sql
echo "Finished schema upgrade for greenplum 4.3.3.0."
echo date
