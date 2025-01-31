#!/bin/bash

set -o errexit

# Execute the script using the following command:
# nohup bash create_db_core.sh > db_core_$(date +%F_%R).log &

# /etc/profile
# greenplum settings
# source /usr/local/greenplum-db/greenplum_path.sh
# MASTER_DATA_DIRECTORY=/greenplum/xtr-1
# export MASTER_DATA_DIRECTORY

## Load parameters and GP-paths
#source /etc/profile
#source /home/gpadmin/.bashrc

## 
## The next 3 lines are replaced during the installation of
## of sociallinks. It prepends the directory specified 
## in the greenplum.install.path variable of sl.properties
## to /greenplum_path.sh
## 

if [ -f /usr/local/greenplum-db/greenplum_path.sh ]; then
    source /usr/local/greenplum-db/greenplum_path.sh
fi


## USER DEFINED PARAMETERS
DB_NAME=mci_chm_gp
DB_HOST=10.115.26.53
DB_PORT=5432
DB_USER=xsl
DB_PASS=xsl@gp
DB_ADMIN=gpadmin
## Example paarmeter values:
#DB_NAME=jarkko
#DB_HOST=10.100.150.114
#DB_PORT=5432
#DB_USER=xsl
#DB_PASS=xsl@gp
#DB_ADMIN=gpadmin

echo date
echo "Database creation started."

## Navigate to folder that includes all the scripts
#cd $PWD/schema_core

#If database already exists it can be dropped using following command
#echo "Delete database if already exist : psql -d postgres -h $DB_HOST -p $DB_PORT --set ON_ERROR_STOP=1 -c \"drop database if exists $DB_NAME;\""
#psql -d postgres -h $DB_HOST -p $DB_PORT --set ON_ERROR_STOP=1 -c "drop database if exists $DB_NAME;"

echo "Create database and base level tables, views, and functions."
bash create_base.sh $DB_NAME $DB_HOST $DB_PORT $DB_USER $DB_PASS $DB_ADMIN

echo "Create views and functions related to SL analytics modules."
bash create_modules.sh $DB_NAME $DB_HOST $DB_PORT $DB_ADMIN

echo date
echo "Database creation finished."

