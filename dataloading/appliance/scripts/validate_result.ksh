#!/bin/ksh
# set -x

# variables to defined during installation (extracted from sl.properties)
GREENPLUM_INSTALL_PATH=/usr/local/greenplum-db
DATALOADING_HOST_PATH=/home/xsl/dataloading/
ANALYTICS_CONNECTION_USERNAME=xsl
DATALOADING_HOST=10.115.26.53
# following two variable values should be extracted from analytics.connection.url
ANALYTICS_DB_NAME=mci_chm_gp
ANALYTICS_DB_PORT=5432
# end of variables to defined during installation


# source ./appliance_env

source $GREENPLUM_INSTALL_PATH/greenplum_path.sh

LOG=$PWD/validation.log

EXPECTED_PATH=$DATALOADING_HOST_PATH/appliance/expected_results
ACTUAL_PATH=$DATALOADING_HOST_PATH/appliance/actual_results


function verification_failed {
    echo "Verification of Churn Appliance installation failed." >&2
    echo "see \"$LOG\" for details" >&2
    echo "" >> $LOG
    echo "End of verification: "`date` >> $LOG
    echo "" >> $LOG
    exit 1
}

function was_ok {
    ret=$1
    command=$2
    if [ "$ret" != 0 ]; then
        echo ">>>>>> ERROR: command \"$command\" failed with return value \"$ret\"" >&2
        verification_failed;
    fi
}

function import_one_file {
    LOADID=$1
    USECASE=$2
    CSVFILE=$3

    # importing expected result files
    cmd="./import_result_file.ksh -d $ANALYTICS_DB_NAME -p $ANALYTICS_DB_PORT -u $ANALYTICS_CONNECTION_USERNAME -H $DATALOADING_HOST -i $LOADID -c $USECASE -f $CSVFILE" 2>&1
    $cmd
    was_ok $? "$cmd"
}

function import {
    # importing expected result files
    import_one_file expected churn_postpaid $EXPECTED_PATH/churn_postpaid_verification.csv
    import_one_file expected churn_inactivity $EXPECTED_PATH/churn_inactivity_verification.csv

    # importing actual result files
    import_one_file actual churn_postpaid $ACTUAL_PATH/churn_postpaid_*.csv
    import_one_file actual churn_inactivity $ACTUAL_PATH/churn_inactivity_*.csv
}

echo "" >> $LOG
echo "Start of verification: "`date` >> $LOG
echo "" >> $LOG

# create appliance db schema
cmd="psql -d $ANALYTICS_DB_NAME -p $ANALYTICS_DB_PORT -U $ANALYTICS_CONNECTION_USERNAME -h $DATALOADING_HOST --set ON_ERROR_STOP=1 -f appliance_db_schema.sql"
$cmd 2>>$LOG 1>&2
was_ok $? "$cmd"

# import the expected and actual result files
import >> $LOG 

# verify the result
RESULT=`psql -d $ANALYTICS_DB_NAME -p $ANALYTICS_DB_PORT -U $ANALYTICS_CONNECTION_USERNAME -h $DATALOADING_HOST -q -t --set ON_ERROR_STOP=1 -c "select appliance.verify();" 2>>$LOG`
was_ok $? "psql -d $ANALYTICS_DB_NAME -p $ANALYTICS_DB_PORT -U $ANALYTICS_CONNECTION_USERNAME -h $DATALOADING_HOST -q -t --set ON_ERROR_STOP=1 -c select appliance.verify();"

RESULT=`echo $RESULT`   # remove any spaces from RESULT

if [ "$RESULT" = t ]; then
    echo "Verification of Churn Appliance installation succeeded."
    echo "" >> $LOG
    echo "End of verification: "`date` >> $LOG
    echo "" >> $LOG
else
    verification_failed
fi

exit 0

