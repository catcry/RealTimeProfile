#!/bin/ksh
# set -x

PROGNAME=$0

DB_HOST=
DB_PORT=
DB_USER=xsl
DB_NAME=
TABLE_NAME=

LOAD_ID=actual

GPSQL_CMD=

DELIMITER=";"
NULLCHAR=""
ERRORS=0

function show_usage {
    echo ""
    echo "Usage: $PROGNAME -c <use_case> -d <db_name> -f <csv-file> [-i <load_id>] [ -n <null_string> ] [-u <db_user>] -H <db_host>] -p <db_port> [-h]"
    echo "-f <csv_file>       : path and name of csv data file."
    echo "-c <use_case>       : Name of the usecase: churn_postpaid or churn_inactivity."
    echo "-d <db_name>        : Name of the database."
    echo "-p <db_port>        : Database port."
    echo "-i <load_id>        : Id for the load, Default: actual"
    echo "-n <null_string>    : string representing the NULL (e.g. null)" 
    echo "-u <db_user>        : db user code (Default = xsl)"
    echo "-H <db_host>        : db host"
    echo "-h                  : this help"
    echo ""
}

function check_params {
# set -x
    while getopts "c:d:f:i:n:p:u:H:h" OPT
    do
      case $OPT in
          c) USECASE=$OPTARG ;;
          d) DB_NAME=$OPTARG ;;
          f) CSV_FILE=$OPTARG ;;
          i) LOAD_ID=$OPTARG ;;
          n) NULLCHAR=$OPTARG ;;
          p) DB_PORT=$OPTARG ;;
          u) DB_USER=$OPTARG ;;
          H) DB_HOST=$OPTARG ;;
          h) show_usage; exit 0 ;;
          *) show_usage; exit 1 ;;
      esac
    done

    if [ "$DB_NAME" = "" ]; then
        echo "ERROR >>>>>: mandatory parameter <db_name> missing"
        ERRORS=1
    fi

    if [ "$DB_HOST" = "" ]; then
        echo "ERROR >>>>>: mandatory parameter <db_host> missing"
        ERRORS=1
    fi

    if [ "$DB_PORT" = "" ]; then
        echo "ERROR >>>>>: mandatory parameter <db_port> missing"
        ERRORS=1
    fi

    if [ "$CSV_FILE" = "" ]; then
        echo "ERROR >>>>>: mandatory parameter <csv_file> missing"
        ERRORS=1
    fi

    if [ "$USECASE" = "" ]; then
        echo "ERROR >>>>>: mandatory parameter <use_case> missing"
        ERRORS=1
    elif [[ "$USECASE" != churn_postpaid ]] && [[ "$USECASE" != churn_inactivity ]]; then
        echo "ERROR >>>>>: wrong value \"$USECASE\" for parameter <use_case>"
        ERRORS=1
    fi

    if [ "$ERRORS" = 1 ]; then
        echo "Program aborted because of errors"
        show_usage
        exit 1
    fi

    GPSQL_CMD="psql -U $DB_USER -d $DB_NAME -h $DB_HOST -p $DB_PORT"

}

check_params "$@";


#-- import from file to greenplum


# first sed command cuts out the fields form number 3 to the end of line (only msisdn and propensity_score are kept)
# second sed command adds load_id and usecase as new columns

if [ ! -f "$CSV_FILE" ]; then
    echo "ERROR >>>>>: csv-file \"$CSV_FILE\" does not exist"
    exit 1
fi

cat $CSV_FILE | sed 's/^\([^;]*;[^;]*\);.*/\1/' |  sed 's/^\(.*\)$/'${LOAD_ID}${DELIMITER}${USECASE}${DELIMITER}'\1/' |
    $GPSQL_CMD -c "SET CLIENT_ENCODING TO 'latin1'; COPY appliance.churn_verification FROM stdin WITH HEADER DELIMITER AS E'$DELIMITER' NULL AS '$NULLCHAR' ;"
RC=$?
if [ "$RC" -ne 0 ]; then
    echo "ERROR >>>>>: Loading of $CSV_FILE failed"
    exit 1;
fi


COUNT=`$GPSQL_CMD -q -t --set ON_ERROR_STOP=1 -c "select count(*) from appliance.churn_verification where id = '"$LOAD_ID"' and usecase = '"$USECASE"';"`
RC=$?
if [ "$RC" -ne 0 ]; then
    echo "ERROR >>>>>: Getting number of loaded rows for $CSV_FILE failed"
    exit 1;
fi

echo "Number of loaded rows for usecase \"$USECASE\" and load_id =\"$LOAD_ID\": $COUNT"


