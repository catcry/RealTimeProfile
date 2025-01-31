#!/bin/bash

# Start the script using following command
# nohup bash EDIT_PATH/database_uploader.sh &> EDIT_PATH/database_uploader_$(date +%F_%R).log &

# ERROR EXIT CODES:
#
# For DataLoading
# 10 - There are no files in any input dir
# 11 - There are files in both input dirs. Must be in one only
# 1- others
#
# For RollBack:
# 100 - The master copy dir doesn't exists
# 101 - The master copy files size is not as stated in the .meta-data
# 1 - others

# VERSION
# 2012-20-26 JTI
# 2017-04-30 LZu: added file inputs. full set of files except topup and portability. device only loaded once. no blacklist files.
# 2017-09-10 LZu: changes as per FS 1.7 and items only delivered once like lookups are not included here, changed validation script.
# 2017-10-05 ZY:  Modified to load lookup tables for MCI project. Loading of lookup tables should be executed manually.
set -e

printFunc() {
  echo "[$(date +%Y-%m-%d_%H:%M:%S) $$] $@"
}


## !! NOTICE : Edit the following parameters according to the environment and avialable data sets etc.
## All the parameters from DB_HOST to SQL_SCRIPTS need to be updated !!
DB_HOST="localhost"                                # IP address of the db front server
DB_PORT="5432"                                     # Port of the db
DB_NAME="mci_chm_gp"                               # Database name
DB_USER="xsl"                                      # Database user
DATASTORE_SSH="ssh ccacp@10.19.129.73"
DATA_LOADING_PATH="/backup/TO_FAA"

SCRIPT_PATH=$(dirname "$(readlink -f "$0")")            # Local path on database front server (it is assumed that all scripts are in the same location as $0)
LOAD_DATA_SCRIPT="$SCRIPT_PATH/load_raw_data_lookup.sh" # A sub-script that is executed from this main script several times
DATA_NULL_CHAR=""                                       # A character that is used if field is empty
UNCOMP_SOFT="gunzip"                                    # A software used to uncompress raw data files, for example gunzip (.gz) or bunzip2 (.bunzip2)
#DATA_COLUMN_DELIMITER="|"                               # A character that is used as a field separator in the raw data files
                                   
DATA_FILES=(
  "churn_status_lookup_bc_*.txt"     # 0 CRM status lookup for postpaid subscribers
  "churn_status_lookup_incc_*.txt"   # 1 CRM status lookup for prepaid subscribers
  "churn_disconn_reason_bc_*.txt"    # 2 CRM disconnection status lookup for postpaid subscribers
  "churn_disconn_reason_incc_*.txt"  # 3 CRM disconnection status lookup for prepaid subscribers
  "churn_bonus_product_bc_*.txt"     # 4 Bonus product lookup postpaid subscribers
  "churn_bonus_product_incc_*.txt"   # 5 Bonus product lookup for prepaid subscribers
  "churn_percent_product_bc_*.txt"   # 6 Discount product lookup for postpaid subscribers
  "churn_percent_product_incc_*.txt" # 7 Discount product lookup for prepaid subscribers
  "NPG_ERROR_CODE_*.csv"             # 8 Portability error code lookup
  "servicerequesttype_*.txt"         # 9 Service request type lookup
  "TCRPLUS*.csv"                     # 10 Device TAC lookup
  "RTD_LOOKUP_[0-9]*.txt"	     # 11 Status/Disconnection reason lookup (new CRM)
  "RTD_LOOKUP_OFFERING*.txt"	     # 12 Product ID lookup (new CRM)
)
 
DB_TABLES=(
  "tmp.prepaid_status_reason"         # 0
  "tmp.postpaid_status_reason"        # 1
  "tmp.prepaid_disconnection_reason"  # 2
  "tmp.postpaid_disconnection_reason" # 3
  "tmp.prepaid_bonus_product_id"      # 4
  "tmp.postpaid_bonus_product_id"     # 5
  "tmp.prepaid_discount_product_id"   # 6
  "tmp.postpaid_discount_product_id"  # 7
  "tmp.portability_error_codes"       # 8
  "tmp.service_request_type"          # 9
  "tmp.device_tac"                    # 10
  "tmp.reason_lookup"		      # 11
  "tmp.product_information"	      # 12
)

# SQL_SCRIPTS=(
# "$SCRIPT_PATH/process_crm.sql"
# "$SCRIPT_PATH/process_cdr.sql"
# "$SCRIPT_PATH/process_topup.sql")

printFunc "Starting $0"

if [ -f /usr/local/greenplum-db/greenplum_path.sh ]; then
    source /usr/local/greenplum-db/greenplum_path.sh
fi

# doesnt do anything since the path is remote
# cd $DATA_LOADING_PATH

######################## COMMAND-LINE ARGS ################################

runId=$1
mode=$2
MASTER_COPY_PATH=$DATA_LOADING_PATH/master_copy/$runId

############################ ROLLBACK MODE ################################

# Do master-copy rollback & Exit
# if [ "$mode" == "rollback" ]; then
	# printFunc "Rolling back using master copy: $MASTER_COPY_PATH"

	# if [ ! -d $MASTER_COPY_PATH ]; then
	      # printFunc "ERROR: The master copy dir doesn't exists"
	      # exit 100;
	# fi

	# dir="$(cat $MASTER_COPY_PATH/.meta-data  | grep dir: | cut -f 2 -d :)"
	# size="$(cat $MASTER_COPY_PATH/.meta-data  | grep size: | cut -f 2 -d :)"
        # realSize="$(du --exclude '.meta-data' $MASTER_COPY_PATH | grep -o '^[0-9]*')"

	# if [ ! $realSize -eq  $size ]; then # check the size
	      # printFunc "ERROR: The master copy files size [$realSize] is not as stated in the .meta-data [$size]"
	      # exit 101;
	# fi
	# if previouss run completed with error (delete files which were put by validator)
	# printFunc "Cleaning up ..."
	# rm -rf input/*
	# rm -rf validation_input/*
	# # copy files back
	# printFunc "Copying master copy ..."
	# cp $MASTER_COPY_PATH/* $DATA_LOADING_PATH/$dir
	# printFunc "Finishing $0"
	# exit 0;
# fi


######################## VERIFY THE INPUT DIRS #############################

# files count in the input directories
# modified for remote dir
filesCountInput=`$DATASTORE_SSH "find $DATA_LOADING_PATH/lookupinput -type f -print | wc -l"`;
filesCountValidationInput=0 #"$(find validation_input -type f -print | wc -l)"; # this is useless so not changing it.

if [ $filesCountInput -eq 0 ] && [ $filesCountValidationInput -eq 0 ]; then
      printFunc "ERROR: There are no files in any input dir"
      exit 10;
elif [ ! $filesCountInput -eq 0 ] && [ ! $filesCountValidationInput -eq 0 ]; then
      printFunc "ERROR: There are files in both input dirs. Must be in one only"
      exit 11;
fi

#Path with Validation
INPUT_PATH=$DATA_LOADING_PATH/lookupinput
MASTER_PATH=validation_input
#Path without validation
if [ ! $filesCountInput -eq 0 ]; then
    MASTER_PATH=lookupinput
    INPUT_PATH=$MASTER_COPY_PATH;
fi

####################### MAKE THE MASTER COPY ##############################
printFunc "Making master copy: $MASTER_COPY_PATH"
$DATASTORE_SSH "mkdir -p $MASTER_COPY_PATH";
# write the master-copy .meta-data
master_path_size=`$DATASTORE_SSH "du $DATA_LOADING_PATH/$MASTER_PATH | grep -o '^[0-9]*'"`

#echo "dir:$MASTER_PATH
#size:$(du $MASTER_PATH | grep -o '^[0-9]*')" > $MASTER_COPY_PATH/.meta-data

echo "dir:$MASTER_PATH
size:$master_path_size" > .meta-data
scp .meta-data ccacp@10.19.129.74:$MASTER_COPY_PATH/.meta-data
rm .meta-data

# move files to the master-copy
$DATASTORE_SSH "mv $DATA_LOADING_PATH/$MASTER_PATH/* $MASTER_COPY_PATH"

########################## VALIDATION #######################################
VALIDATION_ERRORS_PATH=$DATA_LOADING_PATH/validation_errors

#from previous run
printFunc "Cleaning up validation errors dir ..."
rm -rf validation_errors/*

#run validation when there are any files in the 'validation_input'
if [ ! $filesCountValidationInput -eq 0 ]; then
	coresCount="$(cat /proc/cpuinfo | egrep "core id|physical id" | tr -d "\n" | sed s/physical/\\nphysical/g | grep -v ^$ | sort | uniq | wc -l)"
    if [ $coresCount -eq 0 ]; then
        coresCount="$(cat /proc/cpuinfo | grep ^processor | wc -l)"
   	fi
	printFunc "Running data validator on $coresCount cores ..."
	java  -Xms64m -Xmx1024m -Dinput.dir=$MASTER_COPY_PATH -Dthread.count.cdr=$coresCount -jar etl-runner-1.0.0-with-dependencies.jar validation.ktr 
	# do we need etl-runner-1.0-SNAPSHOT-with-dependencies.jar???

	# Merge CDR files if validator splited CDR files into several parts (when ETL flow was run with several copies of 'CDR Valid Output' step)
	# cdr_2012-07-01_0.csv.gz & cdr_2012-07-01_1.csv.gz >>> cdr_2012-07-01.csv.gz
	for cdrFileDate in $( find input -type f -name "*FAA_*_*txt.gz" | cut -f 2 -d _ | sort -u ); do
		 printFunc "Merge cdr parts of ${cdrFileDate}"
		 cat input/cdr_${cdrFileDate}_*.txt.gz > input/cdr_${cdrFileDate}.txt.gz
		 rm input/cdr_${cdrFileDate}_*.txt.gz
	done
	
fi

#################### LOAD VALIDATION ERRORS ##################################
ERROR_FILES_PATTERN="errors_*.txt.gz"
PSQL_COPY_OPTIONS="text"
VALIDATION_FILES=$($DATASTORE_SSH "find $VALIDATION_ERRORS_PATH -maxdepth 1 -name '$ERROR_FILES_PATTERN' -type f -print | sort -f")

if [ -z "$VALIDATION_FILES" ]; then
  printFunc "No validation errors files found, only truncating tables"
  for TTAB in tmp.validation_errors tmp.validation_errors_err; do
    psql -h $DB_HOST -p $DB_PORT -d $DB_NAME -U $DB_USER -a -c "truncate table $TTAB;"
    PSQL_RC=$?
    if [ "$PSQL_RC" -ne 0 ]; then
      printFunc "ERROR: psql command to truncate table $TTAB exited with $PSQL_RC"
      exit 1
    fi
  done
else
  case ${ERROR_FILES_PATTERN[$i]} in
    *port*) LOAD_DATA_SCRIPT="$SCRIPT_PATH/load_raw_data_portability.sh";;
    *) LOAD_DATA_SCRIPT="$SCRIPT_PATH/load_raw_data_lookup.sh";;
  esac	
  case ${ERROR_FILES_PATTERN[$i]} in
    *port*) UNCOMP_SOFT="";;
    *) UNCOMP_SOFT="gunzip";;
  esac
  case ${ERROR_FILES_PATTERN[$i]} in
    *RTD_LOOKUP*) DATA_COLUMN_DELIMITER=",";;
    *) DATA_COLUMN_DELIMITER="|";;
  esac
  printFunc "Loading validation errors into DB ..."
  $LOAD_DATA_SCRIPT "$DATASTORE_SSH" $VALIDATION_ERRORS_PATH $ERROR_FILES_PATTERN $UNCOMP_SOFT $VALIDATION_ERRORS_PATH "$DATA_COLUMN_DELIMITER" "$DATA_NULL_CHAR" "tmp.validation_errors" $DB_NAME $DB_HOST $DB_PORT $DB_USER "$PSQL_COPY_OPTIONS"
  LOAD_DATA_SCRIPT_EXIT_CODE=$?
  echo $LOAD_DATA_SCRIPT_EXIT_CODE
  if [ "$LOAD_DATA_SCRIPT_EXIT_CODE" -eq 64 ]; then
    # load_data_script did not find one of psql, gunzip (or similar program) or ssh
    printFunc "ERROR: $(basename $LOAD_DATA_SCRIPT) exited with $LOAD_DATA_SCRIPT_EXIT_CODE"
    exit 1;
  fi
fi


############################# LOAD INPUT #######################################

COUNT_DATA_SOURCES=${#DB_TABLES[@]}
for (( i=0; i<${COUNT_DATA_SOURCES}; i++ )); do
  printFunc "Loading data source \"${DATA_FILES[$i]}\""
  # NOTICE : If the files have different field separators edit following
  case ${DATA_FILES[$i]} in
    *RTD_LOOKUP*) DATA_COLUMN_DELIMITER=",";;
    *) DATA_COLUMN_DELIMITER="|";;
  esac

  set +e
  $LOAD_DATA_SCRIPT "$DATASTORE_SSH" $INPUT_PATH "${DATA_FILES[$i]}" $UNCOMP_SOFT $INPUT_PATH "$DATA_COLUMN_DELIMITER" "$DATA_NULL_CHAR" ${DB_TABLES[$i]} $DB_NAME $DB_HOST $DB_PORT $DB_USER
  LOAD_DATA_SCRIPT_EXIT_CODE=$?
  echo $LOAD_DATA_SCRIPT_EXIT_CODE
  if [ "$LOAD_DATA_SCRIPT_EXIT_CODE" -eq 64 ]; then
      # load_data_script did not find one of psql, gunzip (or similar program) or ssh
      printFunc "ERROR: $(basename $LOAD_DATA_SCRIPT) exited with $LOAD_DATA_SCRIPT_EXIT_CODE"
      exit 1;
  fi
  set -e
done

##############################################################################  
# When the files are put here by validator
# printFunc "Cleaning up ..."
# $DATASTORE_SSH "rm -rf $DATA_LOADING_PATH/lookupinput/*"
# $DATASTORE_SSH "rm -rf $DATA_LOADING_PATH/validation_errors/*"

printFunc "Finishing $0"

