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
# 2017-09-10 LZu: changes as per FS 1.7 and items only delivered once like lookups are not included here, changed validation script
set -e

printFunc() {
  echo "[$(date +%Y-%m-%d_%H:%M:%S) $$] $@"
}


## !! NOTICE : Edit the following parameters according to the environment and available data sets etc.
## All the parameters from DB_HOST to SQL_SCRIPTS need to be updated !!
DB_HOST="localhost"                      	# IP address of the db front server
DB_PORT="5432"                           	# Port of the db
DB_NAME="mci_chm_gp"                      	# Database name
DB_USER="xsl"                             	# Database user
DATASTORE_SSH="ssh ccacp@10.19.129.73"
DATA_LOADING_PATH="/backup/TO_FAA"

SCRIPT_PATH=$(dirname "$(readlink -f "$0")")	# Local path on database front server (it is assumed that all scripts are in the same location as $0)
LOAD_DATA_SCRIPT="$SCRIPT_PATH/load_raw_data.sh"			# A sub-script that is executed from this main script several times
LOAD_DATA_CRMDUMP_SCRIPT="$SCRIPT_PATH/load_raw_data_crm.sh"		# customized script for loading crm dump zip files and do ETL conversion for file.
LOAD_DATA_PRODUCT_SCRIPT="$SCRIPT_PATH/load_raw_data_prod_takeup.sh"	# customized script for loading product takeup zip files and do ETL conversion for file.
DATA_NULL_CHAR=""				# A character that is used if field is empty
UNCOMP_SOFT="gunzip"				# A software used to uncompress raw data files, for example gunzip (.gz) or bunzip2 (.bunzip2)
#DATA_COLUMN_DELIMITER=","			# A character that is used as a field separator in the raw data files
                                   
DATA_FILES=(
  "FAA_*.txt.gz"				# 0 CDR daily at 00:00 d-1 multiple files FAA_WWW-SeqNo_YYYYMMDDhhmmss.txt.gz where WWW is source (8+ files)
  "CRM_DUMP*.zip"				# 1 CRM every Sat at 02:00 CRM_DUMP_XXX_YYYYMDhhmmss.txt.gz  where XXX = INCC or BC (pre/post) (2 files)
  "churn_hourly*.zip"				# 2 product_takeup hourly, churn_hourly_XXX_YYYYMMDDhhmm.txt.gz where XXX = INCC or BC (pre/post) (2 files)
  "customer_care*.txt.gz"			# 3 Customer Care Log Weekly on Tuesday, 00:00:00
  "PRE_AGGREGATES*.txt.gz"			# 4 Pre-aggregated Data Monthly on 1st day of the month, 00:00:00 INCC or BC (pre/post) (2 files) 
  "vou*.unl.gz"     				# 5 VOU CDR Daily, 00:00:00 (40k+ files)  timestamp in file is in persian | delimited
  "mgr*.unl.gz"     				# 6 MGR CDR Daily, 00:00:00 (40k+ files)  timestamp in file is in persian | delimited
  "RTD_CUSTPROFILE*.txt.gz"			# 7 CRM (new) from HUAWEI.
  "RTD_SUBSOFFER*.txt.gz"			# 8 Product take-up (new) from HUAWEI.
#  "blacklist*.txt.gz"				# 9 Blacklist (currently not available from MCI). Uncomment this when blacklist becomes available
)
 
DB_TABLES=(
  "tmp.cdr"			# 0
  "tmp.crm"			# 1  
  "tmp.product_takeup"		# 2
  "tmp.customer_care"		# 3
  "tmp.pre_aggregates" 		# 4
  "tmp.topup"			# 5
  "tmp.balance_transfer"	# 6
  "tmp.crm_new"			# 7
  "tmp.product_takeup_new"	# 8
#  "tmp.blacklist"		# 9		# Uncomment this when blacklist becomes available
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
#cd $DATA_LOADING_PATH

######################## COMMAND-LINE ARGS ################################

runId=$1
mode=$2
MASTER_COPY_PATH=$DATA_LOADING_PATH/master_copy/$runId

# ############################ ROLLBACK MODE ################################

# # Do master-copy rollback & Exit
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
	# # if previouss run completed with error (delete files which were put by validator)
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
#modified for remote dir
filesCountInput=`$DATASTORE_SSH "find $DATA_LOADING_PATH/input -type f -print | wc -l"`;
filesCountValidationInput=0 #"$(find validation_input -type f -print | wc -l)"; # this is useless so not changing it.

if [ $filesCountInput -eq 0 ] && [ $filesCountValidationInput -eq 0 ]; then
      printFunc "ERROR: There are no files in any input dir"
      exit 10;
elif [ ! $filesCountInput -eq 0 ] && [ ! $filesCountValidationInput -eq 0 ]; then
      printFunc "ERROR: There are files in both input dirs. Must be in one only"
      exit 11;
fi

#Path with Validation
INPUT_PATH=$DATA_LOADING_PATH/input
MASTER_PATH=validation_input
#Path without validation
if [ ! $filesCountInput -eq 0 ]; then
    MASTER_PATH=input
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
    *port*)  LOAD_DATA_SCRIPT="$SCRIPT_PATH/load_raw_data_portability.sh";;
    *) LOAD_DATA_SCRIPT="$SCRIPT_PATH/load_raw_data.sh";;
  esac	
  case ${ERROR_FILES_PATTERN[$i]} in
    *port*)  UNCOMP_SOFT="";;
    *) UNCOMP_SOFT="gunzip";;
  esac	
  case ${ERROR_FILES_PATTERN[$i]} in
    *vou*|*mgr*|*CRM_DUMP*|*churn_hourly*) DATA_COLUMN_DELIMITER="|";;
    *) DATA_COLUMN_DELIMITER=";";;
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
    *vou*|*mgr*|*CRM_DUMP*|*churn_hourly*|*PRE_AGGREGATES*) DATA_COLUMN_DELIMITER="|";;
    *customer_care*|*RTD_CUSTPROFILE*|*RTD_SUBSOFFER*) DATA_COLUMN_DELIMITER=",";;
    *) DATA_COLUMN_DELIMITER=";";;
  esac
  
  set +e

  if [[ "${DATA_FILES[$i]}" == CRM_DUMP* ]]
  then
    $LOAD_DATA_CRMDUMP_SCRIPT "$DATASTORE_SSH" $INPUT_PATH "${DATA_FILES[$i]}" $UNCOMP_SOFT $INPUT_PATH "$DATA_COLUMN_DELIMITER" "$DATA_NULL_CHAR" ${DB_TABLES[$i]} $DB_NAME $DB_HOST $DB_PORT $DB_USER
  elif [[ "${DATA_FILES[$i]}" == churn_hourly* ]]
  then
    $LOAD_DATA_PRODUCT_SCRIPT "$DATASTORE_SSH" $INPUT_PATH "${DATA_FILES[$i]}" $UNCOMP_SOFT $INPUT_PATH "$DATA_COLUMN_DELIMITER" "$DATA_NULL_CHAR" ${DB_TABLES[$i]} $DB_NAME $DB_HOST $DB_PORT $DB_USER
  else
    $LOAD_DATA_SCRIPT "$DATASTORE_SSH" $INPUT_PATH "${DATA_FILES[$i]}" $UNCOMP_SOFT $INPUT_PATH "$DATA_COLUMN_DELIMITER" "$DATA_NULL_CHAR" ${DB_TABLES[$i]} $DB_NAME $DB_HOST $DB_PORT $DB_USER
  fi
  LOAD_DATA_SCRIPT_EXIT_CODE=$?

  echo $LOAD_DATA_SCRIPT_EXIT_CODE
  if [ "$LOAD_DATA_SCRIPT_EXIT_CODE" -eq 64 ]; then
    # load_data_script did not find one of psql, gunzip (or similar program) or ssh
    printFunc "ERROR: $(basename $LOAD_DATA_SCRIPT) exited with $LOAD_DATA_SCRIPT_EXIT_CODE"
    exit 1;
  fi
  set -e

  #if [ $LOAD_DATA_SCRIPT_EXIT_CODE -ne 0 ]; then
  #  case ${DB_TABLES[$i]} in
  #    *cdr*   ) RM_SQL_SCRIPTS=("process_cdr.sql")                ;;
  #    *crm*   ) RM_SQL_SCRIPTS=("process_crm.sql")                ;;
  #    *topup* ) RM_SQL_SCRIPTS=("process_topup.sql")              ;;
  #    *       ) RM_SQL_SCRIPTS=("")                               ;;
  #  esac
  #  for RM_SQL_SCRIPT in ${RM_SQL_SCRIPTS[@]}; do
  #    if [ -n "$RM_SQL_SCRIPT" ]; then
  #      printFunc "ERROR: Excluding $(basename $RM_SQL_SCRIPT) because $(basename $LOAD_DATA_SCRIPT) exited with code $LOAD_DATA_SCRIPT_EXIT_CODE"
  #      SQL_SCRIPTS=(${SQL_SCRIPTS[@]%%*$RM_SQL_SCRIPT})
  #    fi
  #  done
  #fi
done

#for SQL_SCRIPT in ${SQL_SCRIPTS[@]}; do
#  printFunc "Starting $SQL_SCRIPT"
#  psql -d $DB_NAME -h $DB_HOST -p $DB_PORT -U $DB_USER --set ON_ERROR_STOP=1 -a -f $SQL_SCRIPT
#done

############################# LOAD INPUT PORTABILITY #######################################

DATA_FILES_PORTABILITY=( 	# the files are split due to different structure within
  "PORTIN_NOTPORTED*.csv"   	# 0  Daily, 21:00:00, incremental MSISDNs are provided without country code prefix.
  "PORTIN_ONGOING*.csv"   	# 1  Daily, 21:00:00, incremental MSISDNs are provided without country code prefix.
  "PORTIN_PORTED*.csv"   	# 2  Daily, 21:00:00, incremental MSISDNs are provided without country code prefix.
  "PORTOUT_NOTPORTED*.csv"  	# 3  Daily, 21:00:00, incremental MSISDNs are provided without country code prefix.
  "PORTOUT_ONGOING*.csv"   	# 4  Daily, 21:00:00, incremental MSISDNs are provided without country code prefix.
  "PORTOUT_PORTED*.csv"   	# 5  Daily, 21:00:00, incremental MSISDNs are provided without country code prefix.  
)

DB_TABLES_PORTABILITY=(
  "tmp.portin_notported"   	# 0
  "tmp.portin_ongoing"   	# 1
  "tmp.portin_ported"   	# 2
  "tmp.portout_notported"   	# 3
  "tmp.portout_ongoing"   	# 4
  "tmp.portout_ported"   	# 5
)

DATA_COLUMN_DELIMITER_PORTABILITY="," # A character that is used as a field separator in the raw data files
LOAD_DATA_SCRIPT_PORTABILITY="$SCRIPT_PATH/load_raw_data_portability.sh" # A sub-script that is executed from this main script several times
UNCOMP_SOFT_PORTABILITY="gunzip" # No decompression needed but kept as it is to minimize changes in stadard scripts

COUNT_DATA_SOURCES_PORTABILITY=${#DB_TABLES_PORTABILITY[@]}
for (( i=0; i<${COUNT_DATA_SOURCES_PORTABILITY}; i++ )); do
  printFunc "Loading data source \"${DATA_FILES_PORTABILITY[$i]}\""

  set +e
  $LOAD_DATA_SCRIPT_PORTABILITY "$DATASTORE_SSH" $INPUT_PATH "${DATA_FILES_PORTABILITY[$i]}" $UNCOMP_SOFT_PORTABILITY $INPUT_PATH "$DATA_COLUMN_DELIMITER_PORTABILITY" "$DATA_NULL_CHAR" ${DB_TABLES_PORTABILITY[$i]} $DB_NAME $DB_HOST $DB_PORT $DB_USER
  LOAD_DATA_SCRIPT_PORTABILITY_EXIT_CODE=$?
  echo $LOAD_DATA_SCRIPT_PORTABILITY_EXIT_CODE
  if [ "$LOAD_DATA_SCRIPT_PORTABILITY_EXIT_CODE" -eq 64 ]; then
      # load_data_script did not find one of psql, gunzip (or similar program) or ssh
      printFunc "ERROR: $(basename $LOAD_DATA_SCRIPT_PORTABILITY) exited with $LOAD_DATA_SCRIPT_EXIT_CODE"
      exit 1;
  fi
  set -e
done

##############################################################################  
# When the files are put here by validator
# printFunc "Cleaning up ..."
# $DATASTORE_SSH "rm -rf $DATA_LOADING_PATH/input/*"
# $DATASTORE_SSH "rm -rf $DATA_LOADING_PATH/validation_errors/*"

printFunc "Finishing $0"

