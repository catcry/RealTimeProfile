#!/bin/bash
set -o pipefail

# This script loads compressed files from disc to database and then moves the
# files to another place. The target and error tables are truncated before the
# loading, and they are vacuumed and analyzed afterwards. The contents of the
# error table is also summarized. Please note that this script adds a new
# column to the data (the first column) which holds the name of the file from
# which the line originates. Take this into account in the create-table query.

# VERSION
# 2012-20-26 JTI
# 2017-09-10 LZu: | iconv -f ARABIC -t UTF8//IGNORE is only option but ruins persian text, no decompression 

help() {
  echo "arguments: {datastore ssh} {datastore path} {file pattern} {uncompress software} {datastore archive path} {delimiter} {nullchar} {database table} {database name} {database host} {database port} {database user} {characters to be removed}"
}

printFunc() {
  echo "[$(date +%Y-%m-%d_%H:%M:%S) $$] $@"
}

# ! NOTICE : There should not be any reason to edit following parameters since they are given as arguments to the scripts !
if [ $# -lt 12 ]; then
  help
  exit
else
  DATASTORE_SSH=$1          # Datastore server: "ssh dataloader@10.100.150.96", local server: "/bin/bash -c"
  DATASTORE_PATH=$2         # Files are in this directory.
  FILE_PATTERN=$3           # Files look like this, i.e. the files can detected by using gfiven pattern for examle *cdr*
  UNCOMP_SOFT=$4            # software that is used to uncompress raw data files, for example gunzip (.gz) or bunzip2 (.bz2)
  DATASTORE_ARCHIVE_PATH=$5 # Files go to this directory. If $DATASTORE_PATH is given, files stay there.
  DELIMITER=$6              # A character that is used as a field separator in the raw data files
  NULLCHAR=$7               # A charater that is used if field is empty
  TABLE=$8                  # The name of the table in which data is uploaded
  DB_NAME=$9                # The name of the database
  DB_HOST=${10}             # The IP address of the database front server
  DB_PORT=${11}             # The port of the database
  DB_USER=${12}             # The database user
  PSQL_COPY_OPTIONS=${13}   # Additional options for the COPY command. like:"CSV HEADER"
  if [ $# -eq 13 ]; then    # REMCHARS = A character pattern that is removed from each line if defined
    REMCHARS=''
  else
    REMCHARS=${14}
  fi
  printFunc "Starting to execute script $0"
  printFunc "Using parameters:"
  echo "  DATASTORE_SSH=$DATASTORE_SSH"
  echo "  DATASTORE_PATH=$DATASTORE_PATH"
  echo "  FILE_PATTERN=$FILE_PATTERN"
  echo "  DATASTORE_ARCHIVE_PATH=$DATASTORE_ARCHIVE_PATH"
  echo "  DELIMITER=$DELIMITER"
  echo "  NULLCHAR=$NULLCHAR"
  echo "  TABLE=$TABLE"
  echo "  DB_NAME=$DB_NAME"
  echo "  DB_HOST=$DB_HOST"
  echo "  DB_PORT=$DB_PORT"
  echo "  DB_USER=$DB_USER"
  echo "  REMCHARS=$REMCHARS"
fi

# # Check if requred binaries are present
# MUST_BINARIES="psql $UNCOMP_SOFT ssh"
# for must_binaries in $MUST_BINARIES
# do
  # which $must_binaries &>/dev/null
  # if [ $? -ne 0 ]; then
    # printFunc "ERROR: $must_binaries : is not found"
    # exit 64;
  # fi
# done

# Define beginning of the psql command
PSQL_CMD="psql -h $DB_HOST -p $DB_PORT -d $DB_NAME -U $DB_USER -a -c"
echo "The beginning of the psql commnad : $PSQL_CMD"

# The error table
TABLE_ERROR="${TABLE}_err"

# Fetch raw data files that are uploaded to the database
DATASTORE_FILES=$($DATASTORE_SSH "find $DATASTORE_PATH -maxdepth 1 -name '$FILE_PATTERN' -type f -print | sort -f")

printFunc "Truncating the target and error tables"
$PSQL_CMD "TRUNCATE TABLE $TABLE;"
$PSQL_CMD "TRUNCATE TABLE $TABLE_ERROR;"

if [ -z "$DATASTORE_FILES" ]; then
  printFunc "ERROR: Files could not be found, with pattern " $FILE_PATTERN
  exit 65;
fi

printFunc "Copying files to database $DB_NAME table $TABLE"

if [ "$DELIMITER" = "," ]; then
  DELIMITER_SED='\,'
else
  DELIMITER_SED=$DELIMITER
fi

typeset -i PROCESSED_FILES=0
typeset -i LOADED_FILES=0
typeset -i ERROR_FILES=0
EXIT_CODE=0
for FILE in ${DATASTORE_FILES[@]}; do
  let PROCESSED_FILES=PROCESSED_FILES+1
  BASE_FILE=$(echo $FILE | xargs -n1 basename)
  if $DATASTORE_SSH "cat $FILE" | iconv -f ARABIC -t UTF8//IGNORE | sed "s,^,${BASE_FILE}${DELIMITER_SED},g" | tr -d "$REMCHARS" | $PSQL_CMD "SET CLIENT_ENCODING TO 'utf8'; COPY $TABLE FROM stdin WITH DELIMITER AS E'$DELIMITER' NULL AS '$NULLCHAR' $PSQL_COPY_OPTIONS LOG ERRORS INTO $TABLE_ERROR KEEP SEGMENT REJECT LIMIT 5000;"; then # UNCOMP_SOFT removed
     if [ "$DATASTORE_ARCHIVE_PATH" != "$DATASTORE_PATH" ]; then
        $DATASTORE_SSH "mv $FILE $DATASTORE_ARCHIVE_PATH"
        printFunc "File $FILE loaded to database and moved to $DATASTORE_ARCHIVE_PATH"
     fi
     let LOADED_FILES=LOADED_FILES+1
  else
      printFunc " ERROR: File $FILE was not loaded. Pipestatus = ( ${PIPESTATUS[@]} )"
      ## POSSIBLE DEV : Collect the names of unsuccessfully loaded files into one file
      #$DATASTORE_SSH "echo $FILE >> $DATASTORE_ERROR_FILE"
      let ERROR_FILES=ERROR_FILES+1
      EXIT_CODE=66
  fi
done

printFunc "Vacuuming/analyzing the target and error tables"
$PSQL_CMD "VACUUM ANALYZE $TABLE;"
$PSQL_CMD "VACUUM ANALYZE $TABLE_ERROR;"

printFunc "Processed files"

echo "Number of processed files:   $PROCESSED_FILES"
echo "Number of loaded files:      $LOADED_FILES"
echo "Number of files with errors: $ERROR_FILES"

printFunc "Checking the error table"
$PSQL_CMD "SELECT relname, errmsg, count(*) FROM $TABLE_ERROR GROUP BY 1, 2 ORDER BY 1, 2;"

printFunc "Checking the loaded table"
$PSQL_CMD "SELECT count(*) FROM $TABLE;"

# printFunc "Printing the error file list"
# $DATASTORE_SSH "cat $DATASTORE_ERROR_FILE"

printFunc "The execution of script $0 is finished with the following exit code:"
exit $EXIT_CODE;
