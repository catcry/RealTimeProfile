#!/bin/bash
set -e
DATASTORE_SSH="ssh ccacp@10.19.129.73"
DATA_LOADING_PATH="/backup/TO_FAA"

$DATASTORE_SSH <<EOF

cd $DATA_LOADING_PATH
######################## VERIFY THE INPUT DIRS #############################

# files count in the input directories
filesCountInput="$(find input -type f -print | wc -l)";
filesCountValidationInput="$(find validation_input -type f -print | wc -l)";

EOF
MASTER_COPY_PATH=$DATA_LOADING_PATH/master_copy/test

# files count in the input directories
if [ $filesCountInput -eq 0 ] && [ $filesCountValidationInput -eq 0 ]; then
      printFunc "ERROR: There are no files in any input dir"
      exit 10;
elif [ ! $filesCountInput -eq 0 ] && [ ! $filesCountValidationInput -eq 0 ]; then
      printFunc "ERROR: There are files in both input dirs. Must be in one only"
      exit 11;
fi



#Path with Validation
#INPUT_PATH=$DATA_LOADING_PATH/input
#MASTER_PATH=validation_input
#Path without validation
#if [ ! $filesCountInput -eq 0 ]; then
#    MASTER_PATH=input
#    INPUT_PATH=$MASTER_COPY_PATH;
#fi
