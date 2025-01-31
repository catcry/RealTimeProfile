#!/bin/ksh

###############################################
#
# Greenplum DB High Availability Interface
#
# (c) 2013 Comptel Plc
#
###############################################
# Beginning of variables #############
##############################
######################
##############
####


###############################################
#
# Initialising general variables (please check and configure all variables)
#
###############################################

###############################################
#
# Variables for Greenplum DB connection
#
###############################################

GPHOME=<CHANGE_ME>; export GPHOME
GPUSER=<CHANGE_ME>; export GPUSER

CONTROL_GREENPLUM_PREFIX="su - ${GPUSER} -c "

GPPASSWORD=<CHANGE_ME>; export GPPASSWORD
GPHOST=<CHANGE_ME>; export GPHOST
GPPORT=<CHANGE_ME>; export GPPORT
GPDATABASE=<CHANGE_ME>; export GPDATABASE
GPMASTERDIR=<CHANGE_ME>; export GPMASTERDIR

MASTER_HOST_FILE="${0%/*}/master.host"
STANDBY_HOST_FILE="${0%/*}/standby.host"
LOGFILE=/comptel/projects/${GPUSER}/gpAdminLogs/gp_ha.log

if [ ! -f $MASTER_HOST_FILE ]; then
  CURRENT_HOST=$(uname -n)
  echo "${CURRENT_HOST}" > ${MASTER_HOST_FILE}
fi
GPMASTER=$(cat ${MASTER_HOST_FILE})

if [ -f $STANDBY_HOST_FILE ]; then
  GPSTANDBY=$(cat ${STANDBY_HOST_FILE})
else 
  GPSTANDBY=""
fi

# Run profile to get environment correct
. ${GPHOME}/greenplum_path.sh

#####
##############
######################
##############################
# End of variables ###############
###############################################

###############################################
#
# Control functions for Greenplum DB
#
###############################################
checkGreenplum() {
RESULT=$(${CONTROL_GREENPLUM_PREFIX} "gpstate -f")
STATE=$(echo "$RESULT" | grep 'Summary state' | cut -f4 -d' ')
if [ "x$STATE" != "xSynchronized" ]; then
  print_log "Greenplum connection is not available"
  exit 3
fi

if [ "x$GPSTANDBY" = "x" ]; then
  STANDBY=$(echo "$RESULT" |grep 'Standby address' | cut -f2 -d'=' | sed -e 's/^ *//')
  if [ "x$STANDBY" != "x" ]; then
    print_log "New standby $STANDBY detected"
    echo "$STANDBY" > ${STANDBY_HOST_FILE}
  fi
fi

}

restartGreenplum() {
print_log "Restarting Greenplum"
# Restart commands for Greenplum
${CONTROL_GREENPLUM_PREFIX} "gpstop -a -r"
if [ $? -ne 0 ]; then
  print_log "Restarting Greenplum failed"
else
  print_log "Restarting Greenplum succeeded"
fi
}

analyzeGreenplum() {
PGPASSWORD=${GPPASSWORD}
RESULT=$(psql -h ${GPHOST} -p ${GPPORT} -d ${GPDATABASE} -U ${GPUSER} -c 'ANALYZE;')
if [ $? -ne 0 ]; then
  print_log "PSQL command failed  - ${RESULT}"
  return 1

else
  print_log "PSQL command succeeded"
  return 0
fi
}

promoteStandByMaster() {
RESULT=$(${CONTROL_GREENPLUM_PREFIX} "gpactivatestandby -a -d \${MASTER_DATA_DIRECTORY}")
if [ $? -ne 0 ]; then
  let COUNT=$(echo $RESULT |grep  -c 'This may have been caused by a kill -9 of the master postgres process')
  if [ $COUNT -gt 0 ]; then
    gpssh -h ${GPMASTER} -u root -e "rm -fr /tmp/.s.PGSQL.${GPPORT}*"
    gpssh -h ${GPMASTER} -u root -e "rm -fr ${GPMASTERDIR}/postmaster.pid"
    RESULT=$(${CONTROL_GREENPLUM_PREFIX} "gpactivatestandby -a -f -d \${MASTER_DATA_DIRECTORY}")
    if [ $? -ne 0 ]; then
        print_log "Promoting (forced) Greenplum standby master failed - $RESULT"
        exit 3
    else
        print_log "Standby master promotion succeeded after cleanup"
    fi
  else
    print_log "Promoting Greenplum standby master failed - $RESULT"
    exit 3
  fi
else
  print_log "Standby master promotion succeeded"
fi
}

###############################################
#
# Print Log messages to $LOGFILE
#
###############################################
print_log() {
    current_time=`date '+%Y/%m/%d %H:%M:%S'`
    current_host=`hostname`
    current_name=`whoami`
    echo "$current_time: [${current_name}@${current_host}] $1" | tee -a $LOGFILE
}

###############################################
#
# Print Usage
#
###############################################
usage() {
    echo "USAGE:"
    echo "$0 [ start | stop | status | try-restart | restart | reload | force-reload ]"
    echo
}

###############################################
#
# Main
#
###############################################
if [ $# -ne 1 ]; then usage; exit 1; fi;
case "$1" in
    start)

    print_log "Received START order..."

    CURRENT_HOST=$(uname -n)
    if [ "x$GPSTANDBY" != "x" ] && [ "x${CURRENT_HOST}" = "x${GPSTANDBY}" ]; then
      promoteStandByMaster
      analyzeGreenplum
      echo "${CURRENT_HOST}" > ${MASTER_HOST_FILE}
      echo "" > ${STANDBY_HOST_FILE}
    fi
    checkGreenplum
    ;;

    stop)

    print_log "Received STOP order..."
    
    ;;

    status)

    #print_log "Received STATUS order..."

    checkGreenplum
    ;;

    restart)
    
    print_log "Received RESTART order..."

    #restartGreenplum
    ;;

    try-restart)
    
    print_log "Greenplum try-restart not yet supported"
    ;;

    reload)
    
    print_log "Greenplum reload not yet supported"
    ;;

    force-reload)
    
    print_log "Greenplum force-reload not yet supported"
    ;;

    *)

    usage
    exit 127
    ;;
esac
