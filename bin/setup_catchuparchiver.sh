#/bin/bash
BASE=/app
source /home/ARC/miniconda3/bin/activate
source $OSPL_HOME/release.com
export PYTHONPATH=$BASE/dm_csc_base/python:$BASE/dm_CatchupArchiver/python:$PYTHONPATH
export PATH=$BASE/dm_csc_base/bin:$BASE/dm_CatchupArchiver/bin:$PATH
export DM_CATCHUPARCHIVER_DIR=$BASE/dm_CatchupArchiver
export DM_CSC_BASE_DIR=$BASE/dm_csc_base
export DM_CONFIG_CATCHUP_DIR=$BASE/dm_config_catchup
LOGPATH=/tmp/ospl_logs.$$
mkdir $LOGPATH
export OSPL_LOGPATH=$LOGPATH
export HOME=/tmp
