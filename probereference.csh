#!/bin/csh -f

#
# Wrapper script to create & load new probe/references
#
# Usage:  probereference.csh configFile
#

setenv CONFIGFILE $1

source ${CONFIGFILE}

rm -rf ${PROBELOG}
touch ${PROBELOG}

date >> ${PROBELOG}

${PROBELOAD}/probereference.py >>& ${PROBELOG}

date >> ${PROBELOG}

