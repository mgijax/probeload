#!/bin/csh -f

#
# Wrapper script to create & load new primers
#
# Usage:  primerload.csh configFile
#

setenv CONFIGFILE $1

source ${CONFIGFILE}

rm -rf ${PROBELOG}
touch ${PROBELOG}

date >> ${PROBELOG}

${PROBELOAD}/probeload.py >>& ${PROBELOG}

date >> ${PROBELOG}

