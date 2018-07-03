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

source ${NOTELOAD}/Configuration
${NOTELOAD}/mginoteload.py ${NOTELOAD_CMD} -I${NOTEINPUTFILE} -M${NOTEMODE} -O${NOTEOBJECTTYPE} -T"${NOTETYPE}"

date >> ${PROBELOG}

