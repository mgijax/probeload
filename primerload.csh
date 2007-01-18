#!/bin/csh -f

#
# Wrapper script to create & load new primers
#
# Usage:  primerload.csh configFile
#

setenv CONFIGFILE $1

source ${CONFIGFILE}

rm -rf ${PRIMERLOG}
touch ${PRIMERLOG}

date >> ${PRIMERLOG}

${PROBELOAD}/primerload.py >>& ${PRIMERLOG}

date >> ${PRIMERLOG}

