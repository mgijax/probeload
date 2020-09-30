
#
# Program: probeassay.py
#
# Original Author: Lori Corbani
#
# Purpose:
#
#	Given 2 probes A & B:
#
#	.  Add alias to probe B using probe A's name, J:
#	.  Move probe A's assay associations to probe B
#	.  Move probe A's references to probe B (assumes no duplicates)
#	.  Delete probe A
#	
#	probe A/gene == probe B/gene == assay/gene
#	if the genes for probe A, probe B and the assay do not match,
#		return with error
#
# Requirements Satisfied by This Program:
#
# Usage:
#	probeassay.py
#
# Envvars:
#
# Inputs:
#
#	A tab-delimited file in the format:
#		field 1:  MGI Probe ID A (from)
#		field 2:  Probe A name
#		field 3:  MGI Probe ID B (to)
#		field 4:  J: (for alias)
#		field 5:  Created by
#
# Outputs:
#
#       2 BCP files:
#
#       PRB_Reference.bcp               Probe Reference records
#       PRB_Alias.bcp                   Probe Alias records
#
#       Diagnostics file of all input parameters and SQL commands
#       Error file
#
# Exit Codes:
#
# Assumes:
#
#	That no one else is adding such records to the database.
#
# Bugs:
#
# Implementation:
#
# History
#
# 02/02/2010	lec
#	- TR9931/Eurexpress/new
#

import sys
import os
import string
import accessionlib
import db
import mgi_utils
import loadlib
import sourceloadlib

#globals

#
# from configuration file
#
user = os.environ['PG_DBUSER']
passwordFileName = os.environ['PG_1LINE_PASSFILE']
mode = os.environ['PROBELOADMODE']
currentDir = os.environ['PROBELOADDIR']
inputFileName = os.environ['PROBEDATAFILE']
outputDir = os.environ['PROBELOADDATADIR']

bcpCommand = os.environ['PG_DBUTILS'] + '/bin/bcpin.csh '

bcpon = 1  

DEBUG = 0		# if 0, not in debug mode
TAB = '\t'		# tab
CRT = '\n'		# carriage return/newline

diagFile = ''		# diagnostic file descriptor
errorFile = ''		# error file descriptor
inputFile = ''		# file descriptor

refFile = ''            # file descriptor
aliasFile = ''          # file descriptor

refTable = 'PRB_Reference'
aliasTable = 'PRB_Alias'

refFileName = refTable + '.bcp'
aliasFileName = aliasTable + '.bcp'

diagFileName = ''	# diagnostic file name
errorFileName = ''	# error file name

refKey = 0              # PRB_Reference._Reference_key
aliasKey = 0            # PRB_Alias._Alias_key

mgiTypeKey = '3'
updateAssaySQL = '''update GXD_ProbePrep set _Probe_key = %s where _Probe_key = %s'''
updateRefSQL = '''update PRB_Reference set _Probe_key = %s where _Probe_key = %s and _Refs_key != %s'''
deleteProbeSQL = '''delete PRB_Probe from PRB_Probe where _Probe_key = %s'''

execAssaySQL = []
execRefSQL = []
execProbeSQL = []

loaddate = loadlib.loaddate

# Purpose: prints error message and exits
# Returns: nothing
# Assumes: nothing
# Effects: exits with exit status
# Throws: nothing

def exit(
    status,          # numeric exit status (integer)
    message = None   # exit message (str.
    ):

    if message is not None:
        sys.stderr.write('\n' + str(message) + '\n')
 
    try:
        diagFile.write('\n\nEnd Date/Time: %s\n' % (mgi_utils.date()))
        errorFile.write('\n\nEnd Date/Time: %s\n' % (mgi_utils.date()))
        diagFile.close()
        errorFile.close()
        inputFile.close()
    except:
        pass

    db.useOneConnection(0)
    sys.exit(status)
 
# Purpose: process command line options
# Returns: nothing
# Assumes: nothing
# Effects: initializes global variables
#          exits if files cannot be opened
# Throws: nothing

def init():
    global bcpCommand
    global diagFile, errorFile, inputFile, errorFileName, diagFileName
    global refFile, aliasFile
 
    db.useOneConnection(1)
    db.set_sqlUser(user)
    db.set_sqlPasswordFromFile(passwordFileName)
 
    bcpCommand = bcpCommand + db.get_sqlServer() + ' ' + db.get_sqlDatabase() + ' %s ' + currentDir + ' %s "\\t" "\\n" mgd'

    head, tail = os.path.split(inputFileName) 

    diagFileName = outputDir + '/' + tail + '.diagnostics'
    errorFileName = outputDir + '/' + tail + '.error'

    try:
        diagFile = open(diagFileName, 'w')
    except:
        exit(1, 'Could not open file %s\n' % diagFileName)
                
    try:
        errorFile = open(errorFileName, 'w')
    except:
        exit(1, 'Could not open file %s\n' % errorFileName)
                
    try:
        inputFile = open(inputFileName, 'r')
    except:
        exit(1, 'Could not open file %s\n' % inputFileName)

    try:
        refFile = open(refFileName, 'w')
    except:
        exit(1, 'Could not open file %s\n' % refFileName)

    try:
        aliasFile = open(aliasFileName, 'w')
    except:
        exit(1, 'Could not open file %s\n' % aliasFileName)

    # Log all SQL
    db.set_sqlLogFunction(db.sqlLogAll)

    diagFile.write('Start Date/Time: %s\n' % (mgi_utils.date()))
    diagFile.write('Server: %s\n' % (db.get_sqlServer()))
    diagFile.write('Database: %s\n' % (db.get_sqlDatabase()))

    errorFile.write('Start Date/Time: %s\n\n' % (mgi_utils.date()))

    return

# Purpose: verify processing mode
# Returns: nothing
# Assumes: nothing
# Effects: if the processing mode is not valid, exits.
#	   else, sets global variables
# Throws:  nothing

def verifyMode():

    global DEBUG

    if mode == 'preview':
        DEBUG = 1
        bcpon = 0
    elif mode != 'load':
        exit(1, 'Invalid Processing Mode:  %s\n' % (mode))

# Purpose:  sets global primary key variables
# Returns:  nothing
# Assumes:  nothing
# Effects:  sets global primary key variables
# Throws:   nothing

def setPrimaryKeys():

    global refKey, aliasKey

    results = db.sql('''select nextval('prb_reference_seq') as maxKey''', 'auto')
    refKey = results[0]['maxKey']

    results = db.sql('''select nextval('prb_alias_seq') as maxKey''', 'auto')
    aliasKey = results[0]['maxKey']

# Purpose:  BCPs the data into the database
# Returns:  nothing
# Assumes:  nothing
# Effects:  BCPs the data into the database
# Throws:   nothing

def bcpFiles():

    diagFile.write(execAssaySQL)
    diagFile.write(execRefSQL)
    diagFile.write(execProbeSQL)

    if DEBUG or not bcpon:
        return

    refFile.close()
    aliasFile.close()

    db.commit()

    # execute the sql commands

    # move assay information from fromID to toID
    for r in execAssaySQL:
        db.sql(r, None)

    # move fromID (from) references to toID
    db.sql(execRefSQL, None)

    # delete fromID (from)
    db.sql(execProbeSQL, None)

    db.commit()

    bcp1 = bcpCommand % (refTable, refFileName)
    bcp2 = bcpCommand % (aliasTable, aliasFileName)

    for bcpCmd in [bcp1, bcp2]:
        diagFile.write('%s\n' % bcpCmd)
        os.system(bcpCmd)

    # update prb_reference_seq auto-sequence
    db.sql('''select setval('prb_reference_seq', (select max(_Reference) from PRB_Reference))''', None)
    db.commit()

    # update prb_alias_seq auto-sequence
    db.sql('''select setval('prb_alias_seq', (select max(_Alias_key) from PRB_Alias))''', None)
    db.commit()

    return

# Purpose:  processes data
# Returns:  nothing
# Assumes:  nothing
# Effects:  verifies and processes each line in the input file
# Throws:   nothing

def processFile():

    global refKey, aliasKey
    global execProbeSQL
    global execAssaySQL
    global execRefSQL

    lineNum = 0
    # For each line in the input file

    for line in inputFile.readlines():

        error = 0
        lineNum = lineNum + 1

        # Split the line into tokens
        tokens = str.split(line[:-1], '\t')

        try:
            fromID = tokens[0]
            name = tokens[1]
            toID = tokens[2]
            jnum = tokens[3]
            createdBy = tokens[4]
        except:
            exit(1, 'Invalid Line (%d): %s\n' % (lineNum, line))

        fromKey = loadlib.verifyObject(fromID, mgiTypeKey, None, lineNum, errorFile)
        toKey = loadlib.verifyObject(toID, mgiTypeKey, None, lineNum, errorFile)
        referenceKey = loadlib.verifyReference(jnum, lineNum, errorFile)
        createdByKey = loadlib.verifyUser(createdBy, lineNum, errorFile)

        if fromKey == 0:
            errorFile.write('Invalid Probe "From":  %s\n' % (fromID))
            error = 1

        if toKey == 0:
            errorFile.write('Invalid Probe "To":  %s\n' % (toID))
            error = 1

        if referenceKey == 0:
            errorFile.write('Invalid Reference:  %s\n' % (jnum))
            error = 1

        if createdByKey == 0:
            errorFile.write('Invalid Creator:  %s\n\n' % (createdBy))
            error = 1

        # check that all genes are the same
        checkGenesSQL = '''
                        select f.*
                        from PRB_Marker f, PRB_Marker t, GXD_ProbePrep p, GXD_Assay a
                        where f._Probe_key = %s
                        and t._Probe_key = %s
                        and p._Probe_key = %s
                        and p._ProbePrep_key = a._ProbePrep_key
                        and f._Marker_key = t._Marker_key
                        and f._Marker_key = a._Marker_key
                        ''' % (fromKey, toKey, fromKey)

        checkGenes = db.sql(checkGenesSQL, 'auto')
        if len(checkGenes) == 0:
            errorFile.write('Gene of GenePaint, Eurexpress and Assay are not the same:  %s, %s\n' % (fromID, toID))
            error = 1

        # check that the J: is on at least one Assay
        checkJAssaySQL = '''
                         select a.*
                         from GXD_ProbePrep p, GXD_Assay a
                         where p._Probe_key = %s
                         and p._ProbePrep_key = a._ProbePrep_key
                         and a._Refs_key = %s
                         ''' % (fromKey, referenceKey)

        checkJAssay = db.sql(checkJAssaySQL, 'auto')
        if len(checkJAssay) == 0:
            errorFile.write('J: is not on any Assays attached to the probe:  %s\n' % (fromID))
            error = 1

        # if errors, continue to next record
        if error:
            continue

        # add alias using fromID name (from) to toID

        refFile.write('%s\t%s\t%s\t0\t0\t%s\t%s\t%s\t%s\n' \
                % (refKey, toKey, referenceKey, createdByKey, createdByKey, loaddate, loaddate))
        aliasFile.write('%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
                % (aliasKey, refKey, name, createdByKey, createdByKey, loaddate, loaddate))
        refKey = refKey + 1
        aliasKey = aliasKey + 1

        # move assay information from fromID to toID
        execAssaySQL.append(updateAssaySQL % (toKey, fromKey))

        # move fromID (from) references to toID
        execRefSQL.append(updateRefSQL % (toKey, fromKey, referenceKey))

        # delete fromID (from)
        execProbeSQL.append(deleteProbeSQL % (fromKey))

    #	end of "for line in inputFile.readlines():"

#
# Main
#

init()
verifyMode()
setPrimaryKeys()
processFile()
bcpFiles()
exit(0)
