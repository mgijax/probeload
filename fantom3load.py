#!/usr/local/bin/python

#
# Program: fantom3load.py
#
# Original Author: Lori Corbani
#
# Purpose:
#
#	To load new Probes into:
#
#	PRB_Probe
#	PRB_Reference
#	ACC_Accession
#	ACC_AccessionReference
#
# Requirements Satisfied by This Program:
#
# Usage:
#	fantom3load.py
#
# Envvars:
#
# Inputs:
#
#	A tab-delimited file in the format:
#		field 1:  Probe Name
#		field 2:  MGI ID of Clone
#		field 3:  Reference (J:#####)
#		field 4:  Library Name
#		field 5:  Region Covered
#		field 6:  Insert Site
#		field 7:  Insert Size
#		field 8:  Accession ID (Sequence DB:####|...)
#		field 9:  Created By
#
# Outputs:
#
#       4 BCP files:
#
#       PRB_Probe.bcp                   master Probe records
#       PRB_Reference.bcp         	Probe Reference records
#       ACC_Accession.bcp               Accession records
#       ACC_AccessionReference.bcp      Accession Reference records
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
user = os.environ['MGD_DBUSER']
passwordFileName = os.environ['MGD_DBPASSWORDFILE']
mode = os.environ['LOADMODE']
inputFileName = os.environ['PROBELOADINPUT']

DEBUG = 0		# if 0, not in debug mode
TAB = '\t'		# tab
CRT = '\n'		# carriage return/newline

bcpon = 1		# can the bcp files be bcp-ed into the database?  default is yes.

diagFile = ''		# diagnostic file descriptor
errorFile = ''		# error file descriptor
inputFile = ''		# file descriptor
probeFile = ''          # file descriptor
refFile = ''            # file descriptor
accFile = ''            # file descriptor
accRefFile = ''         # file descriptor

probeTable = 'PRB_Probe'
refTable = 'PRB_Reference'
accTable = 'ACC_Accession'
accRefTable = 'ACC_AccessionReference'

probeFileName = probeTable + '.bcp'
refFileName = refTable + '.bcp'
accFileName = accTable + '.bcp'
accRefFileName = accRefTable + '.bcp'

diagFileName = ''	# diagnostic file name
errorFileName = ''	# error file name

probeKey = 0            # PRB_Probe._Probe_key
refKey = 0		# PRB_Reference._Reference_key
accKey = 0              # ACC_Accession._Accession_key

mgiTypeKey = 3		# Molecular Segment
vectorType = 'Phagemid'
segmentType = 'cDNA'

loaddate = loadlib.loaddate

# Purpose: prints error message and exits
# Returns: nothing
# Assumes: nothing
# Effects: exits with exit status
# Throws: nothing

def exit(
    status,          # numeric exit status (integer)
    message = None   # exit message (string)
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
    global diagFile, errorFile, inputFile, errorFileName, diagFileName
    global probeFile, refFile, accFile, accRefFile
 
    db.useOneConnection(1)
    db.set_sqlUser(user)
    db.set_sqlPasswordFromFile(passwordFileName)
 
    fdate = mgi_utils.date('%m%d%Y')	# current date
    head, tail = os.path.split(inputFileName) 

    diagFileName = tail + '.' + fdate + '.diagnostics'
    errorFileName = tail + '.' + fdate + '.error'

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
        probeFile = open(probeFileName, 'w')
    except:
        exit(1, 'Could not open file %s\n' % probeFileName)

    try:
        refFile = open(refFileName, 'w')
    except:
        exit(1, 'Could not open file %s\n' % refFileName)

    try:
        accFile = open(accFileName, 'w')
    except:
        exit(1, 'Could not open file %s\n' % accFileName)

    try:
        accRefFile = open(accRefFileName, 'w')
    except:
        exit(1, 'Could not open file %s\n' % accRefFileName)

    # Log all SQL
    db.set_sqlLogFunction(db.sqlLogAll)

    # Set Log File Descriptor
    db.set_sqlLogFD(diagFile)

    diagFile.write('Start Date/Time: %s\n' % (mgi_utils.date()))
    diagFile.write('Server: %s\n' % (db.get_sqlServer))
    diagFile.write('Database: %s\n' % (db.get_sqlDatabase))

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
    elif mode != 'full':
        exit(1, 'Invalid Processing Mode:  %s\n' % (mode))

# Purpose:  sets global primary key variables
# Returns:  nothing
# Assumes:  nothing
# Effects:  sets global primary key variables
# Throws:   nothing

def setPrimaryKeys():

    global probeKey, refKey, accKey

    results = db.sql('select maxKey = max(_Probe_key) + 1 from PRB_Probe', 'auto')
    probeKey = results[0]['maxKey']

    results = db.sql('select maxKey = max(_Reference_key) + 1 from PRB_Reference', 'auto')
    refKey = results[0]['maxKey']

    results = db.sql('select maxKey = max(_Accession_key) + 1 from ACC_Accession', 'auto')
    accKey = results[0]['maxKey']

# Purpose:  BCPs the data into the database
# Returns:  nothing
# Assumes:  nothing
# Effects:  BCPs the data into the database
# Throws:   nothing

def bcpFiles():

    bcpdelim = "|"

    if DEBUG or not bcpon:
        return

    probeFile.close()
    refFile.close()
    accFile.close()
    accRefFile.close()

    bcpI = 'cat %s | bcp %s..' % (passwordFileName, db.get_sqlDatabase())
    bcpII = '-c -t\"|" -S%s -U%s' % (db.get_sqlServer(), db.get_sqlUser())

    bcp1 = '%s%s in %s %s' % (bcpI, probeTable, probeFileName, bcpII)
    bcp2 = '%s%s in %s %s' % (bcpI, refTable, refFileName, bcpII)
    bcp3 = '%s%s in %s %s' % (bcpI, accTable, accFileName, bcpII)
    bcp4 = '%s%s in %s %s' % (bcpI, accRefTable, accRefFileName, bcpII)

    for bcpCmd in [bcp1, bcp2, bcp3, bcp4]:
	diagFile.write('%s\n' % bcpCmd)
	os.system(bcpCmd)

    return

# Purpose:  processes data
# Returns:  nothing
# Assumes:  nothing
# Effects:  verifies and processes each line in the input file
# Throws:   nothing

def processFile():

    global probeKey, refKey, accKey

    vectorKey = sourceloadlib.verifyVectorType(vectorType, 0, errorFile)
    segmentTypeKey = sourceloadlib.verifySegmentType(segmentType, 0, errorFile)

    lineNum = 0
    # For each line in the input file

    for line in inputFile.readlines():

        error = 0
        lineNum = lineNum + 1

        # Split the line into tokens
        tokens = string.split(line[:-1], '\t')

        try:
	    name = tokens[0]
	    mgiID = tokens[1]
	    jnum = tokens[2]
	    library = tokens[3]
	    regionCovered = tokens[4]
	    insertSite = tokens[5]
	    insertSize = tokens[6]
	    sequenceIDs = tokens[7]
	    createdBy = tokens[9]
        except:
            exit(1, 'Invalid Line (%d): %s\n' % (lineNum, line))

        referenceKey = loadlib.verifyReference(jnum, lineNum, errorFile)
	libraryKey = sourceloadlib.verifyLibrary(library, lineNum, errorFile)
	userKey = loadlib.verifyUser(createdBy, lineNum, errorFile)

	# sequence IDs
	seqAccDict = {}
	for seqID in string.split(sequenceIDs, '|'):
	    if len(seqID) > 0:
	        [logicalDB, acc] = string.split(seqID, ':')
	        logicalDBKey = loadlib.verifyLogicalDB(logicalDB, lineNum, errorFile)
	        if logicalDBKey > 0:
		    if not seqAccDict.has_key(acc):
			seqAccDict[acc] = []
		    seqAccDict[acc].append(logicalDBKey)

        if vectorKey == 0 or segmentTypeKey == 0 \
	   or referenceKey == 0 or userKey == 0 or libraryKey == 0:
            # set error flag to true
            error = 1

        # if errors, continue to next record
        if error:
            continue

        # if no errors, process the probe

        probeFile.write('%d|%s||%s|%s|%s|||%s|%s|%s||%s|%s|%s|%s\n' \
            % (probeKey, name, libraryKey, vectorKey, segmentTypeKey, mgi_utils.prvalue(regionCovered), \
	    mgi_utils.prvalue(insertSite), mgi_utils.prvalue(insertSize), userKey, userKey, loaddate, loaddate))

        refFile.write('%s|%s|%s|0|0|%s|%s|%s|%s\n' % (refKey, probeKey, referenceKey, userKey, userKey, loaddate, loaddate))

        # MGI Accession ID of clone
	prefixPart, numericPart = accessionlib.split_accnum(mgiID)

        accFile.write('%s|%s|%s|%s|1|%d|%d|0|1|%s|%s|%s|%s\n' \
            % (accKey, mgiID, prefixPart, numericPart, probeKey, mgiTypeKey, userKey, userKey, loaddate, loaddate))

        accKey = accKey + 1

	# sequence accession ids
	for acc in seqAccDict.keys():
	    prefixPart, numericPart = accessionlib.split_accnum(acc)

	    for logicalDB in seqAccDict[acc]:
                accFile.write('%s|%s|%s|%s|%s|%d|%d|0|1|%s|%s|%s|%s\n' \
                    % (accKey, acc, prefixPart, numericPart, logicalDB, probeKey, mgiTypeKey, userKey, userKey, loaddate, loaddate))
                accRefFile.write('%s|%s|%s|%s|%s|%s\n' \
                    % (accKey, referenceKey, userKey, userKey, loaddate, loaddate))
	        accKey = accKey + 1

	refKey = refKey + 1
        probeKey = probeKey + 1

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

