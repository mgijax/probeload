#!/usr/local/bin/python

# $Header$
# $Name$

#
# Program: probeload.py
#
# Original Author: Lori Corbani
#
# Purpose:
#
#	To load new Probes into:
#
#	PRB_Probe
#	PRB_Marker
#	PRB_Reference
#	ACC_Accession
#	ACC_AccessionReference
#
# Requirements Satisfied by This Program:
#
# Usage:
#	program.py
#	-S = database server
#	-D = database
#	-U = user
#	-P = password file
#	-M = mode
#	-I = input file
#
# Envvars:
#
# Inputs:
#
#	A tab-delimited file in the format:
#		field 1:  Probe Name
#		field 2:  Reference (J:#####)
#		field 3:  Organism
#		field 4:  Strain
#		field 5:  Tissue
#		field 6:  Gender
#		field 7:  Cell Line
#		field 8:  Age
#		field 9:  Vector Type
#		field 10: Segment Type
#		field 11: Region Covered
#		field 12: Insert Site
#		field 13: Insert Size
#		field 14: MGI Marker
#		field 15: Relationship
#		field 16: Sequence ID	(LogicalDB:Acc ID|...)
#		field 17: Notes
#		field 18: Created By
#
# Outputs:
#
#       6 BCP files:
#
#       PRB_Probe.bcp                   master Probe records
#	PRB_Marker.bcp			Probe/Marker records
#       PRB_Reference.bcp         	Probe Reference records
#       ACC_Accession.bcp               Accession records
#       ACC_AccessionReference.bcp      Accession Reference records
#       PRB_Notes.bcp               	Probe Notes
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
import getopt
import accessionlib
import db
import mgi_utils
import loadlib
import sourceloadlib

#globals

DEBUG = 0		# if 0, not in debug mode
TAB = '\t'		# tab
CRT = '\n'		# carriage return/newline

bcpon = 1		# can the bcp files be bcp-ed into the database?  default is yes.

diagFile = ''		# diagnostic file descriptor
errorFile = ''		# error file descriptor
inputFile = ''		# file descriptor
probeFile = ''          # file descriptor
markerFile = ''		# file descriptor
refFile = ''            # file descriptor
accFile = ''            # file descriptor
accRefFile = ''           # file descriptor
noteFile = ''		# file descriptor

probeTable = 'PRB_Probe'
markerTable = 'PRB_Marker'
refTable = 'PRB_Reference'
accTable = 'ACC_Accession'
accRefTable = 'ACC_AccessionReference'
noteTable = 'PRB_Notes'

probeFileName = probeTable + '.bcp'
markerFileName = markerTable + '.bcp'
refFileName = refTable + '.bcp'
accFileName = accTable + '.bcp'
accRefFileName = accRefTable + '.bcp'
noteFileName = noteTable + '.bcp'

diagFileName = ''	# diagnostic file name
errorFileName = ''	# error file name
passwordFileName = ''	# password file name

mode = ''		# processing mode (load, preview)
probeKey = 0            # PRB_Probe._Probe_key
refKey = 0		# PRB_Reference._Reference_key
accKey = 0              # ACC_Accession._Accession_key
mgiKey = 0              # ACC_AccessionMax.maxNumericPart

NA = -2			# for Not Applicable fields
mgiTypeKey = 3		# Molecular Segment
mgiPrefix = "MGI:"

referenceDict = {}      # dictionary of references for quick lookup
markerDict = {}      	# dictionary of markers for quick lookup

loaddate = loadlib.loaddate

# Purpose: displays correct usage of this program
# Returns: nothing
# Assumes: nothing
# Effects: exits with status of 1
# Throws: nothing
 
def showUsage():
    usage = 'usage: %s -S server\n' % sys.argv[0] + \
        '-D database\n' + \
        '-U user\n' + \
        '-P password file\n' + \
        '-M mode\n'

    exit(1, usage)
 
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
#          calls showUsage() if usage error
#          exits if files cannot be opened
# Throws: nothing

def init():
    global diagFile, errorFile, inputFile, errorFileName, diagFileName, passwordFileName
    global mode
    global probeFile, markerFile, refFile, accFile, accRefFile, noteFile
 
    try:
        optlist, args = getopt.getopt(sys.argv[1:], 'S:D:U:P:M:I:')
    except:
        showUsage()
 
    #
    # Set server, database, user, passwords depending on options specified
    #
 
    server = ''
    database = ''
    user = ''
    password = ''
 
    for opt in optlist:
        if opt[0] == '-S':
            server = opt[1]
        elif opt[0] == '-D':
            database = opt[1]
        elif opt[0] == '-U':
            user = opt[1]
        elif opt[0] == '-P':
            passwordFileName = opt[1]
        elif opt[0] == '-M':
            mode = opt[1]
        elif opt[0] == '-I':
            inputFileName = opt[1]
        else:
            showUsage()

    # User must specify Server, Database, User and Password
    password = string.strip(open(passwordFileName, 'r').readline())
    if server == '' or database == '' or user == '' or password == '' \
	or mode == '' or inputFileName == '':
        showUsage()

    # Initialize db.py DBMS parameters
    db.set_sqlLogin(user, password, server, database)
    db.useOneConnection(1)
 
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
        markerFile = open(markerFileName, 'w')
    except:
        exit(1, 'Could not open file %s\n' % markerFileName)

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

    try:
        noteFile = open(noteFileName, 'w')
    except:
        exit(1, 'Could not open file %s\n' % noteFileName)

    # Log all SQL
    db.set_sqlLogFunction(db.sqlLogAll)

    # Set Log File Descriptor
    db.set_sqlLogFD(diagFile)

    diagFile.write('Start Date/Time: %s\n' % (mgi_utils.date()))
    diagFile.write('Server: %s\n' % (server))
    diagFile.write('Database: %s\n' % (database))
    diagFile.write('User: %s\n' % (user))

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

    global probeKey, refKey, accKey, mgiKey

    results = db.sql('select maxKey = max(_Probe_key) + 1 from PRB_Probe', 'auto')
    probeKey = results[0]['maxKey']

    results = db.sql('select maxKey = max(_Reference_key) + 1 from PRB_Reference', 'auto')
    refKey = results[0]['maxKey']

    results = db.sql('select maxKey = max(_Accession_key) + 1 from ACC_Accession', 'auto')
    accKey = results[0]['maxKey']

    results = db.sql('select maxKey = maxNumericPart + 1 from ACC_AccessionMax ' + \
        'where prefixPart = "%s"' % (mgiPrefix), 'auto')
    mgiKey = results[0]['maxKey']

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
    markerFile.close()
    refFile.close()
    accFile.close()
    accRefFile.close()
    noteFile.close()

    bcpI = 'cat %s | bcp %s..' % (passwordFileName, db.get_sqlDatabase())
    bcpII = '-c -t\"|" -S%s -U%s' % (db.get_sqlServer(), db.get_sqlUser())

    bcp1 = '%s%s in %s %s' % (bcpI, probeTable, probeFileName, bcpII)
    bcp2 = '%s%s in %s %s' % (bcpI, markerTable, markerFileName, bcpII)
    bcp3 = '%s%s in %s %s' % (bcpI, refTable, refFileName, bcpII)
    bcp4 = '%s%s in %s %s' % (bcpI, accTable, accFileName, bcpII)
    bcp5 = '%s%s in %s %s' % (bcpI, accRefTable, accRefFileName, bcpII)
    bcp6 = '%s%s in %s %s' % (bcpI, noteTable, noteFileName, bcpII)

    for bcpCmd in [bcp1, bcp2, bcp3, bcp4, bcp5, bcp6]:
	diagFile.write('%s\n' % bcpCmd)
	os.system(bcpCmd)

    return

# Purpose:  processes data
# Returns:  nothing
# Assumes:  nothing
# Effects:  verifies and processes each line in the input file
# Throws:   nothing

def processFile():

    global probeKey, refKey, accKey, mgiKey

    lineNum = 0
    # For each line in the input file

    for line in inputFile.readlines():

        error = 0
        lineNum = lineNum + 1

        # Split the line into tokens
        tokens = string.split(line[:-1], '\t')

        try:
	    name = tokens[0]
	    jnum = tokens[1]
	    organism = tokens[2]
	    strain = tokens[3]
	    tissue = tokens[4]
	    gender = tokens[5]
	    cellLine = tokens[6]
	    age = tokens[7]
	    vectorType = tokens[8]
	    segmentType = tokens[9]
	    regionCovered = tokens[10]
	    insertSite = tokens[11]
	    insertSize = tokens[12]
	    markerID = tokens[13]
	    relationship = tokens[14]
	    sequenceIDs = tokens[15]
	    notes = tokens[16]
	    createdBy = tokens[17]
        except:
            exit(1, 'Invalid Line (%d): %s\n' % (lineNum, line))

	organismKey = sourceloadlib.verifyOrganism(organism, lineNum, errorFile)
	strainKey = sourceloadlib.verifyStrain(strain, lineNum, errorFile)
	tissueKey = sourceloadlib.verifyTissue(tissue, lineNum, errorFile)
	genderKey = sourceloadlib.verifyGender(gender, lineNum, errorFile)
	cellLineKey = sourceloadlib.verifyCellLine(cellLine, lineNum, errorFile)
	vectorKey = sourceloadlib.verifyVectorType(vectorType, lineNum, errorFile)
	segmentTypeKey = sourceloadlib.verifySegmentType(segmentType, lineNum, errorFile)
        referenceKey = loadlib.verifyReference(jnum, lineNum, errorFile)
	markerKey = loadlib.verifyMarker(markerID, lineNum, errorFile)
	userKey = loadlib.verifyUser(createdBy, lineNum, errorFile)

	sourceKey = sourceloadlib.verifySource(segmentTypeKey, vectorKey, organismKey, strainKey, tissueKey, genderKey, cellLineKey, age, lineNum, errorFile)

	# sequence IDs
	seqAccDict = {}
	for seqID in string.split(sequenceIDs, '|'):
	    if len(seqID) > 0:
	        [logicalDB, acc] = string.split(seqID, ':')
	        logicalDBKey = loadlib.verifyLogicalDB(logicalDB, lineNum, errorFile)
	        if logicalDBKey > 0:
		    seqAccDict[acc] = logicalDBKey

        if organismKey == 0 or strainKey == 0 or tissueKey == 0 or genderKey == 0 \
	   or cellLineKey == 0 or vectorKey == 0 or segmentTypeKey == 0 \
	   or markerKey == 0 or referenceKey == 0 or userKey == 0 or sourceKey == 0:
            # set error flag to true
            error = 1

        # if errors, continue to next record
        if error:
            continue


        # if no errors, process the probe

        probeFile.write('%d|%s||%s|%s|%s|||%s|%s|%s||%s|%s|%s|%s\n' \
            % (probeKey, name, sourceKey, vectorKey, segmentTypeKey, mgi_utils.prvalue(regionCovered), \
	    mgi_utils.prvalue(insertSite), mgi_utils.prvalue(insertSize), userKey, userKey, loaddate, loaddate))

        markerFile.write('%s|%s|%d|%s|%s|%s|%s|%s\n' % (probeKey, markerKey, referenceKey, relationship, userKey, userKey, loaddate, loaddate))

        refFile.write('%s|%s|%s|0|0|%s|%s|%s|%s\n' % (refKey, probeKey, referenceKey, userKey, userKey, loaddate, loaddate))

        # MGI Accession ID for the marker

        accFile.write('%s|%s%d|%s|%s|1|%d|%d|0|1|%s|%s|%s|%s\n' \
            % (accKey, mgiPrefix, mgiKey, mgiPrefix, mgiKey, probeKey, mgiTypeKey, userKey, userKey, loaddate, loaddate))

	# Notes

	if len(notes) > 0:
	    noteFile.write('%s|1|%s|%s|%s\n' % (probeKey, notes, loaddate, loaddate))

        accKey = accKey + 1
        mgiKey = mgiKey + 1

	# sequence accession ids
	for acc in seqAccDict.keys():
	    prefixPart, numericPart = accessionlib.split_accnum(acc)
            accFile.write('%s|%s|%s|%s|%s|%d|%d|0|1|%s|%s|%s|%s\n' \
                % (accKey, acc, prefixPart, numericPart, seqAccDict[acc], probeKey, mgiTypeKey, userKey, userKey, loaddate, loaddate))
            accRefFile.write('%s|%s|%s|%s|%s|%s\n' \
                % (accKey, referenceKey, userKey, userKey, loaddate, loaddate))
	    accKey = accKey + 1

	refKey = refKey + 1
        probeKey = probeKey + 1

    #	end of "for line in inputFile.readlines():"

    #
    # Update the AccessionMax value
    #

    if not DEBUG:
        db.sql('exec ACC_setMax %d' % (lineNum), None)

#
# Main
#

init()
verifyMode()
setPrimaryKeys()
processFile()
bcpFiles()
exit(0)

# $Log$
