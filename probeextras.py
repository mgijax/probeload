#!/usr/local/bin/python

#
# Program: probeextras.py
#
# Original Author: Lori Corbani
#
# Purpose:
#
#	To load new Probes information into:
#
#	PRB_Marker
#	PRB_Reference
#	PRB_Alias
#
# Requirements Satisfied by This Program:
#
# Usage:
#	probeextras.py
#
# Envvars:
#
# Inputs:
#
#	A tab-delimited file in the format:
#		field 1:  MGI ID Probe		required MGI:
#		field 2:  MGI ID Marker		MGI:; allows null
#		field 3:  Reference 		required J:#####
#		field 4:  Relationship		allows null
#		field 5:  Alias                 allows null
#		field 6:  Created By		required
#
#	PRB_Reference data is always loaded
# 	If Marker given, then PRB_Marker (J:, Relationship) data is also loaded
# 	If Alias given, then PRB_Alias (Alias) is also loaded
#	
# Outputs:
#
#       3 BCP files:
#
#	PRB_Marker.bcp			Probe/Marker records
#       PRB_Reference.bcp         	Probe Reference records
#       PRB_Alias.bcp         		Probe Alias records
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
# 12/16/2009	lec
#	- TR9931/Eurexpress/new
#

import sys
import os
import string
import accessionlib
import db
import mgi_utils
import loadlib

#globals

#
# from configuration file
#
user = os.environ['MGD_DBUSER']
passwordFileName = os.environ['MGD_DBPASSWORDFILE']
mode = os.environ['PROBELOADMODE']
inputFileName = os.environ['PROBEDATAFILE']
outputDir = os.environ['PROBELOADDATADIR']

DEBUG = 0		# if 0, not in debug mode
TAB = '\t'		# tab
CRT = '\n'		# carriage return/newline

bcpon = 1		# can the bcp files be bcp-ed into the database?  default is yes.

diagFile = ''		# diagnostic file descriptor
errorFile = ''		# error file descriptor
inputFile = ''		# file descriptor
markerFile = ''		# file descriptor
refFile = ''            # file descriptor
aliasFile = ''          # file descriptor

markerTable = 'PRB_Marker'
refTable = 'PRB_Reference'
aliasTable = 'PRB_Alias'

markerFileName = outputDir + '/' + markerTable + '.bcp'
refFileName = outputDir + '/' + refTable + '.bcp'
aliasFileName = outputDir + '/' + aliasTable + '.bcp'

diagFileName = ''	# diagnostic file name
errorFileName = ''	# error file name

refKey = 0		# PRB_Reference._Reference_key
aliasKey = 0		# PRB_Alias._Alias_key

loaddate = loadlib.loaddate

# delete the probe/marker relationships so we can add new ones
deleteSQL = 'delete from PRB_Marker where _Probe_key = %s and _Marker_key = %s\n'
execSQL = ''

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
    global markerFile, refFile, aliasFile
 
    db.useOneConnection(1)
    db.set_sqlUser(user)
    db.set_sqlPasswordFromFile(passwordFileName)
 
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
        markerFile = open(markerFileName, 'w')
    except:
        exit(1, 'Could not open file %s\n' % markerFileName)

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

    # Set Log File Descriptor
    db.set_sqlLogFD(diagFile)

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

    results = db.sql('select maxKey = max(_Reference_key) + 1 from PRB_Reference', 'auto')
    refKey = results[0]['maxKey']

    results = db.sql('select maxKey = max(_Alias_key) + 1 from PRB_Alias', 'auto')
    aliasKey = results[0]['maxKey']

# Purpose:  BCPs the data into the database
# Returns:  nothing
# Assumes:  nothing
# Effects:  BCPs the data into the database
# Throws:   nothing

def bcpFiles():

    bcpdelim = "|"

    if DEBUG or not bcpon:
	#print execSQL
        return

    markerFile.close()
    refFile.close()
    aliasFile.close()

    # execute the sql deletions
    if execSQL != '':
        db.sql(execSQL, None)

    bcpI = 'cat %s | bcp %s..' % (passwordFileName, db.get_sqlDatabase())
    bcpII = '-c -t\"|" -S%s -U%s' % (db.get_sqlServer(), db.get_sqlUser())

    bcp1 = '%s%s in %s %s' % (bcpI, markerTable, markerFileName, bcpII)
    bcp2 = '%s%s in %s %s' % (bcpI, refTable, refFileName, bcpII)
    bcp3 = '%s%s in %s %s' % (bcpI, aliasTable, aliasFileName, bcpII)

    for bcpCmd in [bcp1, bcp2, bcp3]:
	diagFile.write('%s\n' % bcpCmd)
	os.system(bcpCmd)

    return

# Purpose:  processes data
# Returns:  nothing
# Assumes:  nothing
# Effects:  verifies and processes each line in the input file
# Throws:   nothing

def processFile():

    global refKey, aliasKey, execSQL

    lineNum = 0
    # For each line in the input file

    for line in inputFile.readlines():

        error = 0
        lineNum = lineNum + 1

        # Split the line into tokens
        tokens = string.split(line[:-1], '\t')

        try:
	    probeID = tokens[0]
	    markerIDs = string.split(tokens[1], '|')
	    jnum = tokens[2]
	    relationship = tokens[3]
	    aliasList = string.split(tokens[4], '|')
	    createdBy = tokens[5]
        except:
            exit(1, 'Invalid Line (%d): %s\n' % (lineNum, line))

        probeKey = loadlib.verifyProbe(probeID, lineNum, errorFile)
        refsKey = loadlib.verifyReference(jnum, lineNum, errorFile)
	createdByKey = loadlib.verifyUser(createdBy, lineNum, errorFile)

	if probeKey == 0:
	    errorFile.write('Invalid Probe:  %s\n' % (probeID))
	    error = 1

	if refsKey == 0:
	    errorFile.write('Invalid Reference:  %s\n' % (jnum))
	    error = 1

	if createdByKey == 0:
	    errorFile.write('Invalid Creator:  %s\n\n' % (createdBy))
	    error = 1

	results = db.sql('''select _Reference_key from PRB_Reference
		where _Probe_key = %s
		and _Refs_key = %s
		''' % (probeKey, refsKey), 'auto')
        referenceKey = results[0]['_Reference_key']
	if referenceKey == 0:
	    errorFile.write('Invalid Probe/Reference:  %s\n' % (jnum))
	    error = 1

	# marker IDs

	markerList = []
	for markerID in markerIDs:

	    if markerID == 'none':
		break

	    markerKey = loadlib.verifyMarker(markerID, lineNum, errorFile)

	    if markerKey == 0:
	        errorFile.write('Invalid Marker:  %s\n' % (markerID))
	        error = 1
            else:
		markerList.append(markerKey)

        # if errors, continue to next record
        if error:
            continue

        # if no errors, process

	for markerKey in markerList:
	    if markerList.count(markerKey) == 1:
                markerFile.write('%s|%s|%d|%s|%s|%s|%s|%s\n' \
		    % (probeKey, markerKey, refsKey, relationship, createdByKey, createdByKey, loaddate, loaddate))
		execSQL = execSQL + deleteSQL % (probeKey, markerKey)
            else:
		errorFile.write('Invalid Marker Duplicate:  %s\n' % (markerID))

	if referenceKey > 0:
	    refKey = referenceKey
	else:
            refFile.write('%s|%s|%s|0|0|%s|%s|%s|%s\n' \
		    % (refKey, probeKey, refsKey, createdByKey, createdByKey, loaddate, loaddate))

        # aliases

        for alias in aliasList:
            aliasFile.write('%s|%s|%s|%s|%s|%s|%s\n' \
		    % (aliasKey, refKey, alias, createdByKey, createdByKey, loaddate, loaddate))
	    aliasKey = aliasKey + 1

	# only used if referenceKey == 0
	refKey = refKey + 1

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

