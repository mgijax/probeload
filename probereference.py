#!/usr/local/bin/python

#
# Program: probereference.py
#
# Original Author: Lori Corbani
#
# Purpose:
#
#	To load new Probe/Reference/Alias into:
#
#	PRB_Reference
#	PRB_Alias
#
# Requirements Satisfied by This Program:
#
# Usage:
#	probereference.py
#
# Envvars:
#
# Inputs:
#
#	A tab-delimited file in the format:
#		field 1:  MGI ID Probe		required MGI:
#		field 2:  Reference 		required J:#####
#		field 3:  Alias                 allows null (Alias|Alias...)
#		field 4:  Created By		required
#
# Outputs:
#
#       2 BCP files:
#
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

refTable = 'PRB_Reference'
aliasTable = 'PRB_Alias'

refFileName = outputDir + '/' + refTable + '.bcp'
aliasFileName = outputDir + '/' + aliasTable + '.bcp'

diagFileName = ''	# diagnostic file name
errorFileName = ''	# error file name

refKey = 0		# PRB_Reference._Reference_key
aliasKey = 0		# PRB_Alias._Alias_key

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
    global refFile, aliasFile
 
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
    diagFile.write('Server: %s\n' % (db.get_sqlServer))
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
        return

    refFile.close()
    aliasFile.close()

    bcpI = 'cat %s | bcp %s..' % (passwordFileName, db.get_sqlDatabase())
    bcpII = '-c -t\"|" -S%s -U%s' % (db.get_sqlServer(), db.get_sqlUser())

    bcp1 = '%s%s in %s %s' % (bcpI, refTable, refFileName, bcpII)
    bcp2 = '%s%s in %s %s' % (bcpI, aliasTable, aliasFileName, bcpII)

    for bcpCmd in [bcp1, bcp2]:
	diagFile.write('%s\n' % bcpCmd)
	os.system(bcpCmd)

    return

# Purpose:  processes data
# Returns:  nothing
# Assumes:  nothing
# Effects:  verifies and processes each line in the input file
# Throws:   nothing

def processFile():

    global refKey, aliasKey

    lineNum = 0
    # For each line in the input file

    for line in inputFile.readlines():

        error = 0
        lineNum = lineNum + 1

        # Split the line into tokens
        tokens = string.split(line[:-1], '\t')

        try:
	    probeID = tokens[0]
	    jnum = tokens[1]
	    aliasList = string.split(tokens[2], '|')
	    createdBy = tokens[3]
        except:
            exit(1, 'Invalid Line (%d): %s\n' % (lineNum, line))

        probeKey = loadlib.verifyProbe(probeID, lineNum, errorFile)
        referenceKey = loadlib.verifyReference(jnum, lineNum, errorFile)
	createdByKey = loadlib.verifyUser(createdBy, lineNum, errorFile)

	if probeKey == 0:
	    errorFile.write('Invalid Probe:  %s\n' % (probeID))
	    error = 1

	if referenceKey == 0:
	    errorFile.write('Invalid Reference:  %s\n' % (jnum))
	    error = 1

	if createdByKey == 0:
	    errorFile.write('Invalid Creator:  %s\n\n' % (createdBy))
	    error = 1

        # if errors, continue to next record
        if error:
            continue

        # if no errors, process

        refFile.write('%s|%s|%s|0|0|%s|%s|%s|%s\n' \
		% (refKey, probeKey, referenceKey, createdByKey, createdByKey, loaddate, loaddate))

        # aliases

        for alias in aliasList:
            aliasFile.write('%s|%s|%s|%s|%s|%s|%s\n' \
		    % (aliasKey, refKey, alias, createdByKey, createdByKey, loaddate, loaddate))
	    aliasKey = aliasKey + 1

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

