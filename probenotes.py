#!/usr/local/bin/python

#
# Program: probenotes.py
#
# Original Author: Lori Corbani
#
# Purpose:
#
#	To load new Probe/Notes into:
#
#	PRB_Notes
#
# Requirements Satisfied by This Program:
#
# Usage:
#	probenotes.py
#
# Envvars:
#
# Inputs:
#
#	A tab-delimited file in the format:
#		field 1:  MGI ID Probe		required MGI:
#		field 2:  Notes
#		field 3:  Created By		required
#
# Outputs:
#
#       1 BCP files:
#
#	PRB_Notes.bcp			Probe/Notes records
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
# 02/10/2010	lec
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
user = os.environ['PG_DBUSER']
passwordFileName = os.environ['PG_DBPASSWORDFILE']
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
notesFile = ''		# file descriptor

notesTable = 'PRB_Notes'

notesFileName = outputDir + '/' + notesTable + '.bcp'

diagFileName = ''	# diagnostic file name
errorFileName = ''	# error file name

loaddate = loadlib.loaddate

# delete the probe/notes so we can add new ones
deleteSQL = 'delete from PRB_Notes where _Probe_key = %s;'

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
    global notesFile
 
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
        notesFile = open(notesFileName, 'w')
    except:
        exit(1, 'Could not open file %s\n' % notesFileName)

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

    if mode in ('preview', 'preview-notdeleted'):
        DEBUG = 1
        bcpon = 0
    elif mode not in ('load', 'load-notdeleted'):
        exit(1, 'Invalid Processing Mode:  %s\n' % (mode))

# Purpose:  BCPs the data into the database
# Returns:  nothing
# Assumes:  nothing
# Effects:  BCPs the data into the database
# Throws:   nothing

def bcpFiles():

    diagFile.write(execSQL)

    if DEBUG or not bcpon:
        return

    notesFile.close()

    db.commit()

    # execute the sql deletions
    if len(execSQL) > 0:
        db.sql(execSQL, None)

    bcpCommand = os.environ['PG_DBUTILS'] + '/bin/bcpin.csh'
    currentDir = os.getcwd()

    bcp1 = '%s %s %s %s %s %s "\\t" "\\n" mgd' % \
        (bcpCommand, db.get_sqlServer(), db.get_sqlDatabase(), noteTable, currentDir, noteFileName)

    for bcpCmd in [bcp1]:
	diagFile.write('%s\n' % bcpCmd)
	os.system(bcpCmd)

    db.commit()

    return

# Purpose:  processes data
# Returns:  nothing
# Assumes:  nothing
# Effects:  verifies and processes each line in the input file
# Throws:   nothing

def processFile():

    global execSQL

    lineNum = 0
    # For each line in the input file

    for line in inputFile.readlines():

        error = 0
        lineNum = lineNum + 1

        # Split the line into tokens
        tokens = string.split(line[:-1], '\t')

        try:
	    probeID = tokens[0]
	    notes = tokens[1]
	    createdBy = tokens[2]
        except:
            exit(1, 'Invalid Line (%d): %s\n' % (lineNum, line))

        probeKey = loadlib.verifyProbe(probeID, lineNum, errorFile)
	createdByKey = loadlib.verifyUser(createdBy, lineNum, errorFile)

	if probeKey == 0:
	    errorFile.write('Invalid Probe:  %s\n' % (probeID))
	    error = 1

	if createdByKey == 0:
	    errorFile.write('Invalid Creator:  %s\n\n' % (createdBy))
	    error = 1

        # if errors, continue to next record
        if error:
            continue

        # if no errors, process

        # Notes

	# automatically deletes any existing notes for this probe
        if mode in ('preview', 'load'):
	    execSQL = execSQL + deleteSQL % (probeKey)

        noteSeq = 1
        if len(notes) > 0:
            notesFile.write('%s\t%d\t%s\t%s\t%s\n' % (probeKey, noteSeq, notes, loaddate, loaddate))

    #	end of "for line in inputFile.readlines():"

#
# Main
#

init()
verifyMode()
processFile()
bcpFiles()
exit(0)

