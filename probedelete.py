
#
# Program: probedelete.py
#
# Original Author: Lori Corbani
#
# Purpose:
#
#	To delete Probes from:
#
#	PRB_Probe
#
# Requirements Satisfied by This Program:
#
# Usage:
#	probedelete.py
#
# Envvars:
#
# Inputs:
#
#	A tab-delimited file in the format:
#		field 1:  MGI Probe ID
#
# Outputs:
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
currentDir = os.environ['PROBELOADDIR']
mode = os.environ['PROBELOADMODE']
inputFileName = os.environ['PROBEDATAFILE']
outputDir = os.environ['PROBELOADDATADIR']

DEBUG = 0		# if 0, not in debug mode
TAB = '\t'		# tab
CRT = '\n'		# carriage return/newline

diagFile = ''		# diagnostic file descriptor
errorFile = ''		# error file descriptor
inputFile = ''		# file descriptor

diagFileName = ''	# diagnostic file name
errorFileName = ''	# error file name

mgiTypeKey = '3'
deleteSQL = '''delete PRB_Probe from PRB_Probe where _Probe_key = %s'''

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
    global diagFile, errorFile, inputFile, errorFileName, diagFileName
 
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

# Purpose:  processes data
# Returns:  nothing
# Assumes:  nothing
# Effects:  verifies and processes each line in the input file
# Throws:   nothing

def processFile():

    lineNum = 0
    # For each line in the input file

    for line in inputFile.readlines():

        lineNum = lineNum + 1

        # Split the line into tokens
        tokens = str.split(line[:-1], '\t')

        try:
            probeID = tokens[0]
        except:
            exit(1, 'Invalid Line (%d): %s\n' % (lineNum, line))

        probeKey = loadlib.verifyObject(probeID, mgiTypeKey, None, lineNum, errorFile)

        if probeKey == 0:
            continue

        if DEBUG:
            print(deleteSQL % (probeKey))
            continue

        db.sql(deleteSQL % (probeKey), None)

    #	end of "for line in inputFile.readlines():"

#
# Main
#

init()
verifyMode()
processFile()
exit(0)
