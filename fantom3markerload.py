#!/usr/local/bin/python

#
# Program: fantom3loadmarker.py
#
# Original Author: Lori Corbani
#
# Purpose:
#
#	To load new Probe/Markers into:
#
#	PRB_Marker
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
#		field 1:  MGI ID of Marker
#		field 2:  RIKEN Clone ID
#
# Outputs:
#
#       1 BCP files:
#
#       PRB_Marker.bcp
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
import db
import mgi_utils
import loadlib

#globals

DEBUG = 0		# if 0, not in debug mode
TAB = '\t'		# tab
CRT = '\n'		# carriage return/newline

bcpon = 1		# can the bcp files be bcp-ed into the database?  default is yes.

diagFile = ''		# diagnostic file descriptor
errorFile = ''		# error file descriptor
inputFile = ''		# file descriptor
probeFile = ''          # file descriptor

probeTable = 'PRB_Marker'
jnum = 'J:99680'
relationship = 'E'
createdBy = 'fantom3'

probeFileName = probeTable + '.bcp'

diagFileName = ''	# diagnostic file name
errorFileName = ''	# error file name
passwordFileName = ''	# password file name

mode = ''		# processing mode (load, preview)
probeKey = 0            # PRB_Probe._Probe_key

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
    global probeFile
 
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
    elif mode != 'full':
        exit(1, 'Invalid Processing Mode:  %s\n' % (mode))

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

    bcpI = 'cat %s | bcp %s..' % (passwordFileName, db.get_sqlDatabase())
    bcpII = '-c -t\"|" -S%s -U%s' % (db.get_sqlServer(), db.get_sqlUser())

    bcp1 = '%s%s in %s %s' % (bcpI, probeTable, probeFileName, bcpII)
    diagFile.write('%s\n' % bcp1)
    os.system(bcp1)

    return

# Purpose:  processes data
# Returns:  nothing
# Assumes:  nothing
# Effects:  verifies and processes each line in the input file
# Throws:   nothing

def processFile():

    lineNum = 0

    referenceKey = loadlib.verifyReference(jnum, lineNum, errorFile)
    userKey = loadlib.verifyUser(createdBy, lineNum, errorFile)

    # For each line in the input file

    for line in inputFile.readlines():

        error = 0
        lineNum = lineNum + 1

        # Split the line into tokens
        tokens = string.split(line[:-1], '\t')

        try:
	    mgiID = tokens[0]
	    cloneIDs = tokens[1]
        except:
            exit(1, 'Invalid Line (%d): %s\n' % (lineNum, line))

	markerKey = loadlib.verifyObject(mgiID, '2', None, lineNum, errorFile)

	for cloneID in string.split(cloneIDs, ','):
	    cloneKey = loadlib.verifyObject(cloneID, '3', None, lineNum, errorFile)

            if referenceKey == 0 or userKey == 0 or markerKey == 0 or cloneKey == 0:
                # set error flag to true
                error = 1

            # if errors, continue to next record
            if error:
                continue

            # if no errors, process the probe

            probeFile.write('%d|%d|%d|%s|%s|%s|%s|%s\n' \
                % (cloneKey, markerKey, referenceKey, relationship, userKey, userKey, loaddate, loaddate))

    #	end of "for line in inputFile.readlines():"

#
# Main
#

init()
verifyMode()
processFile()
bcpFiles()
exit(0)

