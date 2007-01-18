#!/usr/local/bin/python

#
# Program: primerload.py
#
# Original Author: Lori Corbani
#
# Purpose:
#
#	To load new Primers into:
#
#	PRB_Probe
#	PRB_Marker
#	PRB_Reference
#	ACC_Accession
#	PRB_Notes
#
# Requirements Satisfied by This Program:
#
# Usage:
#	primerload.py
#
# Envvars:
#
# Inputs:
#
#	A tab-delimited file in the format:
#		field 1: Marker Symbol
#		field 2: MGI Marker Accession ID
#		field 3: Primer Name
#		field 4: Reference (J:#####)
#		field 5: Region Covered
#		field 6: Sequence 1
#		field 7: Sequence 2
#		field 8: Product Size
#		field 9: Notes
#               field 10: Nucleotide Sequence ID   (|-delimited)
#               field 11: Created By
#
# Outputs:
#
#       5 BCP files:
#
#       PRB_Probe.bcp                   master Primer records
#	PRB_Marker.bcp			Primer/Marker records
#       PRB_Reference.bcp         	Primer Reference records
#       ACC_Accession.bcp               Accession records
#	PRB_Notes.bcp			Primer Notes
#
#       Diagnostics file of all input parameters and SQL commands
#       Error file
#
# Exit Codes:
#
# Assumes:
#
#	That no one else is adding Nomen records to the database.
#
# Bugs:
#
# Implementation:
#

import sys
import os
import string
import db
import mgi_utils
import accessionlib
import loadlib

#globals

#
# from configuration file
#
user = os.environ['MGD_DBUSER']
passwordFileName = os.environ['MGD_DBPASSWORDFILE']
mode = os.environ['PRIMERMODE']
inputFileName = os.environ['PRIMERDATAFILE']
outputDir = os.environ['OUTPUTDIR']

DEBUG = 0		# if 0, not in debug mode
TAB = '\t'		# tab
CRT = '\n'		# carriage return/newline

bcpon = 1		# can the bcp files be bcp-ed into the database?  default is yes.

diagFile = ''		# diagnostic file descriptor
errorFile = ''		# error file descriptor
inputFile = ''		# file descriptor
primerFile = ''         # file descriptor
markerFile = ''		# file descriptor
refFile = ''            # file descriptor
accFile = ''            # file descriptor
accRefFile = ''         # file descriptor
noteFile = ''		# file descriptor

primerTable = 'PRB_Probe'
markerTable = 'PRB_Marker'
refTable = 'PRB_Reference'
accTable = 'ACC_Accession'
accRefTable = 'ACC_AccessionReference'
noteTable = 'PRB_Notes'

primerFileName = outputDir + '/' + primerTable + '.bcp'
markerFileName = outputDir + '/' + markerTable + '.bcp'
refFileName = outputDir + '/' + refTable + '.bcp'
accFileName = outputDir + '/' + accTable + '.bcp'
accRefFileName = outputDir + '/' + accRefTable + '.bcp'
noteFileName = outputDir + '/' + noteTable + '.bcp'

diagFileName = ''	# diagnostic file name
errorFileName = ''	# error file name

primerKey = 0           # PRB_Probe._Probe_key
refKey = 0		# PRB_Reference._Reference_key
accKey = 0              # ACC_Accession._Accession_key
mgiKey = 0              # ACC_AccessionMax.maxNumericPart

segmentTypeKey = 63473	# PRB_Probe._SegmentType_key
vectorKey = 316369	# PRB_Probe._Vector_key
relationship = 'A'	# PRB_Marker.relationship
NA = -2			# for Not Applicable fields
mgiTypeKey = 3		# Molecular Segment
mgiPrefix = "MGI:"
logicalDBKey = 9	# Logical DB for Nucleotide Sequences

referenceDict = {}      # dictionary of references for quick lookup
markerDict = {}      	# dictionary of markers for quick lookup

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
#          calls showUsage() if usage error
#          exits if files cannot be opened
# Throws: nothing

def init():
    global diagFile, errorFile, inputFile, errorFileName, diagFileName
    global primerFile, markerFile, refFile, accFile, accRefFile, noteFile
 
    db.useOneConnection(1)
    db.set_sqlUser(user)
    db.set_sqlPasswordFromFile(passwordFileName)
 
    fdate = mgi_utils.date('%m%d%Y')	# current date
    head, tail = os.path.split(inputFileName) 

    diagFileName = outputDir + '/' + tail + '.' + fdate + '.diagnostics'
    errorFileName = outputDir + '/' + tail + '.' + fdate + '.error'

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
        primerFile = open(primerFileName, 'w')
    except:
        exit(1, 'Could not open file %s\n' % primerFileName)

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

    global primerKey, refKey, accKey, mgiKey

    results = db.sql('select maxKey = max(_Probe_key) + 1 from PRB_Probe', 'auto')
    primerKey = results[0]['maxKey']

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

    primerFile.close()
    markerFile.close()
    refFile.close()
    accFile.close()
    accRefFile.close()
    noteFile.close()

    bcpI = 'cat %s | bcp %s..' % (passwordFileName, db.get_sqlDatabase())
    bcpII = '-c -t\"|" -S%s -U%s' % (db.get_sqlServer(), db.get_sqlUser())
    truncateDB = 'dump transaction %s with truncate_only' % (db.get_sqlDatabase())

    bcp1 = '%s%s in %s %s' % (bcpI, primerTable, primerFileName, bcpII)
    bcp2 = '%s%s in %s %s' % (bcpI, markerTable, markerFileName, bcpII)
    bcp3 = '%s%s in %s %s' % (bcpI, refTable, refFileName, bcpII)
    bcp4 = '%s%s in %s %s' % (bcpI, accTable, accFileName, bcpII)
    bcp5 = '%s%s in %s %s' % (bcpI, accRefTable, accRefFileName, bcpII)
    bcp6 = '%s%s in %s %s' % (bcpI, noteTable, noteFileName, bcpII)

    for bcpCmd in [bcp1, bcp2, bcp3, bcp4, bcp5, bcp6]:
	diagFile.write('%s\n' % bcpCmd)
	os.system(bcpCmd)
#	db.sql(truncateDB, None)

    return

# Purpose:  processes data
# Returns:  nothing
# Assumes:  nothing
# Effects:  verifies and processes each line in the input file
# Throws:   nothing

def processFile():

    global primerKey, refKey, accKey, mgiKey

    lineNum = 0
    # For each line in the input file

    for line in inputFile.readlines():

        error = 0
        lineNum = lineNum + 1

        # Split the line into tokens
        tokens = string.split(line[:-1], '\t')

        try:
	    markerSymbol = tokens[0]	# not used
	    markerID = tokens[1]
	    name = tokens[2]
	    jnum = tokens[3]
	    regionCovered = tokens[4]
	    sequence1 = tokens[5]
	    sequence2 = tokens[6]
	    productSize = tokens[7]
	    notes = tokens[8]
	    sequenceIDs = tokens[9]
	    createdBy = tokens[10]
        except:
            exit(1, 'Invalid Line (%d): %s\n' % (lineNum, line))

	markerKey = loadlib.verifyMarker(markerID, lineNum, errorFile)
        referenceKey = loadlib.verifyReference(jnum, lineNum, errorFile)
	createdByKey = loadlib.verifyUser(createdBy, lineNum, errorFile)

        if markerKey == 0 or referenceKey == 0:
            # set error flag to true
            error = 1

	# sequence IDs
	seqAccList = string.split(sequenceIDs, '|')

        # if errors, continue to next record
        if error:
            continue

        # if no errors, process the primer

        primerFile.write('%d|%s||%d|%d|%s|%s|%s|%s|||%s|%s|%s|%s|%s\n' \
            % (primerKey, name, NA, vectorKey, segmentTypeKey, mgi_utils.prvalue(sequence1), \
	    mgi_utils.prvalue(sequence2), mgi_utils.prvalue(regionCovered), mgi_utils.prvalue(productSize), \
	    createdByKey, createdByKey, loaddate, loaddate))

        markerFile.write('%s|%s|%d|%s|%s|%s|%s|%s\n' % (primerKey, markerKey, referenceKey, relationship, createdByKey, createdByKey, loaddate,
	 loaddate))

        refFile.write('%s|%s|%s|0|0|%s|%s|%s|%s\n' % (refKey, primerKey, referenceKey, createdByKey, createdByKey, loaddate, loaddate))

        # MGI Accession ID for the marker

        accFile.write('%s|%s%d|%s|%s|1|%d|%d|0|1|%s|%s|%s|%s\n' \
            % (accKey, mgiPrefix, mgiKey, mgiPrefix, mgiKey, primerKey, mgiTypeKey, createdByKey, createdByKey, loaddate, loaddate))

        accKey = accKey + 1
        mgiKey = mgiKey + 1

	# sequence accession ids
	for acc in seqAccList:

	    if len(acc) == 0:
		continue

	    prefixPart, numericPart = accessionlib.split_accnum(acc)
            accFile.write('%s|%s|%s|%s|%s|%d|%d|0|1|%s|%s|%s|%s\n' \
                % (accKey, acc, prefixPart, numericPart, logicalDBKey, primerKey, mgiTypeKey, createdByKey, createdByKey, loaddate, loaddate))
            accRefFile.write('%s|%s|%s|%s|%s|%s\n' \
                % (accKey, referenceKey, createdByKey, createdByKey, loaddate, loaddate))
	    accKey = accKey + 1

	# notes

	if len(notes) > 0:
	   noteFile.write('%s|1|%s|%s|%s\n' \
		% (primerKey, notes, loaddate, loaddate))

	refKey = refKey + 1
        primerKey = primerKey + 1

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

