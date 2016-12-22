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
#	PRB_Alias
#	ACC_Accession
#	ACC_AccessionReference
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
#		field 2: MGI Marker Accession IDs (MGI:xxxx|MGI:xxxx)
#		field 3: Primer Name
#		field 4: Reference (J:#####)
#		field 5: Region Covered
#		field 6: Sequence 1
#		field 7: Sequence 2
#		field 8: Product Size
#		field 9: Notes
#               field 10: Nucleotide Sequence ID   (|-delimited)
#               field 11: Alias
#               field 12: Created By
#
# Outputs:
#
#       7 BCP files:
#
#       PRB_Probe.bcp                   master Primer records
#	PRB_Marker.bcp			Primer/Marker records
#       PRB_Reference.bcp         	Primer Reference records
#	PRB_Alias.bcp
#       ACC_Accession.bcp               Accession records
#       ACC_AccessionReference.bcp      Accession Reference records
#	PRB_Notes.bcp			Primer Notes
#
#	attach MGI ID to input primer file
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
user = os.environ['PG_DBUSER']
passwordFileName = os.environ['PG_1LINE_PASSFILE']
mode = os.environ['PRIMERMODE']
currentDir = os.environ['PRIMERLOADDIR']
inputFileName = os.environ['PRIMERDATAFILE']
outputDir = os.environ['OUTPUTDIR']

bcpCommand = os.environ['PG_DBUTILS'] + '/bin/bcpin.csh '

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
aliasFile = ''          # file descriptor
accFile = ''            # file descriptor
accRefFile = ''         # file descriptor
noteFile = ''		# file descriptor

primerTable = 'PRB_Probe'
markerTable = 'PRB_Marker'
refTable = 'PRB_Reference'
aliasTable = 'PRB_Alias'
accTable = 'ACC_Accession'
accRefTable = 'ACC_AccessionReference'
noteTable = 'PRB_Notes'
newPrimerFile = 'newPrimer.txt'

primerFileName = primerTable + '.bcp'
markerFileName = markerTable + '.bcp'
refFileName = refTable + '.bcp'
aliasFileName = aliasTable + '.bcp'
accFileName = accTable + '.bcp'
accRefFileName = accRefTable + '.bcp'
noteFileName = noteTable + '.bcp'
newPrimerFileName = newPrimerFile

diagFileName = ''	# diagnostic file name
errorFileName = ''	# error file name

primerKey = 0           # PRB_Probe._Probe_key
refKey = 0		# PRB_Reference._Reference_key
aliasKey = 0		# PRB_Reference._Reference_key
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
    global bcpCommand
    global diagFile, errorFile, inputFile, errorFileName, diagFileName
    global primerFile, markerFile, refFile, aliasFile, accFile, accRefFile, noteFile, newPrimerFile
 
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
        aliasFile = open(aliasFileName, 'w')
    except:
        exit(1, 'Could not open file %s\n' % aliasFileName)

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

    try:
        newPrimerFile = open(newPrimerFileName, 'w')
    except:
        exit(1, 'Could not open file %s\n' % newPrimerFileName)

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

    global primerKey, refKey, aliasKey, accKey, mgiKey

    results = db.sql('select max(_Probe_key) + 1 as maxKey from PRB_Probe', 'auto')
    primerKey = results[0]['maxKey']

    results = db.sql('select max(_Reference_key) + 1 as maxKey from PRB_Reference', 'auto')
    refKey = results[0]['maxKey']

    results = db.sql('select max(_Alias_key) + 1 as maxKey from PRB_Alias', 'auto')
    aliasKey = results[0]['maxKey']

    results = db.sql('select max(_Accession_key) + 1 as maxKey from ACC_Accession', 'auto')
    accKey = results[0]['maxKey']

    results = db.sql('select maxNumericPart + 1 as maxKey from ACC_AccessionMax ' + \
        'where prefixPart = "%s"' % (mgiPrefix), 'auto')
    mgiKey = results[0]['maxKey']

# Purpose:  BCPs the data into the database
# Returns:  nothing
# Assumes:  nothing
# Effects:  BCPs the data into the database
# Throws:   nothing

def bcpFiles():

    if DEBUG or not bcpon:
        return

    primerFile.close()
    markerFile.close()
    refFile.close()
    aliasFile.close()
    accFile.close()
    accRefFile.close()
    noteFile.close()

    db.commit()

    bcp1 = bcpCommand % (primerTable, primerFileName)
    bcp2 = bcpCommand % (markerTable, markerFileName)
    bcp3 = bcpCommand % (refTable, refFileName)
    bcp4 = bcpCommand % (aliasTable, aliasFileName)
    bcp5 = bcpCommand % (accTable, accFileName)
    bcp6 = bcpCommand % (accTable, accFileName)
    bcp7 = bcpCommand % (noteTable, noteFileName)

    for bcpCmd in [bcp1, bcp2, bcp3, bcp4, bcp5, bcp6, bcp7]:
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

    global primerKey, refKey, aliasKey, accKey, mgiKey

    lineNum = 0
    # For each line in the input file

    for line in inputFile.readlines():

        error = 0
        lineNum = lineNum + 1

        # Split the line into tokens
        tokens = string.split(line[:-1], '\t')

        try:
	    markerSymbol = tokens[0]	# not used
	    markerIDs = string.split(tokens[1], '|')
	    name = tokens[2]
	    jnum = tokens[3]
	    regionCovered = tokens[4]
	    sequence1 = tokens[5]
	    sequence2 = tokens[6]
	    productSize = tokens[7]
	    notes = tokens[8]
	    sequenceIDs = tokens[9]
	    aliasList = string.split(tokens[10], '|')
	    createdBy = tokens[11]
        except:
            exit(1, 'Invalid Line (%d): %s\n' % (lineNum, line))

	# marker IDs

	markerList = []
	for markerID in markerIDs:

	    markerKey = loadlib.verifyMarker(markerID, lineNum, errorFile)

	    if len(markerID) > 0 and markerKey == 0:
	        errorFile.write('Invalid Marker:  %s, %s\n' % (name, markerID))
	        error = 1
            elif len(markerID) > 0:
		markerList.append(markerKey)

        referenceKey = loadlib.verifyReference(jnum, lineNum, errorFile)
	createdByKey = loadlib.verifyUser(createdBy, lineNum, errorFile)

	# sequence IDs
	seqAccList = string.split(sequenceIDs, '|')

        # if errors, continue to next record
        if error:
            continue

        # if no errors, process the primer

        primerFile.write('%d\t%s||%d|%d\t%s\t%s\t%s\t%s||\t%s\t%s\t%s\t%s\t%s\n' \
            % (primerKey, name, NA, vectorKey, segmentTypeKey, mgi_utils.prvalue(sequence1), \
	    mgi_utils.prvalue(sequence2), mgi_utils.prvalue(regionCovered), mgi_utils.prvalue(productSize), \
	    createdByKey, createdByKey, loaddate, loaddate))

	for markerKey in markerList:
	    if markerList.count(markerKey) == 1:
                markerFile.write('%s\t%s|%d\t%s\t%s\t%s\t%s\t%s\n' \
		    % (primerKey, markerKey, referenceKey, relationship, createdByKey, createdByKey, loaddate, loaddate))
            else:
		errorFile.write('Invalid Marker Duplicate:  %s, %s\n' % (name, markerID))

	# loaddate))

        refFile.write('%s\t%s\t%s|0|0\t%s\t%s\t%s\t%s\n' % (refKey, primerKey, referenceKey, createdByKey, createdByKey, loaddate, loaddate))

        # aliases

        for alias in aliasList:
            if len(alias) == 0:
                continue
            aliasFile.write('%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
                    % (aliasKey, refKey, alias, createdByKey, createdByKey, loaddate, loaddate))
            aliasKey = aliasKey + 1

        # MGI Accession ID for the marker

        accFile.write('%s\t%s%d\t%s\t%s|1|%d|%d|0|1\t%s\t%s\t%s\t%s\n' \
            % (accKey, mgiPrefix, mgiKey, mgiPrefix, mgiKey, primerKey, mgiTypeKey, createdByKey, createdByKey, loaddate, loaddate))

	newPrimerFile.write('%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s%d\n' \
	   % (markerSymbol, string.join(markerIDs, '|'), name, jnum, regionCovered, sequence1, sequence2, productSize, notes, sequenceIDs, createdBy, mgiPrefix, mgiKey))

        accKey = accKey + 1
        mgiKey = mgiKey + 1

	# sequence accession ids
	for acc in seqAccList:

	    if len(acc) == 0:
		continue

	    prefixPart, numericPart = accessionlib.split_accnum(acc)
            accFile.write('%s\t%s\t%s\t%s\t%s|%d|%d|0|1\t%s\t%s\t%s\t%s\n' \
                % (accKey, acc, prefixPart, numericPart, logicalDBKey, primerKey, mgiTypeKey, createdByKey, createdByKey, loaddate, loaddate))
            accRefFile.write('%s\t%s\t%s\t%s\t%s\t%s\n' \
                % (accKey, referenceKey, createdByKey, createdByKey, loaddate, loaddate))
	    accKey = accKey + 1

	# notes

	if len(notes) > 0:
	   noteFile.write('%s|1\t%s\t%s\t%s\n' \
		% (primerKey, notes, loaddate, loaddate))

	refKey = refKey + 1
        primerKey = primerKey + 1

    #	end of "for line in inputFile.readlines():"

    #
    # Update the AccessionMax value
    #

    if not DEBUG:
        db.sql('select * from ACC_setMax (%d)' % (lineNum), None)

#
# Main
#

init()
verifyMode()
setPrimaryKeys()
processFile()
bcpFiles()
exit(0)

