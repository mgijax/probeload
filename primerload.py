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
#		field 1: MGI Marker Accession ID
#		field 2: Primer Name
#		field 3: Reference (J:#####)
#		field 4: Region Covered
#		field 5: Sequence 1
#		field 6: Sequence 2
#		field 7: Repeat Unit
#		field 8: More Than One Product (y/n)
#		field 9: Product Size
#
# Outputs:
#
#       4 BCP files:
#
#       PRB_Probe.bcp                   master Primer records
#	PRB_Marker.bcp			Primer/Marker records
#       PRB_Reference.bcp         	Primer Reference records
#       ACC_Accession.bcp               Accession records
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

#globals

#
# from configuration file
#
passwordFileName = os.environ['MGI_DBPASSWORDFILE']
mode = os.environ['LOADMODE']
inputFileName = os.environ['PROBELOADINPUT']

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

primerTable = 'PRB_Probe'
markerTable = 'PRB_Marker'
refTable = 'PRB_Reference'
accTable = 'ACC_Accession'

primerFileName = primerTable + '.bcp'
markerFileName = markerTable + '.bcp'
refFileName = refTable + '.bcp'
accFileName = accTable + '.bcp'

diagFileName = ''	# diagnostic file name
errorFileName = ''	# error file name

primerKey = 0           # PRB_Probe._Probe_key
refKey = 0		# PRB_Reference._Reference_key
accKey = 0              # ACC_Accession._Accession_key
mgiKey = 0              # ACC_AccessionMax.maxNumericPart

dnaType = 'primer'	# PRB_Probe.DNAtype
relationship = 'A'	# PRB_Marker.relationship
NA = -2			# for Not Applicable fields
mgiTypeKey = 3		# Molecular Segment
mgiPrefix = "MGI:"

referenceDict = {}      # dictionary of references for quick lookup
markerDict = {}      	# dictionary of markers for quick lookup

cdate = mgi_utils.date('%m/%d/%Y')	# current date

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
    global primerFile, markerFile, refFile, accFile
 
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


# Purpose:  verify Marker Accession ID
# Returns:  Marker Key if Marker is valid, else 0
# Assumes:  nothing
# Effects:  verifies that the Marker exists either in the marker dictionary or the database
#	writes to the error file if the Marker is invalid
#	adds the marker id and key to the marker dictionary if the Marker is valid
# Throws:  nothing

def verifyMarker(
    markerID, 	# Accession ID of the Marker (string)
    lineNum	# line number (integer)
    ):

    global markerDict

    markerKey = 0

    if markerDict.has_key(markerID):
        errorFile.write('Duplicate Mouse Marker (%d) %s\n' % (lineNum, markerID))
    else:
        results = db.sql('select _Object_key from MRK_Acc_View where accID = "%s" ' % (markerID), 'auto')

        for r in results:
            if r['_Object_key'] is None:
                errorFile.write('Invalid Mouse Marker (%d) %s\n' % (lineNum, markerID))
                markerKey = 0
            else:
                markerKey = r['_Object_key']
                markerDict[markerID] = markerKey

    return markerKey

# Purpose:  verifies the input reference (J:)
# Returns:  the primary key of the reference or 0 if invalid
# Assumes:  nothing
# Effects:  verifies that the Reference exists by checking the referenceDict
#	dictionary for the reference ID or the database.
#	writes to the error file if the Reference is invalid.
#	adds the Reference ID/Key to the global referenceDict dictionary if the
#	reference is valid.
# Throws:

def verifyReference(
    referenceID,          # reference accession ID; J:#### (string)
    lineNum		  # line number (integer)
    ):

    global referenceDict

    if referenceDict.has_key(referenceID):
        referenceKey = referenceDict[referenceID]
    else:
        referenceKey = accessionlib.get_Object_key(referenceID, 'Reference')
        if referenceKey is None:
            errorFile.write('Invalid Reference (%d): %s\n' % (lineNum, referenceID))
            referenceKey = 0
        else:
            referenceDict[referenceID] = referenceKey

    return(referenceKey)

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

    bcpI = 'cat %s | bcp %s..' % (passwordFileName, db.get_sqlDatabase())
    bcpII = '-c -t\"|" -S%s -U%s' % (db.get_sqlServer(), db.get_sqlUser())
    truncateDB = 'dump transaction %s with truncate_only' % (db.get_sqlDatabase())

    bcp1 = '%s%s in %s %s' % (bcpI, primerTable, primerFileName, bcpII)
    bcp2 = '%s%s in %s %s' % (bcpI, markerTable, markerFileName, bcpII)
    bcp3 = '%s%s in %s %s' % (bcpI, refTable, refFileName, bcpII)
    bcp4 = '%s%s in %s %s' % (bcpI, accTable, accFileName, bcpII)

    for bcpCmd in [bcp1, bcp2, bcp3, bcp4]:
	diagFile.write('%s\n' % bcpCmd)
	os.system(bcpCmd)
	db.sql(truncateDB, None)

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
	    markerID = tokens[0]
	    name = tokens[1]
	    jnum = tokens[2]
	    regionCovered = tokens[3]
	    sequence1 = tokens[4]
	    sequence2 = tokens[5]
	    repeatUnit = tokens[6]
	    moreProduct = tokens[7]
	    productSize = tokens[8]
        except:
            exit(1, 'Invalid Line (%d): %s\n' % (lineNum, line))

	markerKey = verifyMarker(markerID, lineNum)
        referenceKey = verifyReference(jnum, lineNum)

	if moreProduct == 'y':
		moreProduct = '1'
	else:
		moreProduct = '0'

        if markerKey == 0 or referenceKey == 0:
            # set error flag to true
            error = 1

        # if errors, continue to next record
        if error:
            continue

        # if no errors, process the primer

        primerFile.write('%d|%s||%d|%d|%s|%s|%s||||%s|%s|%s|%s|%s|%s\n' \
            % (primerKey, name, NA, NA, sequence1, sequence2, mgi_utils.prvalue(regionCovered), \
	    dnaType, mgi_utils.prvalue(repeatUnit), mgi_utils.prvalue(productSize), moreProduct, cdate, cdate))

        markerFile.write('%d|%d|%s|%s|%s\n' % (primerKey, markerKey, relationship, cdate, cdate))

        refFile.write('%d|%d|%d|0|0|%s|%s\n' % (refKey, primerKey, referenceKey, cdate, cdate))

        # MGI Accession ID for the marker

        accFile.write('%d|%s%d|%s|%s|1|%d|%d|0|1|%s|%s|%s\n' \
            % (accKey, mgiPrefix, mgiKey, mgiPrefix, mgiKey, primerKey, mgiTypeKey, cdate, cdate, cdate))

        accKey = accKey + 1
        mgiKey = mgiKey + 1
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

