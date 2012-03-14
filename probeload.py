#!/usr/local/bin/python

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
#       PRB_Alias
#	ACC_Accession
#	ACC_AccessionReference
#	PRB_Notes
#
# Requirements Satisfied by This Program:
#
# Usage:
#	probeload.py
#
# Envvars:
#
# Inputs:
#
#	A tab-delimited file in the format:
#		field 1:  Probe Name		required
#		field 2:  Reference 		required J:#####
#		field 3:  Parent 		allows null or MGI ID
#		field 4:  Source Name		allows null
#		field 5:  Organism		allows null
#		field 6:  Strain		allows null
#		field 7:  Tissue		allows null
#		field 8:  Gender		allows null
#		field 9:  Cell Line		allows null
#		field 10: Age			allows null
#		field 11: Vector Type		required vocab term
#		field 12: Segment Type		required vocab term
#		field 13: Region Covered	allows null
#		field 14: Insert Site		allows null
#		field 15: Insert Size		allows null
#		field 16: MGI Marker    	MGI ID|MGI ID|...
#		field 17: Relationship		required
#		field 18: Sequence ID		LogicalDB:Acc ID|...
#		field 19: Alias			allows null
#		field 20: Notes			allows null
#		field 21: Raw Sequence Note	allows null
#		field 22: Created By		required
#
# 	If Parent is not null, then set Source Name = Source Name of Parent Probe
#	Parent overrides Source
#	
# Outputs:
#
#       7 BCP files:
#
#       PRB_Probe.bcp                   master Probe records
#	PRB_Marker.bcp			Probe/Marker records
#       PRB_Reference.bcp         	Probe Reference records
#       PRB_Alias
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
# History
#
# 03/13/2012	lec
#	- TR10976/add rawNotes
#
# 12/16/2009	lec
#	- TR9931/Eurexpress/add field 3/Parent Probe (derivedFrom)
#
# 11/04/2009	lec
#	- TR9931;add ability to add multiple markers to PRB_Marker
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
probeFile = ''          # file descriptor
markerFile = ''		# file descriptor
refFile = ''            # file descriptor
aliasFile = ''          # file descriptor
accFile = ''            # file descriptor
accRefFile = ''         # file descriptor
noteFile = ''		# file descriptor

probeTable = 'PRB_Probe'
markerTable = 'PRB_Marker'
refTable = 'PRB_Reference'
aliasTable = 'PRB_Alias'
accTable = 'ACC_Accession'
accRefTable = 'ACC_AccessionReference'
noteTable = 'PRB_Notes'
newProbeFile = 'newProbe.txt'
rawNoteFile = 'rawNote.txt'

probeFileName = outputDir + '/' + probeTable + '.bcp'
markerFileName = outputDir + '/' + markerTable + '.bcp'
refFileName = outputDir + '/' + refTable + '.bcp'
aliasFileName = outputDir + '/' + aliasTable + '.bcp'
accFileName = outputDir + '/' + accTable + '.bcp'
accRefFileName = outputDir + '/' + accRefTable + '.bcp'
noteFileName = outputDir + '/' + noteTable + '.bcp'
newProbeFileName = outputDir + '/' + newProbeFile
rawNoteFileName = outputDir + '/' + rawNoteFile

diagFileName = ''	# diagnostic file name
errorFileName = ''	# error file name

probeKey = 0            # PRB_Probe._Probe_key
refKey = 0		# PRB_Reference._Reference_key
aliasKey = 0		# PRB_Reference._Reference_key
accKey = 0              # ACC_Accession._Accession_key
mgiKey = 0              # ACC_AccessionMax.maxNumericPart

NA = -2			# for Not Applicable fields
mgiTypeKey = 3		# Molecular Segment
mgiPrefix = "MGI:"

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
    global probeFile, markerFile, refFile, aliasFile, accFile, accRefFile, noteFile
    global newProbeFile, rawNoteFile
 
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
        newProbeFile = open(newProbeFileName, 'w')
    except:
        exit(1, 'Could not open file %s\n' % newProbeFileName)

    try:
        rawNoteFile = open(rawNoteFileName, 'w')
    except:
        exit(1, 'Could not open file %s\n' % rawNoteFileName)

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

# Purpose:  verify Parent Probe Accession ID
# Returns:  Probe Key if Parent Probe is valid, else 0
#           Source Key if Parent Probe is valid, else 0
# Assumes:  nothing
# Effects:  verifies that the Parent Probe exists either in the Parent Probe dictionary or the database
#       writes to the error file if the Parent Probe is invalid
#       adds the Parent Probe id and key to the Parent Probe dictionary if the Parent Probe is valid
# Throws:  nothing

def verifyParentProbe(
    probeID,    # Accession ID of the Probe (string)
    lineNum,    # line number (integer)
    errorFile   # error file (file descriptor)
    ):

    probeKey = 0
    sourceKey = 0

    results = db.sql('''
        	select a._Object_key, p._Source_key 
		from ACC_Accession a, PRB_Probe p 
		where a.accID = "%s"
		and a._MGIType_key = 3
		and a._Object_key = p._Probe_key
		''' % (probeID), 'auto')

    for r in results:
        if r['_Source_key'] is None:
            if errorFile != None:
                errorFile.write('Invalid Derivied Probe (%d) %s\n' % (lineNum, probeID))
            probeKey = 0
        else:
            probeKey = r['_Object_key']
            sourceKey = r['_Source_key']

    return probeKey, sourceKey

# Purpose:  sets global primary key variables
# Returns:  nothing
# Assumes:  nothing
# Effects:  sets global primary key variables
# Throws:   nothing

def setPrimaryKeys():

    global probeKey, refKey, aliasKey, accKey, mgiKey

    results = db.sql('select maxKey = max(_Probe_key) + 1 from PRB_Probe', 'auto')
    probeKey = results[0]['maxKey']

    results = db.sql('select maxKey = max(_Reference_key) + 1 from PRB_Reference', 'auto')
    refKey = results[0]['maxKey']

    results = db.sql('select maxKey = max(_Alias_key) + 1 from PRB_Alias', 'auto')
    aliasKey = results[0]['maxKey']

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
    aliasFile.close()
    accFile.close()
    accRefFile.close()
    noteFile.close()
    newProbeFile.close()
    rawNoteFile.close()

    bcpI = 'cat %s | bcp %s..' % (passwordFileName, db.get_sqlDatabase())
    bcpII = '-c -t\"|" -S%s -U%s' % (db.get_sqlServer(), db.get_sqlUser())

    bcp1 = '%s%s in %s %s' % (bcpI, probeTable, probeFileName, bcpII)
    bcp2 = '%s%s in %s %s' % (bcpI, markerTable, markerFileName, bcpII)
    bcp3 = '%s%s in %s %s' % (bcpI, refTable, refFileName, bcpII)
    bcp4 = '%s%s in %s %s' % (bcpI, aliasTable, aliasFileName, bcpII)
    bcp5 = '%s%s in %s %s' % (bcpI, accTable, accFileName, bcpII)
    bcp6 = '%s%s in %s %s' % (bcpI, accRefTable, accRefFileName, bcpII)
    bcp7 = '%s%s in %s %s' % (bcpI, noteTable, noteFileName, bcpII)

    for bcpCmd in [bcp1, bcp2, bcp3, bcp4, bcp5, bcp6, bcp7]:
	diagFile.write('%s\n' % bcpCmd)
	os.system(bcpCmd)

    return

# Purpose:  processes data
# Returns:  nothing
# Assumes:  nothing
# Effects:  verifies and processes each line in the input file
# Throws:   nothing

def processFile():

    global probeKey, refKey, aliasKey, accKey, mgiKey

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
	    parentID = tokens[2]
	    sourceName = tokens[3]
	    organism = tokens[4]
	    strain = tokens[5]
	    tissue = tokens[6]
	    gender = tokens[7]
	    cellLine = tokens[8]
	    age = tokens[9]
	    vectorType = tokens[10]
	    segmentType = tokens[11]
	    regionCovered = tokens[12]
	    insertSite = tokens[13]
	    insertSize = tokens[14]
	    markerIDs = string.split(tokens[15], '|')
	    relationship = tokens[16]
	    sequenceIDs = tokens[17]
	    aliasList = string.split(tokens[18], '|')
	    notes = tokens[19]
	    rawnotes = tokens[20]
	    createdBy = tokens[21]
        except:
            exit(1, 'Invalid Line (%d): %s\n' % (lineNum, line))

	isParent = 0
	isSource = 0
	parentProbeKey = '';
	sourceKey = 0

	if parentID != '':
	    isParent = 1

	if sourceName != '':
	    isSource = 1

	if not isParent and not isSource:
	    organismKey = sourceloadlib.verifyOrganism(organism, lineNum, errorFile)
	    strainKey = sourceloadlib.verifyStrain(strain, lineNum, errorFile)
	    tissueKey = sourceloadlib.verifyTissue(tissue, lineNum, errorFile)
	    genderKey = sourceloadlib.verifyGender(gender, lineNum, errorFile)
	    cellLineKey = sourceloadlib.verifyCellLine(cellLine, lineNum, errorFile)
	    vectorKey = sourceloadlib.verifyVectorType(vectorType, lineNum, errorFile)
	    segmentTypeKey = sourceloadlib.verifySegmentType(segmentType, lineNum, errorFile)
	    sourceKey = sourceloadlib.verifySource(segmentTypeKey, \
		vectorKey, organismKey, strainKey, \
		tissueKey, genderKey, cellLineKey, age, lineNum, errorFile)

	    if organismKey == 0 or strainKey == 0 or tissueKey == 0 or \
               genderKey == 0 or cellLineKey == 0 or vectorKey == 0 or \
               segmentTypeKey == 0 or sourceKey == 0:
	        error = 1

        elif not isParent and isSource:
	    vectorKey = sourceloadlib.verifyVectorType(vectorType, lineNum, errorFile)
	    segmentTypeKey = sourceloadlib.verifySegmentType(segmentType, lineNum, errorFile)
	    sourceKey = sourceloadlib.verifyLibrary(sourceName, lineNum, errorFile)

	    if vectorKey == 0 or segmentTypeKey == 0 or sourceKey == 0:
	        error = 1

	# parent from = yes, source given = yes or no (ignored)
	else:
	    parentProbeKey, sourceKey = verifyParentProbe(parentID, lineNum, errorFile)
	    vectorKey = sourceloadlib.verifyVectorType(vectorType, lineNum, errorFile)
	    segmentTypeKey = sourceloadlib.verifySegmentType(segmentType, lineNum, errorFile)

	    if parentProbeKey == 0 or sourceKey == 0 or vectorKey == 0 or segmentTypeKey == 0:
	        error = 1

        referenceKey = loadlib.verifyReference(jnum, lineNum, errorFile)
	createdByKey = loadlib.verifyUser(createdBy, lineNum, errorFile)

	if referenceKey == 0:
	    errorFile.write('Invalid Reference:  %s\n' % (jnum))
	    error = 1

	if createdByKey == 0:
	    errorFile.write('Invalid Creator:  %s\n\n' % (createdBy))
	    error = 1

	# marker IDs

	markerList = []
	for markerID in markerIDs:

	    markerKey = loadlib.verifyMarker(markerID, lineNum, errorFile)

	    if len(markerID) > 0 and markerKey == 0:
	        errorFile.write('Invalid Marker:  %s, %s\n' % (name, markerID))
	        error = 1
            elif len(markerID) > 0:
		markerList.append(markerKey)

	# sequence IDs
	seqAccDict = {}
	for seqID in string.split(sequenceIDs, '|'):
	    if len(seqID) > 0:
	        [logicalDB, acc] = string.split(seqID, ':')
	        logicalDBKey = loadlib.verifyLogicalDB(logicalDB, lineNum, errorFile)
	        if logicalDBKey > 0:
		    seqAccDict[acc] = logicalDBKey

        # if errors, continue to next record
        if error:
            continue

        # if no errors, process the probe

        probeFile.write('%d|%s|%s|%s|%s|%s|||%s|%s|%s||%s|%s|%s|%s\n' \
            % (probeKey, name, parentProbeKey, sourceKey, vectorKey, segmentTypeKey, mgi_utils.prvalue(regionCovered), \
	    mgi_utils.prvalue(insertSite), mgi_utils.prvalue(insertSize), createdByKey, createdByKey, loaddate, loaddate))

	for markerKey in markerList:
	    if markerList.count(markerKey) == 1:
                markerFile.write('%s|%s|%d|%s|%s|%s|%s|%s\n' \
		    % (probeKey, markerKey, referenceKey, relationship, createdByKey, createdByKey, loaddate, loaddate))
            else:
		errorFile.write('Invalid Marker Duplicate:  %s, %s\n' % (name, markerID))

        refFile.write('%s|%s|%s|0|0|%s|%s|%s|%s\n' \
		% (refKey, probeKey, referenceKey, createdByKey, createdByKey, loaddate, loaddate))

        # aliases

        for alias in aliasList:
	    if len(alias) == 0:
		continue
            aliasFile.write('%s|%s|%s|%s|%s|%s|%s\n' \
		    % (aliasKey, refKey, alias, createdByKey, createdByKey, loaddate, loaddate))
	    aliasKey = aliasKey + 1

        # MGI Accession ID for the marker

        accFile.write('%s|%s%d|%s|%s|1|%d|%d|0|1|%s|%s|%s|%s\n' \
            % (accKey, mgiPrefix, mgiKey, mgiPrefix, mgiKey, probeKey, mgiTypeKey, createdByKey, createdByKey, loaddate, loaddate))

	# Print out a new text file and attach the new MGI Probe IDs as the last field

        newProbeFile.write('%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s%d\n' \
	    % (name, jnum, \
	    mgi_utils.prvalue(sourceName), \
	    organism, \
	    mgi_utils.prvalue(strain), \
	    mgi_utils.prvalue(tissue), \
	    mgi_utils.prvalue(gender), \
	    mgi_utils.prvalue(cellLine), \
	    mgi_utils.prvalue(age), \
	    mgi_utils.prvalue(vectorType), \
	    mgi_utils.prvalue(segmentType), \
	    mgi_utils.prvalue(regionCovered) + \
	    mgi_utils.prvalue(insertSite), \
	    mgi_utils.prvalue(insertSize), \
	    string.join(markerIDs, '|'), \
	    relationship, \
	    mgi_utils.prvalue(sequenceIDs), \
	    string.join(aliasList, '|'), \
	    mgi_utils.prvalue(notes), \
	    createdBy, mgiPrefix, mgiKey))

	# Print out a raw note file

        if len(rawnotes) > 0:
            rawNoteFile.write('%s%d\t%s\n' % (mgiPrefix, mgiKey, rawnotes))

	# Notes

        noteSeq = 1
		
        while len(notes) > 255:
	    noteFile.write('%s|%d|%s|%s|%s\n' % (probeKey, noteSeq, notes[:255], loaddate, loaddate))
            newnote = notes[255:]
            notes = newnote
            noteSeq = noteSeq + 1

        if len(notes) > 0:
	    noteFile.write('%s|%d|%s|%s|%s\n' % (probeKey, noteSeq, notes, loaddate, loaddate))

        accKey = accKey + 1
        mgiKey = mgiKey + 1

	# sequence accession ids
	for acc in seqAccDict.keys():
	    prefixPart, numericPart = accessionlib.split_accnum(acc)
            accFile.write('%s|%s|%s|%s|%s|%d|%d|0|1|%s|%s|%s|%s\n' \
                % (accKey, acc, prefixPart, numericPart, seqAccDict[acc], probeKey, mgiTypeKey, createdByKey, createdByKey, loaddate, loaddate))
            accRefFile.write('%s|%s|%s|%s|%s|%s\n' \
                % (accKey, referenceKey, createdByKey, createdByKey, loaddate, loaddate))
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

