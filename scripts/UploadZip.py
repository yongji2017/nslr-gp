# -*- coding: utf-8 -*-
# ---------------------------------------------------------------------------
# Name: UploadZip.py
# Purpose:    Upload the zip file which contains the file geodatabase and then extract the features from the feature classes 
# Author:     Yongji Zhang(yxz@eagle.co.nz)
# Date Created:    2021-09-15
# Last Updated:    2021-09-15
# Copyright:   (c) Eagle Technology
# ArcGIS Pro Version:   2.8.0
# Python Version:   3.9.0
# Script Version: 1.0.0
#--------------------------------------------------------------------------------------------------------------------------------
# This script will be published to ArcGIS enterprise as the geoprocessing service
# Inputs: all the input parameters are defined in the config.py and ready by the main script:
#         zip file (to be uploaded)#         
# Output: a string to contain the feature set in json format (array of esri graphics)
#
# Authored for NZTA NSLR
# --------------------------------------------------------------------------------------------------------------------------------

# import modules
# Import arcpy module and others
import arcpy
import os
import zipfile
import uuid
import logging
import json
import datetime

from arcpy import env
arcpy.env.overwriteOutput = True


class slzAtributes:
    OBJECTID="OBJECTID"
    SpeedLimitZoneId="speedLimitZoneId"
    RcaZoneReferenceId = "rcaZoneReferenceId"
    SpeedLimitZoneName = "speedLimitZoneName"
    SpeedCategoryId = "speedCategoryId"
    SpeedValueTypeId =  "speedValueTypeId"
    SpeedLimitZoneStatusTypeId ="speedLimitZoneStatusTypeId"
    SpeedLimitZoneStartDate = "speedLimitZoneStartDate"
    SpeedLimitZoneEndDate = "speedLimitZoneEndDate"
    RcaZoneReferenceName = "rcaZoneReferenceName"


# Set global variables
# Logging
enableLogging = "true" # Use within code to print and log messages - printMessage("xxx","info"), printMessage("xxx","warning"), printMessage("xxx","error")
logFile = os.path.join(os.path.dirname(__file__), "UploadZipFile.log") # e.g. os.path.join(os.path.dirname(__file__), "UploadZipFile.log")
archiveLogFiles = "false"
errorMessages = {"NOTZIP":"The input is not a zip file",
                  "INPUTREQ":"The input zip file is required", 
                  "WRONGFORMAT":"The input is wrong file format",
                  "MISSINGLAYER":"The required layer does not exist in zipped fgdb",
                  "NOOUTPUT": "No output is generated" }

# layer/table names
slzTableName = "SpeedLimitZone"
geomDraftLayerName = "SpeedLimitZoneGeometryDraft"
rcaReferenceTableName = "RCAZoneReference"
# required layers/tables
datasets = [slzTableName, geomDraftLayerName, rcaReferenceTableName]

# field names
slzFields = [slzAtributes.OBJECTID, 
            slzAtributes.SpeedLimitZoneId,
            slzAtributes.RcaZoneReferenceId,
            slzAtributes.SpeedLimitZoneName,
            slzAtributes.SpeedCategoryId,
            slzAtributes.SpeedValueTypeId,
            slzAtributes.SpeedLimitZoneStatusTypeId,
            slzAtributes.SpeedLimitZoneStartDate,
            slzAtributes.SpeedLimitZoneEndDate]

geomDraftFields = [slzAtributes.SpeedLimitZoneId,'SHAPE@JSON']        
rcaReferenceFileds = [slzAtributes.RcaZoneReferenceId, slzAtributes.RcaZoneReferenceName]

domainFields = ['speedCategoryId', 'speedLimitZoneStatusTypeId','speedValueTypeId', "speedLimitZoneStructureTypeId","speedZoneDirectionTypeId", "speedLimitZoneSeasonalType"]


speedLimitRecordViewTemplate = {
    "legalInstrumentTitle": None,
    "legalInstrumentURL": None,
    "legalInstrumentId": None,
    "legalReference": None,
    "gazetteIssueNumber": None,
    "gazettePageNumber": None,
    "publicationDate": None,
    "effectiveDateFrom": None,
    "yearEnacted": None,
    "speedLimitZoneReasonName": None,   
    "speedLimitZoneVarPrdName": None,
    "speedLimitZoneLanePurposeName": None,
    "speedLimitZoneStructureTypeName": None,
    "speedLimitZoneStatusTypeName": None,
    "speedValueTypeName": None,
    "speedCategoryName": None,
    "variableSpeeds": None,    
    "rcaZoneReferenceName": None,
    "speedLimitZoneId": "{00000000-0000-0000-0000-000000000000}",
    "speedManagementPlanName": None,
    "speedManagementPlanURL": None,
    "speedLimitZoneName": None,
    "speedLimitZoneStatusDate": -2209161600000,
    "speedLimitZoneStartDate": -2209161600000,
    "speedLimitZoneEndDate": None,
    "speedLimitZoneApprovalURL": None,
    "speedLimitZoneApprovalEndDate": None,
    "speedLimitZoneSeasonalRecurring": None,
    "speedLimitZoneSeasonalDesc": None,
    "speedLimitZoneEmergencyReason": None,
    "speedLimitZoneDescription": None,
    "speedZoneDirectionTypeId": None,
    "speedLimitZoneLock": 0,
    "speedLimitZoneLockOwner": None,
    "speedLimitZoneLockStartDate": None,
    "lanesOutFromCentreline": None,
    "speedLimitZoneSubmittedBy": None,
    "GlobalIdGeometry": None,
    "speedLimitZoneSeasonalRecur": None,
    "parentSpeedLimitZoneId": None,
    "CorrectionCount": None,
    "ClarificationCount": None,
    "GlobalID": "{00000000-0000-0000-0000-000000000000}", 
    "OBJECTID": None,
    "displaySpeed": None,
    "GeometryCount": None,
    "lastEditedUser": None,
    "geometry": None
}

##  ------------------ main function-------------------------
def mainFunction():
    result = None
    try:
       
        input_file = arcpy.GetParameterAsText(0)              
        # -----testing ------------
        # input_file = r'C:\projects\gp\data\YZTest_noGeo.gdb.zip' 
        printMessage("input file: {}".format(input_file),"info")
             
        # check if the inpu_file uis empty
        if input_file == '#' or not input_file: processErrorOutput(errorMessages["INPUTREQ"])           

        # check if the input file is zip file
        isZip = zipfile.is_zipfile(input_file)       
        if not isZip: processErrorOutput(errorMessages["WRONGFORMAT"])        
            
        # validate the feature classes and fields in the zip file
        processLayers = checkDatasets(input_file)
        if (len(processLayers) != len(datasets)): processErrorOutput(errorMessages["MISSINGLAYER"])
        
        # generate the json outout from the process layers
        outputParamValue = processOutputResult(processLayers)
        if outputParamValue:
            arcpy.SetParameterAsText(1, outputParamValue)
        else:            
            processErrorOutput(errorMessages["NOOUTPUT"])        
            
    except arcpy.ExecuteError:
        processErrorOutput("arcpy.ExecuteError: {0}".format(arcpy.GetMessages(2)))          
        
    
    except Exception as ex:
        processErrorOutput("Exception: {0}".format(ex.message))  
     

def checkDatasets (input_file):    
    outLayers = {}
    try:
        # Extract zip
        zf = zipfile.ZipFile(input_file, 'r')                      
        # Create a folder in the scratch directory to extract zip to
        guid = str(uuid.uuid1())            
        zipFolder = os.path.join(arcpy.env.scratchFolder, guid)
        os.mkdir(zipFolder)
        printMessage("Zip Folder {}".format(zipFolder),"info")        
    
        # unzip file
        zf.extractall(zipFolder)
        zf.close
 
        ## work through all the FeatureClasses inside the extracted zip folder
        for dirpath, dirnames, filenames in arcpy.da.Walk(zipFolder, datatype=["FeatureClass", "Table"]):            
            for filename in filenames:                 
                if filename in datasets:
                    fc = os.path.join(dirpath, filename)                   
                    outLayers[filename] = fc
                    printMessage("fc: {}".format(fc),"info")               
              
    except Exception as ex:            
        printMessage("error {}".format(ex),"error")           
    return outLayers

def isDateField(fieldName, fieldList):
    for field in fieldList:
        if (field.name.lower() == fieldName.lower()):
            if field.type == "Date":            
                return True
    return False

def getRcaName (rcaid, rcaRows):
    if (rcaId in rcaRows):
        return rca[rcaid]
    else:
        return ''

def getDomainDesc (fieldName, fieldValue, fieldList, gdbDomains):

    if fieldValue == None: return ''
    # if fieldName in domainFields:
    domain_name = ''
    for field in fieldList:
        if (field.name.lower() == fieldName.lower()):
            domain_name = field.domain                  
            domains =  [d for d in gdbDomains if d.name == domain_name]
            if len(domains)> 0:
                domain = domains[0]
                coded_values = domain.codedValues                   
                return coded_values[fieldValue]                    
    return ''


def processOutputResult (layers):
    printMessage("Generating output json ...","info")
    outParamValue = [{}]
    try:
        speedLimitZoneLayer = layers[slzTableName] 
        geometryDraftLayer = layers[geomDraftLayerName]
        rcaLayer = layers[rcaReferenceTableName]

        desc = arcpy.Describe(speedLimitZoneLayer)
        gdbFile = desc.path
        printMessage("process file geodatbase: {}".format(gdbFile),"info")

        fieldList  = desc.fields
        gdbDomains = arcpy.da.ListDomains(gdbFile)
      
        features = []
        geomRows = {}   
        rcaRows = {}   
       
        # Get all the draft geomtries into dict
        printMessage("reading geometry draft features ...","info")
        with arcpy.da.SearchCursor(geometryDraftLayer, geomDraftFields) as cursor:
            for row in cursor:                
                geom = json.loads(row[1])             
                geomRows[row[0]] = geom              
        printMessage("total draft geometries: {}".format(len(geomRows)),"info") 
        del cursor

        # Get all the rca references
        printMessage("reading rca reference records ...","info")
        with arcpy.da.SearchCursor(rcaLayer, rcaReferenceFileds) as cursor:
            for row in cursor:                                           
                rcaRows[row[0]] = row[1]              
        printMessage("total rca reference: {}".format(len(rcaRows)),"info") 
        del cursor

        # Go through each speed limit zone record, generate json including the associated geometry
        printMessage("reading speed limit zone records ...","info")         
        with arcpy.da.SearchCursor(speedLimitZoneLayer, slzFields) as searchCursor:
            for row in searchCursor:               
                feat = {}               
                attr = {**speedLimitRecordViewTemplate}  

                speedLimitZoneId = row[1]             
                printMessage("process speedLimitZoneId: {}".format(speedLimitZoneId),"info")
               
                #  only process the record with geometry
                if (speedLimitZoneId in geomRows):
                    # Adding attributes from SpeedLimitZone table to attr{}
                    idx = 0                    
                    for field in slzFields: 
                        fieldValue = row[idx]  
                        if isDateField(field, fieldList) == True and fieldValue != None:
                            attr[field] = str(fieldValue)                         
                        else:                           
                            attr[field] = fieldValue                                                
                        idx +=1
 
                    rcaRefId = row[2]
                    rcaRefName = rcaRows[rcaRefId]  

                    # get the domain desc for fields 'speedCategoryId':row[4], 'speedValueTypeId':row[5], 'speedLimitZoneStatusTypeId':row[6]
                    speedCatId = row[4]
                    speedCatIdName = getDomainDesc(slzAtributes.SpeedCategoryId, speedCatId, fieldList, gdbDomains)

                    speedValue = row[5]
                    speedValueTypeName = getDomainDesc(slzAtributes.SpeedValueTypeId, speedValue, fieldList, gdbDomains)

                    slzStatusValue = row[6]
                    slzStatusTypeName = getDomainDesc(slzAtributes.SpeedLimitZoneStatusTypeId, slzStatusValue, fieldList, gdbDomains)

                    attr["rcaZoneReferenceName"] = rcaRefName
                    attr["speedLimitZoneStatusTypeName"]= slzStatusTypeName
                    attr["speedValueTypeName"] = speedValueTypeName
                    attr["speedCategoryName"] = speedCatIdName
                    
                    feat["attributes"] = attr
                    feat["geometry"] = geomRows[speedLimitZoneId]
                    feat["geometry"]["type"] = "polygon"  
                    features.append(feat)  
      
               
        del searchCursor

        # Convert dictionary(json) to string
        if len(features) > 0:
            outParamValue = json.dumps(features)
        printMessage("output data: {}".format(outParamValue),"info")
    except Exception as ex:
        # print ('error: {}'.format(ex))  
        printMessage("{}".format(ex),"error")       
    return outParamValue



def processErrorOutput(message):
    printMessage(message,"error")
    outputValues = []
    # outMessage = [{"error": {"message":"The input is not a zip file"}}]
    outputValueObj = {}   
    outputValueObj["error"] = {"message": message}
    outputValues.append(outputValueObj)
    outParamValue = json.dumps(outputValues)
    arcpy.SetParameterAsText(1, outParamValue)
    sys.exit()


# Start of print and logging message function
def printMessage(message,type):
    print(message)
    if (type.lower() == "warning"):
        arcpy.AddWarning(message)       
        # Logging
        if (enableLogging == "true"):
            logger.warning(message)     
    elif (type.lower() == "error"):
        arcpy.AddError(message)       
        # Logging
        if (enableLogging == "true"):
            logger.error(message)   
    else:
        arcpy.AddMessage(message)       
        # Logging
        if (enableLogging == "true"):
            logger.info(message)                
# End of print and logging message function


# Start of set logging function
def setLogging(logFile):
    # Create a logger
    logger = logging.getLogger(os.path.basename(__file__))
    logger.setLevel(logging.DEBUG)
    # Setup log message handler
    logMessage = logging.FileHandler(logFile)
    # Setup the log formatting
    logFormat = logging.Formatter("%(asctime)s: %(levelname)s - %(message)s", "%d/%m/%Y - %H:%M:%S")
    # Add formatter to log message handler
    logMessage.setFormatter(logFormat)
    # Add log message handler to logger
    logger.addHandler(logMessage)
    return logger, logMessage
# End of set logging function
      
# -------------------- main --------------------
if __name__ == "__main__":
    # Logging
    if (enableLogging == "true"):
        # Archive log file
        if (archiveLogFiles == "true"):
            # If file exists
            if (os.path.isfile(logFile)):
                # If file is larger than 10MB
                if ((os.path.getsize(logFile) / 1048576) > 10):
                    # Archive file
                    shutil.move(logFile, os.path.basename(os.path.splitext(logFile)[0]) + "-" + time.strftime("%d%m%Y") + ".log")         
        # Setup logging
        logger, logMessage = setLogging(logFile)
    # Log start of process
    printMessage("Process started...","info")  
    mainFunction()





