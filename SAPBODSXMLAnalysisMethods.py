#!/usr/bin/env python
"""Developed For a CPG Company: Used to extract SAP Data Services Meta-data to analyze the
.xml files that are pushed from each job and help the company understand the jobs
and how data is being transferred around the company.  The functions in this script are called
in the primary python file (SAPBODSXMLAnalysisDriver.py)

def extractTableData() -> get the source and target table information for the different tables

def unWrapXMLTableStruct() -> unwrap the dataflow information from the xml file.

def get getFunctionsFromDataflow() -> analyzes the functions that are being called in each dataflow.
"""

import pandas as pd
pd.options.mode.chained_assignment = None

__author__ = "Dylan Smith"
__copyright__ = "Copyright (C) 2018 Dylan Smith"
__credits__ = ["Dylan Smith"]

__license__ = "Dylan Smith"
__version__ = "1.0"
__maintainer__ = "Dylan Smith"
__email__ = "-"
__status__ = "Development"

def extractTableData(doc,job):
    dfSrcFinal = pd.DataFrame(columns = ['JobName', 'Dataflow', 'SRC_TBLS'])
    dfTgtFinal = pd.DataFrame(columns = ['JobName', 'Dataflow', 'TGT_TBLS'])
    #get the source tables in the ABAP Dataflows
    try:
        for dataflow in  doc['DataIntegratorExport']['DIR3Dataflow']:
            dfTemp = pd.DataFrame()
            dfTemp['JobName'] = [job]
            dfTemp['Dataflow'] =  [dataflow['@name']]
            dfTgtTemp = dfTemp[:]
            dfSrcTemp = dfTemp[:]
            try:
                for tables in dataflow['DITransforms']['DIDatabaseTableSource']:
                    if tables['@tableName'] != 'ZDS_SEED':
                        dfSrcTemp['SRC_TBLS'] = str(tables['@datastoreName'] + '.' + tables['@tableName'])
                        dfSrcFinal = pd.concat([dfSrcFinal, dfSrcTemp], sort = False)
            except:
                pass
    except:
        pass

    #this try catch is used for all normal dataflows (not ABAP dataflows)
    try:
        for dataflow in  doc['DataIntegratorExport']['DIDataflow']:
            dfTemp = pd.DataFrame()
            dfTemp['JobName'] = [job]
            dfTemp['Dataflow'] =  [dataflow['@name']]
            dfTgtTemp = dfTemp[:]
            dfSrcTemp = dfTemp[:]
            #get the structure for all of the source tables.
            try:
                srcTables =  unWrapXMLTableStruct('DIDatabaseTableSource', dataflow['DITransforms'])
                for tbl in srcTables:
                    dfSrcTemp['SRC_TBLS'] = str(tbl)
                    dfSrcFinal = pd.concat([dfSrcFinal, dfSrcTemp], sort = False)
            except:
                pass

            #get the target tables per dataflow
            try:
                tgtTables = unWrapXMLTableStruct('DIDatabaseTableTarget', dataflow['DITransforms'])
                for tbl in tgtTables:
                    dfTgtTemp['TGT_TBLS'] = str(tbl)
                    dfTgtFinal = pd.concat([dfTgtFinal, dfTgtTemp], sort = False)
            except:
                pass
    except:
        pass
    return (dfTgtFinal, dfSrcFinal)

def unWrapXMLTableStruct(tableType, tableDict):
    try:
        return  set([tableDict[tableType]['@datastoreName'] + '.' + tableDict[tableType]['@ownerName'] + '.' + tableDict[tableType]['@tableName']])
    except:
        tablesUsed = set()
        for table in tableDict[tableType]:
            tablesUsed.update([table['@datastoreName'] + '.' + table['@ownerName'] + '.' + table['@tableName']])

        return tablesUsed

def getFunctionsFromDataflow(dataflow):
    dataflowFunctionsUsed = set()
    for query in dataflow['DITransforms']['DIQuery']:
        for functions in query['DISelect']['DIProjection']['DIExpression']:
            #try function is for queries that have function calls
            try:
                for functionCall in functions['FUNCTION_CALL']:
                    if functionCall == 'FUNCTION_CALL':
                        bodsFunc = functions['FUNCTION_CALL']['FUNCTION_CALL']
                        dataflowFunctionsUsed.update([bodsFunc['@name'] + ' as ' + bodsFunc['@tableDatastore'] + '.' + bodsFunc['@tableOwner'] + '.' + bodsFunc['@tableName']] )
                    if functions['FUNCTION_CALL']['@type'] == 'StoredProcedure':
                        bodsFunc = functions['FUNCTION_CALL']
                        dataflowFunctionsUsed.update(['StoredProcedure'+ ' as ' + bodsFunc['@datastore']+ '.' + bodsFunc['@owner']+ '.'+bodsFunc['@name']] )
            except:
                pass
    return dataflowFunctionsUsed

#functions
def getTablesAndFieldInfoFromJob(doc, jobName):
    dfFinal = pd.DataFrame(columns = ['Job','Database', 'Schema', 'Table Name', 'Field Name', 'Description'])
    try:
        for table in doc['DataIntegratorExport']['DITable']:
            if table['@owner'] == '' :
                continue
            dfTemp =  pd.DataFrame()
            dfTemp['Job'] = [jobName]
            dfTemp['Database'], dfTemp['Schema'], dfTemp['Table Name'] =  [table['@database'] , table['@owner'] , table['@name']]
            for field in table['DIColumn']:
                try:
                    dfTemp['Field Name'] = [field['@name']]
                except:
                    dfTemp['Field Name'] = ''

                try:
                    dfTemp['Description'] = [field['@description'].replace(',','')]
                except:
                    dfTemp['Description'] = 'NULL'
                dfFinal = pd.concat([dfFinal,dfTemp])
    except:
        pass
    return dfFinal

def getDataStoreInformation(bodsXML,fileName):
    #dfFinal = pd.DataFrame(columns = ('Job Name', 'Datastore Used', 'Database Type', 'System Configuration', 'user'))
    dfTemp = pd.DataFrame(columns = ('Job Name', 'Datastore Used', 'Database Type', 'System Configuration', 'user'))
    for datastore in  bodsXML['DataIntegratorExport']['DIDatabaseDatastore']:
        #if multiple configurations vs single configurations
        try:
            datastoreSystem = datastore['DIAttributes']['DIAttribute'][6]['DSConfigurations']['DSConfiguration']
            #try sql server vs sap system
            try:
                version = datastoreSystem['sql_server_version']
            except:
                try:
                    version = datastoreSystem['sap_host_name']
                except:
                    version = ''

            dfTemp = dfTemp.append({'Job Name': fileName, 'Datastore Used': datastore['@name'],'Database Type': version, 'System Configuration': datastoreSystem['@name'], 'user':datastoreSystem['user'] }, ignore_index = True)
        except:
            for key in datastore['DIAttributes']['DIAttribute'][6]['DSConfigurations']['DSConfiguration']:
                #try sql server vs sap system
                try:
                    version = key['sql_server_version']
                except:
                    try:
                        version = key['sap_host_name']
                    except:
                        version = ''
                dfTemp = dfTemp.append({'Job Name': fileName, 'Datastore Used': datastore['@name'],'Database Type':version, 'System Configuration': key['@name'], 'user':key['user'] }, ignore_index = True)

    return dfTemp
