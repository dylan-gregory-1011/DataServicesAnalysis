#!/usr/bin/env python
"""Developed For a CPG Client: This script is the driving python script that
triggers the analysis of the BODS xml files and allows for the files to be analyzed

def getTablesAndFieldInfoFromJob(xmlData, jobName) -> Takes the xml data for each job and returns a
    data structure that has all of the table, field, and field description info for each jobself.

def getDataStoreInformation(xml, fileInfo) -> gets information from each of the datastores that are used
    in the jobs
"""

import pandas as pd
import xmltodict
from os import listdir
from os.path import isfile, join, splitext, dirname, abspath
import codecs
from KLGBODSSrcTgtAnalysis import extractTableData, getDataStoreInformation, getTablesAndFieldInfoFromJob

__author__ = "Dylan Smith"
__copyright__ = "Copyright (C) 2018 Dylan Smith"
__credits__ = ["Dylan Smith"]

__license__ = "CPG"
__version__ = "1.0"
__maintainer__ = "Dylan Smith"
__email__ = "-"
__status__ = "Development"

if __name__ == "__main__":
    # Get a list of all the jobs that will be analyzed and set constants
    currFileFolder = dirname(abspath(__file__))
    inpFileFolder = currFileFolder + '\\DataServicesXMLFiles\\'
    outFileFolder = currFileFolder + '\\AnalyzedFiles\\'
    allXMLFiles = [f for f in listdir(inpFileFolder) if isfile(join(inpFileFolder, f)) and (splitext(f))[1] == '.xml']

    #iterate through the jobs and get the data
    for ix, job in enumerate(allXMLFiles):
        print(job.split('.')[0])
        #pull down the xml data for each job
        with codecs.open(inpFileFolder + job, encoding = 'utf-8') as fd:
            document = xmltodict.parse(fd.read())
        #get the table and field info from each job
        dfFinal = getTablesAndFieldInfoFromJob(document, job.split('.')[0])
        #get the table information from each job
        dfTgtFinal, dfSrcFinal = extractTableData(document,job.split('.')[0])

        if ix == 0:
            write_type = 'w+'
        else:
            write_type = 'a'
        #write the data to each of the files
        with open(outFileFolder + 'BottomLevelTableInfo.csv', write_type) as f:
            dfFinal.to_csv(f, header=False)
        with open(outFileFolder + 'DataflowSrcTableInfo.csv', write_type) as f:
            dfSrcFinal.to_csv(f, header=False)
        with open(outFileFolder + 'DataflowTgtTableInfo.csv', write_type) as f:
            dfTgtFinal.to_csv(f, header=False)
