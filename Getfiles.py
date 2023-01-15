#this gets a list of all the filenames from the data directory
    #needs to be able to iterate through directories within data directories
        #needs to go through multiple zipped files -- use os.walk()
    #needs to unzip files
    #
#then calls the xml parser iteratively, changing the file name everytime
from zipfile38 import ZipFile
import os
import pandas as pd
import xml.etree.ElementTree as ET
import csv
from bs4 import BeautifulSoup
import lxml
import xmlparser
import time
import SnowflakeUpload
import MAParser

directory = r'Insert Path here'


#gets a list of zip file names 
def get_file_names(directory):

    #1. Navigate to the directory passed in function call
    #2. Loop through the directory to add all file names with a given suffix to a list of names
    #3. Return list of zip file names
    os.chdir(directory)
    suffix = ".zip"
    names = []
    for folder, dirs, files in os.walk(directory, topdown=False):
        for name in files:
            if name.endswith(suffix):
                names.append(name)
    return names

#Extracts files from zip files and places them in a new folder with the same name
def extractor(names):
        #1.Iterate through the list of names of zip files
        #2.Creates a new folder to hold the extracted files
        #3 Closes and removes zip file
        #4.Records new paths of unzipped folders in a list
    size = len(names)
    unzipfolder = ''
    unzipfolderls = []
    for i in range(0, size):
        zipfolder = names[i]
        myzip = ZipFile(zipfolder, "r")
        unzipfolder = zipfolder[:-4]
        myzip.extractall(path=unzipfolder)
        myzip.close()
    return None

#calls xml parser iteratively by walking through the directory and passing folder by folder
def call_parser():
    directory = r'Insert Path here'

    for root,dirs,files in os.walk(directory,topdown= False):
        for name in dirs:
            filepath = os.path.join(directory,name)
            print(filepath)
            output1 = filepath[:-4]+'geninfo'
            output2 = filepath[:-4]+'genofficerinfo'
            print("Calling Parser")
            xmlparser.iteratebasicfpls(filepath,output1,output2)

    return None


if __name__ == '__main__':
    t1 = time.time()
    #zipfiles = get_file_names(directory)
    #unzipfps = extractor(zipfiles)
    #call_parser()
    #MAParser.parsefiles()
    SnowflakeUpload.add_timestamp()
    conn = SnowflakeUpload.snowflakeconn()
    SnowflakeUpload.uploadsingle(conn)


