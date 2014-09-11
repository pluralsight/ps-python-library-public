#!/usr/bin/env python

import sys
import time

import logging
logging.basicConfig() #included to avoid message when oauth2client tries to write to log


def loop_csv_file(sourceCSV):
    import csv
    fileData = []
    with open(sourceCSV,'rb') as csvfile:
        fileReader = csv.reader(csvfile, delimiter=',', quotechar='"')
        for row in fileReader:
            fileData.append(row)
    return fileData


def loop_delimited_file(sourceFile,delimiter=',',quotechar='"'):
    import csv
    fileData = []
    with open(sourceFile,'rb') as csvfile:
        fileReader = csv.reader(csvfile, delimiter=delimiter, quotechar=quotechar)
        for row in fileReader:
            fileData.append(row)
    return fileData


def gzip_file_create(sourceFile,destFile = None):
    #Create gzip file from existing file (https://docs.python.org/2/library/gzip.html)
    import gzip
    #destCSV = sourceCSV + '.gz'
    if destFile is None:
        destFile = sourceFile + '.gz'
    f_in = open(sourceFile, 'rb')
    f_out = gzip.open(destFile,'wb')
    f_out.writelines(f_in)
    f_out.close()
    f_in.close()

