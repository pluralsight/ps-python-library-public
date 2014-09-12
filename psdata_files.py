#!/usr/bin/env python

import sys
import time

import logging
logging.basicConfig() #included to avoid message when oauth2client tries to write to log


def loop_csv_file(source_csv):
    import csv
    file_data = []
    with open(sourcecsv,'rb') as csvfile:
        file_reader = csv.reader(csvfile, delimiter=',', quotechar='"')
        for row in file_reader:
            file_data.append(row)
    return file_data


def loop_delimited_file(source_file,delimiter=',',quotechar='"'):
    import csv
    file_data = []
    with open(source_file,'rb') as csvfile:
        file_reader = csv.reader(csvfile, delimiter=delimiter, quotechar=quotechar)
        for row in file_reader:
            file_data.append(row)
    return file_data


def gzip_file_create(source_file,dest_file = None):
    #Create gzip file from existing file (https://docs.python.org/2/library/gzip.html)
    import gzip
    #destCSV = sourceCSV + '.gz'
    if dest_file is None:
        dest_file = source_file + '.gz'
    f_in = open(source_file, 'rb')
    f_out = gzip.open(dest_file,'wb')
    f_out.writelines(f_in)
    f_out.close()
    f_in.close()

