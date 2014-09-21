#!/usr/bin/env python

import sys
import time

import logging
logging.basicConfig() #included to avoid message when oauth2client tries to write to log


def loop_csv_file(source_csv):
    """Loops through a comma separated file and returns a list of data.  If column names are on the
    first line of the file they will be the first row of the list

    Args: 
        source_csv: source path for the file

    Results:
        list, one row per list item
    """
    import csv
    file_data = []
    with open(sourcecsv,'rb') as csvfile:
        file_reader = csv.reader(csvfile, delimiter=',', quotechar='"')
        for row in file_reader:
            file_data.append(row)
    return file_data


def loop_delimited_file(source_file,delimiter=',',quotechar='"'):
    """Loops through a delimited file and returns a list of data.  If column names are on the
    first line of the file they will be the first row of the list

    Args: 
        source_file: source path for the file
        delimiter: character/string to use as file delimited (default delimiter is comma)
        quotechar: character used to qualify strings (default is '"')

    Results:
        list, one row per list item
    """
    import csv
    file_data = []
    with open(source_file,'rb') as csvfile:
        file_reader = csv.reader(csvfile, delimiter=delimiter, quotechar=quotechar)
        for row in file_reader:
            file_data.append(row)
    return file_data


def gzip_file_create(source_file,dest_file = None):
    """Create gzip file from existing file (https://docs.python.org/2/library/gzip.html).
    Note: this does not delete the source file.
    
    Args:
        source_file: source path for the file to zip
        dest_file: path for the zipped version of file (default is source_file + .gz)
    
    Returns:
        None
    """
    import gzip
    #destCSV = sourceCSV + '.gz'
    if dest_file is None:
        dest_file = source_file + '.gz'
    f_in = open(source_file, 'rb')
    f_out = gzip.open(dest_file,'wb')
    f_out.writelines(f_in)
    f_out.close()
    f_in.close()

def get_schema_file(schema_csv):
    """Pull in schema from a file and return list with column and type

    Args:
        schema_csv: two column comma delimited schema file formatted ColumnName,Type
    
    Returns:
        list, Schema list of column and type
    """
    schema_raw = loop_delimited_file(schema_csv)
    schema =[]
    for column,datatype in schema_raw:
        schema.append([column.strip(' '),datatype.strip(' ')])
    return schema