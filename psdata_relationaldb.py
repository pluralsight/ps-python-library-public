#!/usr/bin/env python

import sys
import time
import pyodbc
import json
from psdata_toolbox import process_data_row
from psdata_toolbox import _defaultencode


def insert_list_to_sql(connection,lst,tableName):
    """Inserts from a list to a SQL table.  List must have the same format and item order as the table columns.
        Args:
            list: list, Values to insert to table
            tableName: string, Fully qualified SQL table name

        Returns:
            None
    """ 
    sorted_column_values_list = []
    for items in lst:
        sorted_column_values_list.append(items)

    for val in sorted_column_values_list:
        valstring = '('
        for colval in val:
            try:
                valstring += "'" + colval + "',"
            except TypeError:
                valstring += str(colval) +','
        valstring = valstring[0:-1] + ')' #remove trailing comma
        query = "INSERT INTO {0} VALUES {1}".format(tableName, valstring)

        c = run_sql(connection,query)
        #print type(c)
    return


def run_sql(connection,query): #courseTagDict
    """Runs SQL statement and commits changes to database.
        
        Args:
            connection: pyodbc.connect() object, Connection to use when running Sql 
            query: string, Valid query string

        Returns:
            cursor object, Results of the call to pyodb.connection().cursor().execute(query)
    """ 
    cursor=connection.cursor()
    cursor.execute(query)
    connection.commit()

    return cursor


def sql_get_table_data(connection, table, schema='dbo', include_extract_date = True): #courseTagDict
    """Runs SQL statement to get all records from the table (select *)
        
        Args:
            connection: pyodbc.connect() object, Connection to use when selecting data 
            table: string, Valid table

        Returns:
            cursor object, Results of the call to pyodb.connection().cursor().execute(query)
    """ 
    extract_date = ""
    if include_extract_date:
        extract_date = ", getdate() as ExtractDate"
    query = 'select * ' + extract_date + ' from ' + schema + '.' + table + ' with (nolock)'
    cursor=connection.cursor()
    cursor.execute(query)

    return cursor


def truncate_sql_table(connection,table_name):
    """Runs truncate table SQL command and commits changes to database.
        
        Args:
            connection: pyodbc.connect() object, Connection to use for truncate
            tableName: string, Fully qualified SQL table name (make sure this is the table you want to clear!)

        Returns:
            None
    """ 
    sql = "truncate table " + table_name
    cursor=connection.cursor()
    cursor.execute(sql)
    connection.commit()

    return


def sql_get_schema(connection,query,include_extract_date = True):
    import json

    cursor = connection.cursor()
    cursor.execute(query)
    
    schema_list = []
    #colList = []
    #typeList = []
    for i in cursor.description:
        schema_list.append(i[0:2])
        #colList.append(i[0])
        #typeList.append(str(i[1]))
    if include_extract_date:
        schema_list.append(['ExtractDate','datetime'])
    return schema_list


def cursor_to_json(cursor, dest_file):
    schema = []
    for i in cursor.description:
        schema.append([i[0],str(i[1])])

    with open(dest_file,'wb') as outfile:
        for row in cursor:
            result_dct = process_data_row(row,schema)
            outfile.write("%s\n" % json.dumps(result_dct, default=_defaultencode))
