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
    cursor.execute(query.encode('utf-8'))
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
    query = 'select * ' + extract_date + ' from ' + schema + '.[' + table + '] with (nolock)'
    print query
    cursor=connection.cursor()
    cursor.execute(query.encode('utf-8'))

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
    cursor.execute(sql.encode('utf-8'))
    connection.commit()

    return


def sql_get_schema(connection,query,include_extract_date = True):
    """Reads schema from database by running the provided query.  It's recommended to
    pass a query that is limited to 1 record to minimize the amount of rows accessed on 
    the server.

        Args:
            connection: pyodbc.connect() object, Connection to use when running Sql 
            query: string, Valid query string
            include_extract_date: boolean, defaults to True to add current timestamp field 
                    'ExtractDate' to results

        Returns:
            list, each list item contains field name and data type
    """
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


def cursor_to_json(cursor, dest_file, dest_schema_file=None):
    """Takes a cursor and creates JSON file with the data 
    and a schema file for loading to other data systems.

    Args:
        cursor: cursor object with data to extract to file
        dest_file: string, path and file name to save data

    Returns:
        None
    """
    schema = []
    for i in cursor.description:
        schema.append([i[0],str(i[1])])
    
    with open(dest_schema_file,'wb') as schemafile:
        for row in schema:
            col = row[0]
            if 'date' in row[1]:
                datatype = 'timestamp'
            elif 'list' in row[1]:
                datatype = 'list'
            elif 'int' in row[1]:
                datatype = 'integer'
            elif 'float' in row[1]:
                datatype = 'float'
            elif 'bool' in row[1]:
                datatype = 'boolean'
            elif 'str' in row[1]:
                datatype = 'string'
            else:
                datatype = 'string'
            schemafile.write("%s\n" % (col + ',' + datatype))

    with open(dest_file,'wb') as outfile:
        for row in cursor:
            result_dct = process_data_row(row,schema)
            outfile.write("%s\n" % json.dumps(result_dct, default=_defaultencode))