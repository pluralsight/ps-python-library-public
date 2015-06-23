#!/usr/bin/env python
import sys
import time
import pyodbc
import json
import pyhs2
import subprocess

def hive_connect(database, username, password,server='localhost'):
	"""Build pyhs2 connection to hive from file

	    Args:
	        server: string, name or ip address for hive where hiveserver is located
	        database: string, database name
	        username: string, useranme for the database
	        password: string, password for the database

	    Returns:
	        pyhs2 connection object
	"""
	try:
		connection = pyhs2.connect(host=server.encode('utf-8'),
									port=10000,
									authMechanism="PLAIN",
									user=username.encode('utf-8'),
									password=password.encode('utf-8'),
									database=database.encode('utf-8'))
	except (ValueError) as e:
		print "Error creating database connection", e

	return connection

def full_load_sqoop(table , sqlserver, sqldb, sqlconfig, config,cred_file, hiveserver='localhost', database='default'):
	"""Gets table from sql server and sqoops it to hive

	Args:
	    table: table name where csv data will be written
	    sqlserver: sqlserver ip address
	    sqldb: sql server database
	    sqlconfig: config for sqlserver auth
	    server: sql server host name
	    config: which configuration name to pull hive username and password credentials
	    cred_file: location of db login config file
	    hiveserver: location of hive server
	    database: hive database to load table to

	Returns:
	    None
	"""
	with open(cred_file,'rb') as cred:
		db_info = json.loads(cred.read())
	username = db_info[config]['username']
	password = db_info[config]['password']
	sqlusername = db_info[sqlconfig]['username']
	sqlpw = db_info[sqlconfig]['password']
	connection = hive_connect(database, username, password, hiveserver)
	cursor = connection.cursor()
	truncate_and_load(cursor,table,database,table,sqldb,sqlserver,sqlusername,sqlpw)
	# cursor.execute('drop table '+database+'.'+table)
	# subprocess.call('sudo -u hdfs sqoop import --connect "jdbc:sqlserver://'+sqlserver+':1433;database='+
	# 	            sqldb+';username='+sqlusername+';password='+sqlpw+'" --table '+table+
	# 	            ' --as-parquetfile --hive-import -m 1 --hive-database '+database
	# 	            +' --compression-codec org.apache.hadoop.io.compress.SnappyCodec',shell=True)

def truncate_and_load(cursor, hivetable, hivedb, sqltable, sqldb, sqlserver, sqlusr, sqlpw):
	"""drops table and reloads it in hive

	Args:
	    cursor: pyhs2 cursor to execute hive commands
	    hivetable: table name to load in hive
	    hivedb: hive db to load table
	    sqltable: sql table to load
	    sqldb: sql database to load
	    sqlserver: ip address of sql server
	    sqlusr: sql username
	    sqlpw: sql password

	Returns:
	    None
	"""

	cursor.execute('drop table '+hivedb+'.'+hivetable)
	subprocess.call('sudo -u hdfs sqoop import --connect "jdbc:sqlserver://'+sqlserver+':1433;database='+
		            sqldb+';username='+sqlusr+';password='+sqlpw+'" --table '+sqltable+
		            ' --as-parquetfile --hive-import -m 1 --hive-database '+hivedb
		            +' --compression-codec org.apache.hadoop.io.compress.SnappyCodec',shell=True)

def full_database_sqoop(sqlserver, sqldb, sqlconfig, config,cred_file, hiveserver='localhost', database='default'):
	"""truncates and loads full database in sql server into hive

	Args:
	    sqlserver: sqlserver ip address
	    sqldb: sql server database
	    sqlconfig: config for sqlserver auth
	    server: sql server host name
	    config: which configuration name to pull hive username and password credentials
	    cred_file: location of db login config file
	    hiveserver: location of hive server
	    database: hive database to load table to

	Returns:
	    None
	"""
	with open(cred_file,'rb') as cred:
		db_info = json.loads(cred.read())
	username = db_info[config]['username']
	password = db_info[config]['password']
	sqlusername = db_info[sqlconfig]['username']
	sqlpw = db_info[sqlconfig]['password']
	sqlconnection = pyodbc.connect('DRIVER={FreeTDS};SERVER='+sqlserver.encode('utf-8')+';PORT=1433;DATABASE='+sqldb.encode('utf-8')+';UID='+sqlusername.encode('utf-8')+';PWD='+sqlpw.encode('utf-8'))
	connection = hive_connect(database, username, password, hiveserver)
	sqlcursor = sqlconnection.cursor()
	cursor = connection.cursor()
	sqlcursor.execute('select name from sys.tables')
	tablelist = sqlcursor.fetchall()
	for table in tablelist:
		truncate_and_load(cursor,table[0],database,table[0],sqldb,sqlserver,sqlusername,sqlpw)