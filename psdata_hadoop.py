#!/usr/bin/local/python
import sys
import time
import pyodbc
import json
import pyhs2
import subprocess
import MySQLdb
from impala.dbapi import connect
import pexpect

def run_impala_cmd(user, pw, cmd, hostname, timeout=120):
	"""run impala command through cli

	    Args:
	        user: impala username
	        pw: password
	        cmd: command you want run
	        hostname: datanode where impala instance resides

	    Returns:
	        nothing
	"""
	print "impala-shell -i '"+hostname+"' -q '"+cmd+"' -u '"+user+ "' -l"
	child = pexpect.spawn("impala-shell -i '"+hostname+"' -q '"+cmd+"' -u '"+user+ "' -l")
	child.logfile_read = sys.stdout
	child.expect('LDAP password for '+user+':')
	child.sendline(pw)
	child.expect(pexpect.EOF, timeout=timeout)

def impala_query_to_file(user, pw, cmd, hostname, filename, delimiter = '\t', timeout=120):
	"""run impala command through cli

	    Args:
	        user: impala username
	        pw: password
	        cmd: command you want run
	        hostname: datanode where impala instance resides

	    Returns:
	        nothing
	"""
	print "impala-shell -i '"+hostname+"' -q '"+cmd+"' -u '"+user+ "' -l"
	child = pexpect.spawn("impala-shell -i '"+hostname+"' -q '"+cmd+"' -u '"+user+ "' -l -B -o " + filename + " '--output_delimiter=" + delimiter + "'")
	child.logfile_read = sys.stdout
	child.expect('LDAP password for '+user+':')
	child.sendline(pw)
	child.expect(pexpect.EOF, timeout=timeout)

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

def full_mssql_table_sqoop(table , sqlserver, sqldb, sqlconfig, config,cred_file, hiveserver='localhost', database='default',sqlschema='dbo',view=False):
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
	datanode = db_info[config]['datanode']
	sqlusername = db_info[sqlconfig]['username']
	sqlpw = db_info[sqlconfig]['password']
	sqlconnection = pyodbc.connect('DRIVER={FreeTDS};SERVER='+sqlserver.encode('utf-8')+';PORT=1433;DATABASE='+sqldb.encode('utf-8')+';UID='+sqlusername.encode('utf-8')+';PWD='+sqlpw.encode('utf-8'))
	connection = hive_connect('default', username, password, hiveserver)
	cursor = connection.cursor()
	sqlcursor = sqlconnection.cursor()
	if view == True:
		cursor.execute('create database if not exists '+database)
		truncate_and_load(cursor,table,database,table,sqldb,sqlserver,sqlusername,sqlpw,sqlschema)
	else:
		sqlcursor.execute("SELECT t.name, COALESCE(CASE when MIN(k.COLUMN_NAME) <> max(k.COLUMN_NAME) THEN '0' ELSE MAX(k.COLUMN_NAME) END, '0') AS pk, COALESCE(CASE when MIN(SCHEMA_NAME(t.SCHEMA_ID)) <> max(SCHEMA_NAME(t.SCHEMA_ID)) THEN '0' ELSE MAX(SCHEMA_NAME(t.SCHEMA_ID)) END, 'dbo') AS sn FROM sys.tables t LEFT JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE k ON t.name = k.table_name where t.name = '"+table+"' GROUP BY t.name")
		table = sqlcursor.fetchall()
		cursor.execute('create database if not exists '+database)
		if table[0][1] == '0':
			truncate_and_load(cursor,table[0][0],database,table[0][0],sqldb,sqlserver,sqlusername,sqlpw,sqlschema)
		else:
			truncate_and_load_pk(cursor,table[0][0],database,table[0][0],sqldb,sqlserver,sqlusername,sqlpw,sqlschema)
	run_impala_cmd(username, password, 'invalidate metadata',datanode)
	# cursor.execute('drop table '+database+'.'+table)
	# subprocess.call('sudo -u hdfs sqoop import --connect "jdbc:sqlserver://'+sqlserver+':1433;database='+
	# 	            sqldb+';username='+sqlusername+';password='+sqlpw+'" --table '+table+
	# 	            ' --as-parquetfile --hive-import -m 1 --hive-database '+database
	# 	            +' --compression-codec org.apache.hadoop.io.compress.SnappyCodec',shell=True)

def truncate_and_load(cursor, hivetable, hivedb, sqltable, sqldb, sqlserver, sqlusr, sqlpw, sqlschema='dbo'):
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
	    sqlschema: schema of sql table

	Returns:
	    None
	"""

	cursor.execute('drop table '+hivedb+'.'+hivetable + ' purge')
	delete_hdfs_files(hivedb,hivetable)
	subprocess.call('sudo -u hdfs sqoop import --connect "jdbc:sqlserver://'+sqlserver+':1433;database='+
		            sqldb+';username='+sqlusr+';password='+sqlpw+'" --table '+sqltable+
		            ' --as-parquetfile --hive-import -m 1 --hive-database '+hivedb
		            +' --compression-codec org.apache.hadoop.io.compress.SnappyCodec -- --schema '+sqlschema+' --direct',shell=True)

def truncate_and_load(cursor, hivetable, hivedb, sqltable, sqldb, sqlserver, sqlusr, sqlpw, sqlschema='dbo'):
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
	    sqlschema: schema of sql table

	Returns:
	    None
	"""

	cursor.execute('drop table '+hivedb+'.'+hivetable + ' purge')
	delete_hdfs_files(hivedb,hivetable)
	subprocess.call('sudo -u hdfs sqoop import --connect "jdbc:sqlserver://'+sqlserver+':1433;database='+
		            sqldb+';username='+sqlusr+';password='+sqlpw+'" --table '+sqltable+
		            ' --as-parquetfile --hive-import -m 1 --hive-database '+hivedb
		            +' --compression-codec org.apache.hadoop.io.compress.SnappyCodec -- --schema '+sqlschema+' --direct',shell=True)


def mysql_truncate_and_load(cursor, hivetable, hivedb, mysqltable, mysqldb, mysqlserver, mysqlusr, mysqlpw):
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
	    sqlschema: schema of sql table

	Returns:
	    None
	"""

	cursor.execute('drop table '+hivedb+'.'+hivetable + ' purge')
	delete_hdfs_files(hivedb,hivetable)
	print 'sudo -u hdfs sqoop import --connect jdbc:mysql://'+mysqlserver+':3306/'+mysqldb+' --username '+mysqlusr+' --password "'+mysqlpw+'" --table '+mysqltable+' --as-parquetfile --hive-import -m 1 --hive-database '+hivedb+' --compression-codec org.apache.hadoop.io.compress.SnappyCodec'
	subprocess.call("sudo -u hdfs sqoop import --connect jdbc:mysql://"+mysqlserver+":3306/"+mysqldb+" --username "+mysqlusr+" --password '"+mysqlpw+"' --table "+mysqltable+
		            " --as-parquetfile --hive-import -m 1 --hive-database "+hivedb
		            +" --compression-codec org.apache.hadoop.io.compress.SnappyCodec",shell=True)

def mssql_incremental_load(hivetable, hivedb, sqltable, sqldb, icol, sqlserver, config, sqlconfig, impalahost='ip-172-16-100-230.us-west-2.compute.internal',sqlschema='dbo', cred_file='/home/ec2-user/configs/dblogin.config', pk=False):
	"""incrementally loads table from ms sql server into hive

	Args:
	    hivetable: table name to load in hive
	    hivedb: hive db to load table
	    sqltable: sql table to load
	    sqldb: sql database to load
	    icol: incremental column in sql table
	    sqlserver: ip address of sql server
	    config: hive config
	    sqlconfig: ms sql config
	    sqlschema: schema of sql table

	Returns:
	    None
	"""

	# mssql_incremental_load('usersubshistory','source','usersubshistory','source','sourcefiledate','172.16.100.38','datahub','satchmo',pk=True)
	# import pyodbc
	# import json
	# from psdata_hadoop import hive_connect
	# import subprocess
	# from impala.dbapi import connect
	# cred_file = '/home/ec2-user/configs/dblogin.config'
	# sqlschema = 'dbo'
	# config = 'datahub'
	# hivetable = 'usersubshistory'
	# hivedb = 'source'
	# sqltable = 'usersubshistory'
	# sqldb = 'source'
	# icol = 'sourcefiledate'
	# sqlserver = '172.16.100.38'
	# sqlconfig = 'satchmo'
	# hiveserver = 'localhost'
	# impalahost = 'ip-172-16-100-230.us-west-2.compute.internal'

	with open(cred_file,'rb') as cred:
		db_info = json.loads(cred.read())

	username = db_info[config]['username']
	password = db_info[config]['password']
	datanode = db_info[config]['datanode']
	sqlusername = db_info[sqlconfig]['username']
	sqlpw = db_info[sqlconfig]['password']
	sqlconnection = pyodbc.connect('DRIVER={FreeTDS};SERVER='+sqlserver.encode('utf-8')+';PORT=1433;DATABASE='+sqldb.encode('utf-8')+';UID='+sqlusername.encode('utf-8')+';PWD='+sqlpw.encode('utf-8'))
	impala_connection = pyodbc.connect('DRIVER={Cloudera ODBC Driver for Impala 64-bit};HOST='+datanode+';PORT=21050;UID='+username+';PWD='+password+';SSL=1;AuthMech=3;Database=default',autocommit=True)
	connection = hive_connect('default', username, password)
	sqlcursor = sqlconnection.cursor()
	cursor = connection.cursor()
	impala_cursor = impala_connection.cursor()
	impala_cursor.execute("Select max("+icol+") from "+hivedb+"."+sqltable)
	maxval = impala_cursor.fetchall()
	maxval = maxval[0][0]
	impala_cursor.execute('describe '+hivedb+'.'+sqltable)
	collist = impala_cursor.fetchall()
	external_str = "create external table if not exists "+hivedb+"."+sqltable+"_incremental("

	for a in collist:
		external_str = external_str + a[0] + ' ' + a[1] +', '

	id_datatype = ''

	for a in collist:
		if a[0] == icol:
			id_datatype = a[1]

	if id_datatype == 'bigint' and 'id' not in icol:
		impala_cursor = impala_connection.cursor()
		impala_cursor.execute('select date_add(cast(0 as timestamp), interval '+str(maxval)+' millisecond)')
		maxval = impala_cursor.fetchall()
		maxval = str(maxval[0][0])


	# external_str = external_str[:-2] + ") ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\\n' STORED AS TEXTFILE LOCATION '/etl/incremental/"+sqltable+"'"

	if id_datatype != 'int' and 'id' not in icol:
		impala_cursor.execute('drop table if exists '+hivedb+'.'+hivetable +'_incremental')
		try:
			subprocess.call("sudo -u hdfs hadoop fs -rm /user/hive/warehouse/"+hivedb+".db/"+sqltable+"_incremental/*",shell=True)
		except OSError:
			print 'folder empty'
		if pk == True:
			subprocess.call('sudo -u hdfs sqoop import --connect "jdbc:sqlserver://'+sqlserver+':1433;database='+sqldb+';username='+sqlusername+';password='+sqlpw+'" -m 16 --as-parquetfile --split-by '+icol+ ' --compression-codec org.apache.hadoop.io.compress.SnappyCodec --hive-import --hive-database '+hivedb+' --hive-table '+ sqltable +'_incremental --query \"select * from ['+sqldb+'].['+sqlschema+'].['+sqltable+'] where '+icol+' > \''+str(maxval)[:-3]+'\' and \$CONDITIONS\" --target-dir /etl/incremental/'+sqltable+' -- --schema '+sqlschema+' --direct', shell=True)
		else:
			subprocess.call('sudo -u hdfs sqoop import --connect "jdbc:sqlserver://'+sqlserver+':1433;database='+sqldb+';username='+sqlusername+';password='+sqlpw+'" -m 1 --as-parquetfile --split-by '+icol+ ' --compression-codec org.apache.hadoop.io.compress.SnappyCodec --hive-import --hive-database '+hivedb+' --hive-table '+ sqltable +'_incremental --query \"select * from ['+sqldb+'].['+sqlschema+'].['+sqltable+'] where '+icol+' > \''+str(maxval)[:-3]+'\' and \$CONDITIONS\" --target-dir /etl/incremental/'+sqltable+' -- --schema '+sqlschema+' --direct', shell=True)
	else:
		impala_cursor.execute('drop table if exists '+hivedb+'.'+hivetable +'_incremental')
		try:
			subprocess.call("sudo -u hdfs hadoop fs -rm /user/hive/warehouse/"+hivedb+".db/"+sqltable+"_incremental/*",shell=True)
		except OSError:
			print 'folder empty'
		if pk == True:
			subprocess.call('sudo -u hdfs sqoop import --connect "jdbc:sqlserver://'+sqlserver+':1433;database='+sqldb+';username='+sqlusername+';password='+sqlpw+'" -m 16 --as-parquetfile --split-by '+icol+ '  --compression-codec org.apache.hadoop.io.compress.SnappyCodec --hive-import --hive-database '+hivedb+' --hive-table '+ sqltable +'_incremental --query \"select * from ['+sqldb+'].['+sqlschema+'].['+sqltable+'] where '+icol+' > '+str(maxval)+' and \$CONDITIONS\" --target-dir /etl/incremental/'+sqltable+' -- --schema '+sqlschema+' --direct', shell=True)
		else:
			subprocess.call('sudo -u hdfs sqoop import --connect "jdbc:sqlserver://'+sqlserver+':1433;database='+sqldb+';username='+sqlusername+';password='+sqlpw+'" -m 1 --as-parquetfile --split-by '+icol+ '  --compression-codec org.apache.hadoop.io.compress.SnappyCodec --hive-import --hive-database '+hivedb+' --hive-table '+ sqltable +'_incremental --query \"select * from ['+sqldb+'].['+sqlschema+'].['+sqltable+'] where '+icol+' > '+str(maxval)+' and \$CONDITIONS\" --target-dir /etl/incremental/'+sqltable+' -- --schema '+sqlschema+' --direct', shell=True)
	run_impala_cmd(username, password, 'invalidate metadata',datanode)
	subprocess.call("sudo -u hdfs hive -e 'insert into table "+hivedb+"."+sqltable+ " select * from "+hivedb+"."+sqltable+"_incremental'",shell=True)
	try:
		subprocess.call("sudo -u hdfs hadoop fs -rm /user/hive/warehouse/"+hivedb+".db/"+sqltable+"_incremental/*",shell=True)
	except OSError:
		print 'folder empty'

	impala_cursor.execute('drop table if exists '+hivedb+'.'+hivetable +'_incremental')
	run_impala_cmd(username, password, 'invalidate metadata',datanode)

def mysql_incremental_load(cursor, hivetable, hivedb, mysqltable, mysqldb, mysqlserver, mysqlusr, mysqlpw):
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
	    sqlschema: schema of sql table

	Returns:
	    None
	"""

	cursor.execute('drop table '+hivedb+'.'+hivetable + ' purge')
	delete_hdfs_files(hivedb,hivetable)
	print 'sudo -u hdfs sqoop import --connect jdbc:mysql://'+mysqlserver+':3306/'+mysqldb+' --username '+mysqlusr+' --password "'+mysqlpw+'" --table '+mysqltable+' --as-parquetfile --hive-import -m 1 --hive-database '+hivedb+' --compression-codec org.apache.hadoop.io.compress.SnappyCodec'
	subprocess.call("sudo -u hdfs sqoop import --connect jdbc:mysql://"+mysqlserver+":3306/"+mysqldb+" --username "+mysqlusr+" --password '"+mysqlpw+"' --table "+mysqltable+
		            " --as-parquetfile --hive-import -m 1 --hive-database "+hivedb
		            +" --compression-codec org.apache.hadoop.io.compress.SnappyCodec",shell=True)

def truncate_and_load_pk(cursor, hivetable, hivedb, sqltable, sqldb, sqlserver, sqlusr, sqlpw,sqlschema='dbo'):
	"""drops table and reloads it in hive but using the primary key

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

	cursor.execute('drop table '+hivedb+'.'+hivetable+ ' purge')
	delete_hdfs_files(hivedb,hivetable)
	subprocess.call('sudo -u hdfs sqoop import --connect "jdbc:sqlserver://'+sqlserver+':1433;database='+
		            sqldb+';username='+sqlusr+';password='+sqlpw+'" --table '+sqltable+
		            ' --as-parquetfile --hive-import -m 16 --hive-database '+hivedb
		            +' --compression-codec org.apache.hadoop.io.compress.SnappyCodec -- --schema '+sqlschema+' --direct',shell=True)

def delete_hdfs_files(hivedb, t):
	"""deletes all files in hdfs folder

	Args:
		hivedb_dest: hive database to drop table
		t: table to drop 

	Returns:
	    None
	"""
	t = t.lower()
	subprocess.call('sudo -u hdfs hadoop fs -rm /user/hive/warehouse/'+hivedb+'.db/'+t+'/*',shell=True)

# def refresh_impala_metadata():
# 	"""Refreshes Impalas Metadata after loading

# 	Args:

# 	Returns:
# 	    None
# 	"""

# 	subprocess.call("impala-shell -i 172.16.100.230 -q 'invalidate metadata'",shell=True)


def full_database_sqoop(sqlserver, sqldb, sqlconfig, config,cred_file, hiveserver='localhost', database='default',sqlschema='dbo',omitlist=[]):
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
	datanode = db_info[config]['datanode']
	sqlusername = db_info[sqlconfig]['username']
	sqlpw = db_info[sqlconfig]['password']
	sqlconnection = pyodbc.connect('DRIVER={FreeTDS};SERVER='+sqlserver.encode('utf-8')+';PORT=1433;DATABASE='+sqldb.encode('utf-8')+';UID='+sqlusername.encode('utf-8')+';PWD='+sqlpw.encode('utf-8'))
	connection = hive_connect('default', username, password, hiveserver)
	sqlcursor = sqlconnection.cursor()
	cursor = connection.cursor()
	sqlcursor.execute("SELECT t.name, COALESCE(CASE when MIN(k.COLUMN_NAME) <> max(k.COLUMN_NAME) THEN '0' ELSE MAX(k.COLUMN_NAME) END, '0') AS pk, COALESCE(CASE when MIN(SCHEMA_NAME(t.SCHEMA_ID)) <> max(SCHEMA_NAME(t.SCHEMA_ID)) THEN '0' ELSE MAX(SCHEMA_NAME(t.SCHEMA_ID)) END, 'dbo') AS sn FROM sys.tables t LEFT JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE k ON t.name = k.table_name GROUP BY t.name")
	cursor.execute('create database if not exists '+database)
	tablelist = sqlcursor.fetchall()
	for table in tablelist:
		if table[0] in omitlist:
			continue
		elif table[1] == '0':
			truncate_and_load(cursor,table[0],database,table[0],sqldb,sqlserver,sqlusername,sqlpw,table[2])
		else:
			truncate_and_load_pk(cursor,table[0],database,table[0],sqldb,sqlserver,sqlusername,sqlpw,table[2])
	run_impala_cmd(username, password, 'invalidate metadata',datanode)

def full_mysql_db_sqoop(mysqlserver, mysqldb, mysqlconfig, config,cred_file, hiveserver='localhost', database='default'):
	"""truncates and loads full database in mysql into hive

	Args:
	    mysqlserver: mysqlserver ip address
	    mysqldb: mysql database
	    mysqlconfig: config for mysql auth
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
	datanode = db_info[config]['datanode']
	mysqlusername = db_info[mysqlconfig]['username']
	mysqlpw = db_info[mysqlconfig]['password']
	mysqlconnection = MySQLdb.connect(
    host=mysqlserver,
    port=3306,
    user=mysqlusername,
    passwd=mysqlpw,
    use_unicode=True,
    charset='utf8')
	connection = hive_connect('default', username, password, hiveserver)
	mysqlcursor = mysqlconnection.cursor()
	cursor = connection.cursor()
	mysqlcursor.execute("SHOW TABLES IN "+mysqldb)
	cursor.execute('create database if not exists '+database)
	tablelist = mysqlcursor.fetchall()
	for table in tablelist:
		mysql_truncate_and_load(cursor,table[0],database,table[0],mysqldb,mysqlserver,mysqlusername,mysqlpw)
	run_impala_cmd(username, password, 'invalidate metadata', datanode)



