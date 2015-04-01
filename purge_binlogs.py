#!/usr/bin/python

# This is a script to check the current log file slave is processing and purge the binary logs accordingly.
# The script relies on the SHOW SLAVE STATUS output, hence the user credentials should have adequate privileges.

import MySQLdb as mysql

def master_connection():
    conn = mysql.connect(host="localhost",db="test",read_default_file="~/.my.cnf")
    return conn

def get_master_logs():
    master_conn = master_connection()
    cursor = master_conn.cursor()
    cursor.execute("SHOW MASTER LOGS")
    master_logs = []
    result_set = cursor.fetchall()
    for row in result_set:
        master_logs.append(row[0])
    #print master_logs
    cursor.close()
    return master_logs

def get_slaves_connected():
    master_conn = master_connection()
    cursor = master_conn.cursor(mysql.cursors.DictCursor)
    cursor.execute("SHOW PROCESSLIST")
    result_set = cursor.fetchall()
    slave_hosts = []
    for row in result_set:
        if row["Command"].lower() == "binlog dump":
            slave_hosts.append(row["Host"].split(":")[0])
    cursor.close()
    return slave_hosts

def get_slave_master_logs():
    slave_master_logs = []
    for hosts in get_slaves_connected():
        slave_conn = mysql.connect(host=hosts,read_default_file="~/.my.cnf")
        cursor = slave_conn.cursor(mysql.cursors.DictCursor)
        cursor.execute("SHOW SLAVE STATUS")
        slave_status = cursor.fetchone()
        slave_master_log = slave_status["Master_Log_File"]
        slave_master_logs.append(slave_master_log)
        cursor.close()
    slave_master_logs.sort()
    return slave_master_logs

def purge_master_logs():
    master_log_file = get_slave_master_logs()
    query = "PURGE BINARY LOGS TO '%s'" % master_log_file[0]
    master_conn = master_connection()
    cursor = master_conn.cursor()
    cursor.execute(query)
    print "Purged binary logs on master to %s" % master_log_file
    cursor.close()

purge_master_logs()
#get_slave_master_logs()
#get_slaves_connected()
#get_master_logs()
