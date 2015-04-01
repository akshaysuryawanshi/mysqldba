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
    print master_logs
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
    return slave_hosts



get_slaves_connected()
#get_master_logs()
