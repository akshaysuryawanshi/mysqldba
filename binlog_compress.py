#!/usr/bin/env python

# Copyright 2012-2014 by Percona LLC or its affiliates, all rights reserved.
''' This is a script to compress master's binary log, it checks the seconds_behind_master and
uses config file at /etc/rdba/binlog_compress.yml'''

import MySQLdb as mysql
import yaml
import glob
import sys
import os
import time
import subprocess
import logging
import shutil
from warnings import filterwarnings

sys.path.append('/usr/local/rdba/lib/')
import lockfile_lib
now = time.time()

# Logging method.
LOGGING_DIR = '/var/log/percona/'
logging.basicConfig(filename=os.path.join(LOGGING_DIR, 'binlog_compress.log'),
                    level=logging.INFO,
                    format='%(asctime)s PID<%(process)d> %(levelname)s::%(message)s')
logger = logging.getLogger()


# Method to create a mysql connection.
def get_mysql_conn(host, port_no):
    conn = mysql.connect(host = host, port = port_no, read_default_group = 'client', read_default_file = '~/.my.cnf')
    filterwarnings('ignore', category = mysql.Warning)
    return conn


# Method to get the slaves connected to the server. At this moment the only discovery is available through processlist.
def get_slaves_connected():
    hostname = cfg.get('MASTER_HOST', 'localhost')
    port = 3306
    slave_hosts = []
    if cfg['SLAVES'] == "DISCOVER":
        cursor = get_mysql_conn(hostname, port).cursor(mysql.cursors.DictCursor)
        logger.info('Connected to master host')
        cursor.execute("SHOW PROCESSLIST")
        result_set = cursor.fetchall()
        slave_hosts = [(row["Host"].split(":")[0]) for row in result_set if row["Command"].strip().lower() in ("binlog dump, table dump")]
        cursor.close()
        logger.info('Retrieved Slaves: %s' % slave_hosts)
    elif cfg['SLAVES'] != "NONE":
        for slaves in cfg['SLAVES'].split(","):
            slave_hosts.append(slaves)
        logger.info('Checking specified slaves %s' % slave_hosts)
    return slave_hosts


# We retrieve the sbm over here.
def get_seconds_behind_master():
    port = 3306
    slave_lags = []
    max_slave_lag = 0
    slaves = get_slaves_connected()
    if len(slaves) > 0:
        for _slave in slaves:
            try:
                cursor = get_mysql_conn(_slave, port).cursor(mysql.cursors.DictCursor)
                cursor.execute("SHOW SLAVE STATUS")
                result_set = cursor.fetchone()
                cursor.close()
            except mysql.Error, e:
                logger.warning('An error has occurred: %s' % e)
                break
            if result_set["Seconds_Behind_Master"] is None:
                logging.warning('Slave %s is not connected to master' % _slave)
            else:
                slave_lags.append(result_set["Seconds_Behind_Master"])
                logger.info('Found slave delay: %d for server: %s' % (result_set["Seconds_Behind_Master"],_slave))
                max_slave_lag = max(slave_lags)
    else:
        logger.info("No Slaves connected.")
        max_slave_lag = 0
    return max_slave_lag


# Actual method to compress binlogs. Determines to time to compress either from config or based on slave lag.
def compress_binlogs():
    minutes_to_compress = cfg.get('MINUTES_TO_COMPRESS', 120)
    if cfg['SLAVES'] == "NONE":
        max_slave_lag = 0
    else:
        max_slave_lag = get_seconds_behind_master()
    if minutes_to_compress > max_slave_lag/60:
        compress_time = minutes_to_compress
    else:
        compress_time = (max_slave_lag/60)+30
    total_binlogs = []
    binlogs = []
    if cfg.get('BINLOG_DIR', '/var/lib/mysql'):
        binlog_search_path = cfg.get('BINLOG_DIR', '/var/lib/mysql') + '/*.[0-9][0-9][0-9][0-9][0-9][0-9]'
        for filename in glob.glob(binlog_search_path):
            total_binlogs.append(filename)
            if os.stat(filename).st_mtime < now - float(compress_time*60):
                binlogs.append(filename)
    else:
        logger.error('Please specify a valid BINLOG_DIR')
    if len(binlogs) > 0 and max(binlogs) == max(total_binlogs):
        binlogs.remove(max(binlogs))
    else:
        logger.info("No binlog to compress.")
    for files_to_gzip in sorted(binlogs):
        subprocess.call(['gzip', files_to_gzip])
        logger.info("Binlog compressed : %s" % files_to_gzip)


def remove_compressed_binlogs(location):
    minutes_to_remove = cfg.get('MINUTES_TO_REMOVE', 10080)
    binlog_location = location
    try:
        compressed_file_search_path = binlog_location + '/*.[0-9][0-9][0-9][0-9][0-9][0-9].gz'
        purge_log = []
        if len(glob.glob(compressed_file_search_path)) > 0:
            for _file in glob.glob(compressed_file_search_path):
                if os.stat(_file).st_mtime < now - float(minutes_to_remove*60):
                    os.unlink(_file)
                    logger.info("Compressed binlog purged : %s" % _file)
                    purge_log.append(os.path.splitext(_file.split("/")[-1])[0])
        else:
            logger.info("Old compressed binlog already cleaned up.")
            print purge_log
        if len(purge_log) > 0 and cfg.get('BINLOG_PURGE') == 1:
            logger.info("Executing PURGE BINARY LOGS to '%s'" % max(purge_log))
            cursor = get_mysql_conn(cfg.get('MASTER_HOST', 'localhost'),
                                    cfg.get('MASTER_PORT', 3306)).cursor(mysql.cursors.DictCursor)
            sql = "PURGE BINARY LOGS TO '%s'" % max(purge_log)
            print sql
            cursor.execute(sql)
            cursor.close()
    except OSError:
        logger.exception('Problem encountered while deleting files. Please check file permissions.')


# Optional method, incase the user doesnt want to purge the binlogs but wants to move them.
def move_binlogs(location, binlog_dir):
    minutes_to_remove = cfg.get('MINUTES_TO_REMOVE', 10080)
    try:
        compressed_file_search_path = binlog_dir + '/*.[0-9][0-9][0-9][0-9][0-9][0-9].gz'
        print compressed_file_search_path
        if len(glob.glob(compressed_file_search_path)) > 0:
            for _file in glob.glob(compressed_file_search_path):
                if os.stat(_file).st_mtime < now - float(minutes_to_remove*60):
                    shutil.move(_file, location)
            logger.info('Moved compressed binlogs as specified.')
    except OSError:
        logger.exception('Move location doesnt exist.')


def main():
    global cfg
    cfg_path = "/etc/rdba/binlog_compress.yml"
    try:
        with open(cfg_path, 'r') as cfg_file:
            cfg = yaml.safe_load(cfg_file)
    except Exception:
        logger.exception("Could not read config file %r", cfg_path)

    # Here we are trying to acquire a lock
    lock_file = lockfile_lib.LockFile('/tmp/binlog_compress.lock')
    try:
        lock_file.get_lock()
    except lockfile_lib.LockInUse:
        logger.debug('already running, unable to obtain lock on %r',
                     '/tmp/binlog_compress.lock')

    # Here we execute the compression and compressed binlog remove method.
    logger.info('Starting compression script')
    compress_binlogs()
    logger.info('Starting removal of old compressed binlog')
    if cfg.get('BINLOG_MOVE'):
        move_binlogs(cfg.get('BINLOG_MOVE'), cfg.get('BINLOG_DIR'))
    else:
        remove_compressed_binlogs(cfg.get('BINLOG_DIR'))

    lock_file.free_lock()
    return 0

if __name__ == "__main__":
    main()
