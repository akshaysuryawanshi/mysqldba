#!/usr/bin/env python

# This is wrapper for pt-online-schema-change which does all other things too.

import MySQLdb as mysql
import argparse
import logging
import sys
import subprocess
import time
import os
from warnings import filterwarnings

sys.path.append('/usr/local/rdba/lib')
import ftwrl_guardian_lib

""" Logging method. Uses the alter_parser to generate the log file name as per the table name. So multiple instances of
the tool can be run for different tables and maintain its own log file."""
def logging_setup():
    LOGGING_DIR = '/var/log/percona/'
    (schema, table, statement) = alter_parser()
    log_file_name = 'rdba_osc_' + schema + '_' + table + '.log'
    logging.basicConfig(filename=os.path.join(LOGGING_DIR, log_file_name),
                        level=logging.INFO,
                        format='%(asctime)s PID<%(process)d> %(levelname)s::%(message)s')


""" Option parser as expected."""
def parse_options():
    parser = argparse.ArgumentParser()
    parser.add_argument("-a", "--alter", dest="alter_statement",
                        help="Specify the complete ALTER statement to be executed")
    parser.add_argument("-o", "--type", dest="type", choices=['online', 'direct'], default="online",
                        help="Specify the type of ALTER statement to execute. ONLINE or STRAIGHT")
    parser.add_argument("-H", "--host", dest="host", default="localhost",
                        help="Specify the HOST to execute ALTER on.")
    parser.add_argument("-P", "--port", dest="port", default=3306,
                        help="MySQL port to connect")
    parser.add_argument("-S", "--socket", dest="socket",
                        help="Socket for connecting to MySQL instance")
    parser.add_argument("-n", "--skip-binlog", dest="skip_binlog", action="store_true", default=False,
                        help="Disable binary logging")
    parser.add_argument("-b", "--for-table-sync", dest="set_rbr", action="store_true", default=False,
                        help="Specify to set BINLOG FORMAT to ROW-BASED REPLICATION for using it to sync table between Master-Slave")
    parser.add_argument("-r", "--slack-room", dest="slack_room",
                        help="Specify the Slack chat room to post status.")
    parser.add_argument("-l", "--load", dest="load",choices=['high', 'medium', 'low'], default="high",
                        help="Specify the value for checking server load to pause and abort osc run")
    parser.add_argument("-p", "--print-only", dest="print_only", action="store_true",
                        help="Print only the pt-online-schema-change statement without executing.")
    parser.add_argument("-f", "--do-not-check-fk", dest="do_not_check_fk", action="store_true",
                        help="Specify if foreign keys should not searched at all. For large Information_Schema clients.")
    parser.add_argument("-g", "--ftwrl-guard", dest="ftwrl_guard", action="store_true",
                        help="Specify to enable ftwrl_guardian.")
    parser.add_argument("-e", "--extra-args", dest="extra_args",
                        help="Specify any extra arguments accepted by pt-online-schema-change tool.")

    if len(sys.argv) == 1:
        parser.print_help()
        sys.exit(1)
    return parser.parse_args()


""" Method to get a mysql connection."""
def get_mysql_conn(host, port_no):
    conn = mysql.connect(host=host, port=port_no, read_default_group='client', read_default_file='~/.my.cnf')
    filterwarnings('ignore', category=mysql.Warning)
    return conn


""" Method to check the existence of percona toolkit and discovering the pt-online-schema-change binary."""
def check_pt_toolkit():
    rc = subprocess.Popen(["which", "pt-online-schema-change"], stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                          stderr=subprocess.PIPE)
    osc_bin = rc.stdout.read().rstrip()
    rc.communicate()[0]
    if rc.returncode == 0:
        logging.info('pt-online-schema-change exists.')
        return osc_bin
    else:
        print "pt-online-schema-change not found. Please install percona-toolkit package."


""" Method to parse the statement received as an option to the tool."""
def alter_parser():
    try:
        fqn = options.alter_statement.split()[2]
        table = fqn.split(".")[1].replace('`', '')
        schema = fqn.split(".")[0].replace('`', '')
        statement = ' '.join(options.alter_statement.split(" ")[3:])
    except Nonetype:
        print parse_options()
    return schema, table, statement


""" Method to get the CREATE TABLE statement for before/after image in the logs."""
def show_create(db, tbl):
    try:
        cursor = get_mysql_conn(options.host, options.port).cursor(mysql.cursors.DictCursor)
        sql = "SHOW CREATE TABLE %s.%s" % (db,tbl)
        cursor.execute(sql)
        result_set = cursor.fetchone()['Create Table']
        cursor.close()
        return result_set
    except mysql.Error, e:
        logging.error('An error has occurred: %s' % e)


""" Method to detect any foreign keys. """
def detect_foreign_keys(db, tbl):
    try:
        cursor = get_mysql_conn(options.host, options.port).cursor(mysql.cursors.DictCursor)
        sql = """SELECT COUNT(*) AS count FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
              WHERE REFERENCED_TABLE_NAME IS NOT NULL
              AND ((TABLE_NAME='%s' AND TABLE_SCHEMA='%s')
              OR (REFERENCED_TABLE_NAME='%s' AND REFERENCED_TABLE_SCHEMA='%s'))""" % (tbl, db, tbl, db)
        cursor.execute(sql)
        result_set = int(cursor.fetchone()['count'])
        return result_set
    except mysql.Error, e:
        logging.error('An error has occurred: %s' % e)


""" Method to prepare and execute the actual pt-online-schema-change command."""
def osc_command():
    # Parsing the arguments provided.
    (schema, table, statement) = alter_parser()

    # Finding the appropriate value for MAX-LOAD and CRITICAL-LOAD
    try:
        cursor = get_mysql_conn(options.host, options.port).cursor(mysql.cursors.DictCursor)
        cursor.execute("SHOW GLOBAL VARIABLES LIKE 'max_connections'")
        result_set = cursor.fetchall()
        result = int(result_set[0]['Value'])
        cursor.close()
    except mysql.Error, e:
        logging.error('An error has occurred: %s' % e)

    if options.load == "high":
        max_load_val = int(round(result*0.75,0))
        crit_load_val = result + 1
    elif options.load == "medium":
        max_load_val = int(round(result*0.50,0))
        crit_load_val = int(round(result*0.75,0))
    else:
        max_load_val = int(round(result*0.25,0))
        crit_load_val = int(round(result*0.50,0))
    max_load = "Threads_running=%s" % max_load_val
    crit_load = "Threads_running=%s" % crit_load_val

    # Finding the binary for pt-online-schema-change
    PT_OSC_BIN = check_pt_toolkit()

    # Preparing the pt-osc command.
    if options.print_only is True:
        alter = '--alter="%s"'
    else:
        alter = '--alter=%s'

    cmd = [PT_OSC_BIN, alter % statement,
           '--database=%s' % schema,
           't=%s' % table,
           '--host=%s' % options.host,
           '--port=%s' % options.port,
           '--max-load=%s' % max_load,
           '--critical-load=%s' % crit_load,
           '--no-check-replication-filters',
           '--execute']

    # If foreign_keys found, then enable --alter-foreign-keys-method=auto
    if options.do_not_check_fk is not True:
        if detect_foreign_keys(schema, table) > 0:
            cmd.append('--alter-foreign-keys-method=auto')
        else:
            logging.info('No foreign keys found on the table.')

    if options.extra_args:
        [cmd.append(opt) for opt in options.extra_args.split(',')]
    else:
        logging.info('No --extra-args provided.')

    # Setting the --set-vars options for the command.
    if options.skip_binlog:
        set_var = "--set-vars=SQL_LOG_BIN=OFF"
        cmd.append(set_var)
    elif options.set_rbr:
        set_var = "--set-vars=BINLOG_FORMAT=ROW"
        cmd.append(set_var)
    else:
        logging.info('Not setting --set-vars since no option was specified.')

    # Executing the pt-osc command.
    if options.print_only is True:
        print ' '.join(cmd)
        logging.info('Not executing statement since --print option was specified.')
    else:
        logging.info(show_create(schema, table))
        logging.info('Executing the pt-online-schema-change command %s' % ' '.join(cmd))
        start_time = time.time()
        try:
            osc_proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
            while True:
                update_time = start_time
                line = osc_proc.stdout.readline().rstrip("\n")
                if line.split(' ')[0].lower() == 'created':
                    temp_tbl = line.split(' ')[3].split('.')[1]
                if not line:
                    break
                elif (time.time() - update_time) > 3600:
                    text = line
                    icon = ":pencil2:"
                    update_slack(text, icon)
                    update_time = time.time()
                if options.ftwrl_guard is True:
                    # Execute the ftwrl_guardian once process reaches TRIGGER creation.
                    server_data = {'HOST': options.host, 'PORT': options.port}
                    if line.split()[1].lower() == 'creating' and line.split()[2].lower() == 'triggers...':
                        ftwrl_guardian_lib.ftwrl_guardian(server_data, check_metadata=options.ftwrl_guard)
                logging.info(line)
        except KeyboardInterrupt:
            osc_proc.kill()
            logging.error('Process has been killed abruptly')
        osc_proc.communicate()
        elapsed_time = time.strftime('%H:%M:%S', time.gmtime(time.time() - start_time))
        logging.info('Online schema change for %s.%s completed in %s' % (schema,table,elapsed_time))
        logging.info(show_create(schema, table))
        ret = osc_proc.returncode

    # Updating final status on Slack-room provided.
        if options.slack_room is None:
            logging.info('Not updating Slack room since option was not specified.')
            if ret != 0 and ret != 255:
                cleanup_objects(table,temp_tbl,schema)
        else:
            if ret == 0:
                text = "Online schema change for table %s.%s has SUCCEEDED" % (schema, table)
                icon = ":heavy_check_mark:"
                update_slack(text, icon)
            elif ret == 255:
                text = "Online schema change for table %s.%s has FAILED" % (schema, table)
                icon = ":heavy_multiplication_x:"
                update_slack(text, icon)
            else:
                text = "Online schema change for table %s.%s has an UNKNOWN ERROR" % (schema, table)
                icon = ":interrobang:"
                update_slack(text, icon)
                cleanup_objects(table,temp_tbl,schema)


""" Method to execute the alter directly in MySQL if specified as an option."""
def direct_alter():
    try:
        cursor = get_mysql_conn(options.host, options.port).cursor(mysql.cursors.DictCursor)
        start_time = time.time()
        cursor.execute(options.alter_statement)
        end_time = time.time()
        cursor.close()
        logging.info('Completed direct alter in %s seconds'
                      % time.strftime('%H:%M:%S', time.gmtime(end_time - start_time)))
        if not options.slack_room:
            logging.info('Not updating Slack room since option was not specified.')
        else:
            text = "%s has SUCCEEDED" % options.alter_statement
            icon = ":heavy_check_mark:"
            update_slack(text, icon)
    except mysql.Error, e:
        logging.error('An error has occurred: %s' % e)
        if options.slack_room is None:
            logging.info('Not updating Slack room since option was not specified.')
        else:
            text = "%s has FAILED" % options.alter_statement
            icon = ":heavy_multiplication_mark:"
            update_slack(text, icon)


""" Method to do a clean up in case pt-online-schema-change fails abruptly. Dropping TRIGGERS before the
TEMP TABLE is dropped avoiding INSERT, UPDATE and DELETE statement failure. """
def cleanup_objects(tbl,temp_tbl,db):
    ins_trigger = 'pt_osc_' + db + '_' + tbl + '_ins'
    del_trigger = 'pt_osc_' + db + '_' + tbl + '_del'
    upd_trigger = 'pt_osc_' + db + '_' + tbl + '_upd'
    sql_ins = """ DROP TRIGGER IF EXISTS %s.%s""" % (db,ins_trigger)
    sql_del = """ DROP TRIGGER IF EXISTS %s.%s""" % (db,del_trigger)
    sql_upd = """ DROP TRIGGER IF EXISTS %s.%s""" % (db,upd_trigger)
    sql_drop = """ DROP TABLE IF EXISTS %s.%s """ % (db,temp_tbl)
    logging.info('Cleaning up TRIGGERS.')
    try:
        cursor = get_mysql_conn(options.host, options.port).cursor(mysql.cursors.DictCursor)
        cursor.execute(sql_ins)
        cursor.execute(sql_upd)
        cursor.execute(sql_del)
        logging.info('Dropped the Insert, Update and Delete TRIGGERS, after online schema change failed.')
    except mysql.Error, e:
        logging.error('Could not clean up triggers due to some problem: %s' % e)
        logging.info('Please execute the following SQL commands to DROP triggers safely,'
                          '%s;'
                          '%s;'
                          '%s;' % (sql_ins, sql_del, sql_upd))
    logging.info('Cleaning up TEMP TABLE: %s' % temp_tbl)
    try:
        cursor.execute(sql_drop)
        logging.info('Dropped the TEMP TABLE created by tool pt-online-schema-change')
    except mysql.Error, e:
        logging.error('Could not clean up triggers due to some problem: %s' % e)
        logging.info('Please execute the following SQL commands to DROP triggers safely,'
                          '%s;' % sql_drop)


""" Method to update the Slack chat using Incoming Webhooks available as an integration in Slack. The chat room is
provided as an argument to the tool."""
def update_slack(text, icon):
    # Preparing the webhook for Slack.
    url = 'https://hooks.slack.com/services/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'
    payload = 'payload={"channel": "%s","username": "Online-Alter-Update", "text": "%s", "icon_emoji": "%s"}' \
              % (options.slack_room, text, icon)
    slack_cmd = ['curl', '-X', 'POST', '--data-urlencode', payload, url]
    slac_proc = subprocess.Popen(slack_cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    slac_proc.communicate()


""" main() method."""
def main():
    global options
    options = parse_options()
    logging_setup()
    if options.type == "online":
        logging.info('Using pt-online-schema-change by default.')
        osc_command()
    else:
        logging.info('Using direct "ALTER TABLE" statement as specified with --type option.')
        direct_alter()

if __name__ == "__main__":
    main()
