#!/usr/bin/python
# -*- coding: utf8 -*-
"""
Usage:
        pybackup.py mydumper ARG_WITH_NO_--... (([--no-ftp] [--no-history]) | [--only-backup]) --inst-port=<port>
        pybackup.py only-ftp --inst-port=<port> [--backup-dir=<DIR>] [--bk-id=<id>] [--log-file=<log>]
        pybackup.py mark-del --inst-port=<port> [--bk-id=<id>]
        pybackup.py validate-backup --inst-port=<port> --log-file=<log> [--bk_id=]
        pybackup.py -h | --help
        pybackup.py --version

Options:
        -h --help                      Show help information.
        --version                      Show version.
        --no-ftp                     Do not use ftp.
        --no-history                   Do not record backup history information.
        --only-backup                  Equal to use both --no-ftp and --no-history.
        --only-ftp                   When you backup complete, but ftp failed, use this option to ftp your backup.
        --backup-dir=<DIR>             The directory where the backuped files are located. [default: ./]
        --bk-id=<id>                   bk-id in table user_backup.
        --log-file=<log>               log file [default: ./pybackup_default.log]
        --inst-port=<port>             instance port

more help information in:
https://github.com/Fanduzi
"""

#示例
"""
python pybackup.py only-ftp --backup-dir=/data/backup_db/2017-11-28 --bk-id=9fc4b0ba-d3e6-11e7-9fd7-00163f001c40 --log-file=ftp.log
--backup-dir 最后日期不带/ 否则将传到ftp://platform@106.3.130.84/db_backup2/120.27.143.36/目录下而不是ftp://platform@106.3.130.84/db_backup2/120.27.143.36/2017-11-28目录下
python /data/backup_db/pybackup.py mydumper password=xx user=root socket=/data/mysql/mysql.sock outputdir=/data/backup_db/2017-11-28 verbose=3 compress threads=8 triggers events routines use-savepoints logfile=/data/backup_db/pybackup.log
"""

import os
import sys
import subprocess
import datetime
import logging
import pymysql
import uuid
import copy
import ConfigParser
import time
import socket
import shutil
from optparse import OptionParser

from docopt import docopt



def confLog(bk_date,bk_id):
    '''日志配置'''
    if arguments['only-ftp'] or arguments['validate-backup']:
        log = arguments['--log-file']
        if log == './pybackup_default.log':
            log = '/data/backup/mysql/%s/mydumper/backup_log/pybackup_%s.log' %(inst_port,inst_port)
    else:
        log_file = [x for x in arguments['ARG_WITH_NO_--'] if 'logfile' in x]
        if not log_file:
            print('必须指定--logfile选项')
            sys.exit(1)
        else:
            log = log_file[0].split('=')[1]
            if ".log" not in str(log):
                log_dir = log + '/backup_log'
                log = log_dir + '/bak_' + bk_date + '_' + bk_id + '.log'
            if not os.path.isdir(log_dir):
                os.makedirs(log_dir)
            arguments['ARG_WITH_NO_--'].remove(log_file[0])
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                        #datefmt='%a, %d %b %Y %H:%M:%S',
                        datefmt='%Y-%m-%d %H:%M:%S',
                        filename=log,
                        filemode='a')


def getMdumperCmd(*args):
    '''拼接mydumper命令'''
    cmd = 'mydumper '
    for i in range(0, len(args)):
        if i == len(args) - 1:
            cmd += str(args[i])
        else:
            cmd += str(args[i]) + ' '
    return(cmd)


def getDBS(targetdb):
    '''获取查询数据库的语句'''
    if tdb_list:
        sql = 'select SCHEMA_NAME from schemata where 1=1 '
        if tdb_list != '%':
            dbs = tdb_list.split(',')
            for i in range(0, len(dbs)):
                if dbs[i][0] != '!':
                    if len(dbs) == 1:
                        sql += "and (SCHEMA_NAME like '" + dbs[0] + "')"
                    else:
                        if i == 0:
                            sql += "and (SCHEMA_NAME like '" + dbs[i] + "'"
                        elif i == len(dbs) - 1:
                            sql += " or SCHEMA_NAME like '" + dbs[i] + "')"
                        else:
                            sql += " or SCHEMA_NAME like '" + dbs[i] + "'"
                elif dbs[i][0] == '!':
                    if len(dbs) == 1:
                        sql += "and (SCHEMA_NAME not like '" + dbs[0][1:] + "')"
                    else:
                        if i == 0:
                            sql += "and (SCHEMA_NAME not like '" + dbs[i][1:] + "'"
                        elif i == len(dbs) - 1:
                            sql += " and SCHEMA_NAME not like '" + dbs[i][1:] + "')"
                        else:
                            sql += " and SCHEMA_NAME not like '" + dbs[i][1:] + "'"
        elif tdb_list == '%':
            dbs = ['%']
            sql = "select SCHEMA_NAME from schemata where SCHEMA_NAME like '%'"
        print('getDBS: ' + sql)
        bdb = targetdb.dql(sql)
        bdb_list = []
        for i in range(0, len(bdb)):
            bdb_list += bdb[i]
        return bdb_list
    else:
        return None


class Fandb:
    '''定义pymysql类'''

    def __init__(self, host, port, user, password, db, charset='utf8mb4'):
        self.host = host
        self.port = int(port)
        self.user = user
        self.password = password
        self.db = db
        self.charset = charset
        try:
            self.conn = pymysql.connect(host=self.host, port=self.port, user=self.user,
                                        password=self.password, db=self.db, charset=self.charset)
            self.cursor = self.conn.cursor()
            self.diccursor = self.conn.cursor(pymysql.cursors.DictCursor)
        except Exception, e:
            logging.error('connect error', exc_info=True)

    def dml(self, sql, val=None):
        self.cursor.execute(sql, val)

    def version(self):
        self.cursor.execute('select version()')
        return self.cursor.fetchone()

    def dql(self, sql):
        self.cursor.execute(sql)
        return self.cursor.fetchall()

    def commit(self):
        self.conn.commit()

    def close(self):
        self.cursor.close()
        self.diccursor.close()
        self.conn.close()


def runBackup(targetdb):
    '''执行备份'''
    # 是否指定了--database参数
    isDatabase_arg = [ x for x in arguments['ARG_WITH_NO_--'] if 'database' in x ]
    isTables_list = [ x for x in arguments['ARG_WITH_NO_--'] if 'tables-list' in x ]
    isRegex = [ x for x in arguments['ARG_WITH_NO_--'] if 'regex' in x ]
    #备份的数据库 字符串
    start_time = datetime.datetime.now()
    logging.info('Begin Backup')
    print(str(start_time) + ' Begin Backup')
    # 指定了--database参数,则为备份单个数据库,即使配置文件中指定了也忽略

    if isTables_list:
        targetdb.close()
        print(mydumper_args)
        cmd = getMdumperCmd(*mydumper_args)
        cmd_list = cmd.split(' ')
        if 'password' in cmd:
            passwd = [x.split('=')[1] for x in cmd_list if 'password' in x][0]
        else:
            passwd = tdb_passwd
            cmd = cmd + ' --password=' + passwd
        if '--user' not in cmd:
            mydumper_user = tdb_user
            cmd = cmd + ' --user=' + mydumper_user
        cmd = cmd.replace(passwd, '"'+passwd+'"')
        backup_dest_root = [x.split('=')[1] for x in cmd_list if 'outputdir' in x][0]
        bk_date = time.strftime("%Y%m%d", time.localtime())
        backup_dest = os.path.join(backup_dest_root,bk_date)
        if backup_dest[-1] != '/':
            uuid_dir = backup_dest + '/' + bk_id + '/'
        else:
            uuid_dir = backup_dest + bk_id + '/'

        if not os.path.isdir(uuid_dir):
            os.makedirs(uuid_dir)
        cmd = cmd.replace(backup_dest_root, uuid_dir)
        child = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        while child.poll() == None:
            stdout_line = child.stdout.readline().strip()
            if stdout_line:
                logging.info(stdout_line)
        logging.info(child.stdout.read().strip())
        state = child.returncode
        logging.info('backup state:'+str(state))
        # 检查备份是否成功
        if state != 0:
            logging.critical(' Backup Failed!')
            is_complete = 'N'
            end_time = datetime.datetime.now()
            print(str(end_time) + ' Backup Failed')
        elif state == 0:
            end_time = datetime.datetime.now()
            logging.info('End Backup')
            is_complete = 'Y'
            print(str(end_time) + ' Backup Complete')
        elapsed_time = (end_time - start_time).total_seconds()
        bdb = [ x.split('=')[1] for x in cmd_list if 'tables-list' in x ][0]
        return start_time, end_time, elapsed_time, is_complete, cmd, bdb, uuid_dir, uuid_dir, 'tables-list'
    elif isRegex:
        targetdb.close()
        print(mydumper_args)
        cmd = getMdumperCmd(*mydumper_args)
        cmd_list = cmd.split(' ')
        #passwd = [x.split('=')[1] for x in cmd_list if 'password' in x][0]
        #cmd = cmd.replace(passwd, '"'+passwd+'"')
        if 'password' in cmd:
            passwd = [x.split('=')[1] for x in cmd_list if 'password' in x][0]
        else:
            passwd = tdb_passwd
            cmd = cmd + ' --password=' + passwd
        if '--user' not in cmd:
            mydumper_user = tdb_user
            cmd = cmd + ' --user=' + mydumper_user
        cmd = cmd.replace(passwd, '"'+passwd+'"')
        regex_expression = [x.split('=')[1] for x in cmd_list if 'regex' in x][0]
        cmd = cmd.replace(regex_expression, "'" + regex_expression + "'")
        backup_dest_root = [x.split('=')[1] for x in cmd_list if 'outputdir' in x][0]
        bk_date = time.strftime("%Y%m%d", time.localtime())
        backup_dest = os.path.join(backup_dest_root,bk_date)
        if backup_dest[-1] != '/':
            uuid_dir = backup_dest + '/' + bk_id + '/'
        else:
            uuid_dir = backup_dest + bk_id + '/'

        if not os.path.isdir(uuid_dir):
            os.makedirs(uuid_dir)
        cmd = cmd.replace(backup_dest_root, uuid_dir)
        child = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        while child.poll() == None:
            stdout_line = child.stdout.readline().strip()
            if stdout_line:
                logging.info(stdout_line)
        logging.info(child.stdout.read().strip())
        state = child.returncode
        logging.info('backup state:'+str(state))
        # 检查备份是否成功
        if state != 0:
            logging.critical(' Backup Failed!')
            is_complete = 'N'
            end_time = datetime.datetime.now()
            print(str(end_time) + ' Backup Failed')
        elif state == 0:
            end_time = datetime.datetime.now()
            logging.info('End Backup')
            is_complete = 'Y'
            print(str(end_time) + ' Backup Complete')
        elapsed_time = (end_time - start_time).total_seconds()
        bdb = [ x.split('=')[1] for x in cmd_list if 'regex' in x ][0]
        return start_time, end_time, elapsed_time, is_complete, cmd, bdb, uuid_dir, uuid_dir, 'regex'
    elif isDatabase_arg:
        targetdb.close()
        print(mydumper_args)
        bdb = isDatabase_arg[0].split('=')[1]
        # 生成备份命令
        database = [ x.split('=')[1] for x in mydumper_args if 'database' in x ][0]
        outputdir_arg = [ x for x in mydumper_args if 'outputdir' in x ]
        bk_date = time.strftime("%Y%m%d", time.localtime())
        temp_mydumper_args = copy.deepcopy(mydumper_args)
        if outputdir_arg[0][-1] != '/':
            outputdir_arg_new = outputdir_arg[0]+'/'+bk_date
            temp_mydumper_args.remove(outputdir_arg[0])
            temp_mydumper_args.append(outputdir_arg_new+'/' + bk_id + '/' + database)
            last_outputdir = (outputdir_arg_new + '/' + bk_id + '/' + database).split('=')[1]
            last_outputdir_compress = (outputdir_arg_new +'/' + bk_id).split('=')[1]
        else:
            outputdir_arg_new = outputdir_arg[0]+bk_date
            temp_mydumper_args.remove(outputdir_arg[0])
            temp_mydumper_args.append(outputdir_arg_new + bk_id + '/' + database)
            last_outputdir = (outputdir_arg_new + bk_id + '/' + database).split('=')[1]
            last_outputdir_compress = (outputdir_arg_new +'/' + bk_id).split('=')[1]
        if not os.path.isdir(last_outputdir):
            os.makedirs(last_outputdir)
        cmd = getMdumperCmd(*temp_mydumper_args)
        #密码中可能有带'#'或括号的,处理一下用引号包起来
        cmd_list = cmd.split(' ')
        #passwd = [x.split('=')[1] for x in cmd_list if 'password' in x][0]
        #cmd = cmd.replace(passwd, '"'+passwd+'"')
        if 'password' in cmd:
            passwd = [x.split('=')[1] for x in cmd_list if 'password' in x][0]
        else:
            passwd = tdb_passwd
            cmd = cmd + ' --password=' + passwd
        if '--user' not in cmd:
            mydumper_user = tdb_user
            cmd = cmd + ' --user=' + mydumper_user
        cmd = cmd.replace(passwd, '"'+passwd+'"')
        child = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        while child.poll() == None:
            stdout_line = child.stdout.readline().strip()
            if stdout_line:
                logging.info(stdout_line)
        logging.info(child.stdout.read().strip())
        state = child.returncode
        logging.info('backup state:'+str(state))
        # 检查备份是否成功
        if state != 0:
            logging.critical(' Backup Failed!')
            is_complete = 'N'
            end_time = datetime.datetime.now()
            print(str(end_time) + ' Backup Failed')
        elif state == 0:
            end_time = datetime.datetime.now()
            logging.info('End Backup')
            is_complete = 'Y'
            print(str(end_time) + ' Backup Complete')
        elapsed_time = (end_time - start_time).total_seconds()
        return start_time, end_time, elapsed_time, is_complete, cmd, bdb, last_outputdir, last_outputdir_compress, 'database'
    # 没有指定--database参数
    elif not isDatabase_arg:
        # 获取需要备份的数据库的列表
        bdb_list = getDBS(targetdb)
        targetdb.close()
        print(bdb_list)
        bdb = ','.join(bdb_list)
        # 如果列表为空,报错
        if not bdb_list:
            logging.critical('必须指定--database或在配置文件中指定需要备份的数据库')
            sys.exit(1)

        if db_consistency.upper() == 'TRUE':
            regex = ' --regex="^(' + '\.|'.join(bdb_list) + '\.' + ')"'
            print(mydumper_args)
            cmd = getMdumperCmd(*mydumper_args)
            cmd_list = cmd.split(' ')
            #passwd = [x.split('=')[1] for x in cmd_list if 'password' in x][0]
            #cmd = cmd.replace(passwd, '"'+passwd+'"')
            if 'password' in cmd:
                print 'a'
                passwd = [x.split('=')[1] for x in cmd_list if 'password' in x][0]
            else:
                passwd = tdb_passwd
                cmd = cmd + ' --password=' + passwd
            if '--user' not in cmd:
                mydumper_user = tdb_user
                cmd = cmd + ' --user=' + mydumper_user
            cmd = cmd.replace(passwd, '"'+passwd+'"')
            backup_dest_root = [x.split('=')[1] for x in cmd_list if 'outputdir' in x][0]
            bk_date = time.strftime("%Y%m%d", time.localtime())
            backup_dest = os.path.join(backup_dest_root,bk_date)
            if backup_dest[-1] != '/':
                uuid_dir = backup_dest + '/' + bk_id + '/'
            else:
                uuid_dir = backup_dest + bk_id + '/'
            if not os.path.isdir(uuid_dir):
                os.makedirs(uuid_dir)
            cmd = cmd.replace(backup_dest_root, uuid_dir)
            cmd = cmd + regex
            child = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
            while child.poll() == None:
                stdout_line = child.stdout.readline().strip()
                if stdout_line:
                    logging.info(stdout_line)
            logging.info(child.stdout.read().strip())
            state = child.returncode
            logging.info('backup state:'+str(state))
            # 检查备份是否成功
            if state != 0:
                logging.critical(' Backup Failed!')
                is_complete = 'N'
                end_time = datetime.datetime.now()
                print(str(end_time) + ' Backup Failed')
            elif state == 0:
                end_time = datetime.datetime.now()
                logging.info('End Backup')
                is_complete = 'Y'
                print(str(end_time) + ' Backup Complete')
                for db in bdb_list:
                    os.makedirs(uuid_dir + db)
                    os.chdir(uuid_dir)
                    mv_cmd = 'mv `ls ' + uuid_dir + '|grep -v "^' + db + '$"|grep -E "' + db + '\.|' + db + '-' + '"` '  + uuid_dir + db + '/'
                    print(mv_cmd)
                    child = subprocess.Popen(mv_cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
                    while child.poll() == None:
                        stdout_line = child.stdout.readline().strip()
                        if stdout_line:
                            logging.info(stdout_line)
                    logging.info(child.stdout.read().strip())
                    state = child.returncode
                    logging.info('mv state:'+str(state))
                    if state != 0:
                        logging.critical(' mv Failed!')
                        print('mv Failed')
                    elif state == 0:
                        logging.info('mv Complete')
                        print('mv Complete')
                    cp_metadata = 'cp ' + uuid_dir + 'metadata ' + uuid_dir + db + '/'
                    subprocess.call(cp_metadata, shell=True)
            elapsed_time = (end_time - start_time).total_seconds()
            return start_time, end_time, elapsed_time, is_complete, cmd, bdb, uuid_dir, uuid_dir, 'db_consistency'
        else:
            # 多个备份,每个备份都要有成功与否状态标记
            is_complete = ''
            bk_date = time.strftime("%Y%m%d", time.localtime())
            # 在备份列表中循环
            for i in bdb_list:
                comm = []
                # 一次备份一个数据库,下次循环将comm置空
                outputdir_arg = [ x for x in mydumper_args if 'outputdir' in x ]
                temp_mydumper_args = copy.deepcopy(mydumper_args)
                if outputdir_arg[0][-1] != '/':
                    outputdir_arg_new = outputdir_arg[0]+'/'+bk_date
                    temp_mydumper_args.remove(outputdir_arg[0])
                    temp_mydumper_args.append(outputdir_arg_new + '/' + bk_id + '/' + i)
                    last_outputdir = (outputdir_arg_new +'/' + bk_id + '/' + i).split('=')[1]
                    last_outputdir_compress = (outputdir_arg_new +'/' + bk_id).split('=')[1]
                else:
                    outputdir_arg_new = outputdir_arg[0]+bk_date
                    temp_mydumper_args.remove(outputdir_arg[0])
                    temp_mydumper_args.append(outputdir_arg_new + bk_id + '/' + i)
                    last_outputdir = (outputdir_arg_new + bk_id + '/' + i).split('=')[1]
                    last_outputdir_compress = (outputdir_arg_new +'/' + bk_id).split('=')[1]
                if not os.path.isdir(last_outputdir):
                    os.makedirs(last_outputdir)
                comm = temp_mydumper_args + ['--database=' + i]
                # 生成备份命令
                cmd = getMdumperCmd(*comm)
                #密码中可能有带'#'或括号的,处理一下用引号包起来
                cmd_list = cmd.split(' ')
                #passwd = [x.split('=')[1] for x in cmd_list if 'password' in x][0]
                #cmd = cmd.replace(passwd, '"'+passwd+'"')
                if 'password' in cmd:
                    passwd = [x.split('=')[1] for x in cmd_list if 'password' in x][0]
                else:
                    passwd = tdb_passwd
                    cmd = cmd + ' --password=' + passwd
                if '--user' not in cmd:
                    mydumper_user = tdb_user
                    cmd = cmd + ' --user=' + mydumper_user
                cmd = cmd.replace(passwd, '"'+passwd+'"')
                child = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
                while child.poll() == None:
                    stdout_line = child.stdout.readline().strip()
                    if stdout_line:
                        logging.info(stdout_line)
                logging.info(child.stdout.read().strip())
                state = child.returncode
                logging.info('backup state:'+str(state))
                if state != 0:
                    logging.critical(i + ' Backup Failed!')
                    # Y,N,Y,Y
                    if is_complete:
                        is_complete += ',N'
                    else:
                        is_complete += 'N'
                    end_time = datetime.datetime.now()
                    print(str(end_time) + ' ' + i + ' Backup Failed')
                elif state == 0:
                    if is_complete:
                        is_complete += ',Y'
                    else:
                        is_complete += 'Y'
                    end_time = datetime.datetime.now()
                    logging.info(i + ' End Backup')
                    print(str(end_time) + ' ' + i + ' Backup Complete')
        end_time = datetime.datetime.now()
        elapsed_time = (end_time - start_time).total_seconds()
        full_comm = 'mydumper ' + \
            ' '.join(mydumper_args) + ' database=' + ','.join(bdb_list)
        print "cmd:",cmd
        return start_time, end_time, elapsed_time, is_complete, full_comm, bdb, last_outputdir, last_outputdir_compress, 'for database'


#def getIP():
#    '''获取ip地址'''
#    # 过滤内网IP
#    cmd = "/sbin/ifconfig  | /bin/grep  'inet addr:' | /bin/grep -v '127.0.0.1' | /bin/grep -v '192\.168' | /bin/grep -v '10\.'|  /bin/cut -d: -f2 | /usr/bin/head -1 |  /bin/awk '{print $1}'"
#    child = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE)
#    child.wait()
#    ipaddress = child.communicate()[0].strip()
#    return ipaddress

def getIP():
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(('8.8.8.8', 80))
        ip = s.getsockname()[0]
    finally:
        s.close()

    return (ip)


def getBackupSize(outputdir):
    '''获取备份集大小'''
    cmd = 'du -sh ' + os.path.abspath(outputdir)
    child = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE)
    child.wait()
    backup_size = child.communicate()[0].strip().split('\t')[0]
    return backup_size

def getMetadata(outputdir):
    '''从metadata中获取 SHOW MASTER STATUS / SHOW SLAVE STATUS 信息'''
    if outputdir[-1] != '/':
        metadata = outputdir + '/metadata'
    else:
        metadata = outputdir + 'metadata'
    with open(metadata, 'r') as file:
        content = file.readlines()

    separate_pos = content.index('\n')

    master_status = content[:separate_pos]
    master_log = [x.split(':')[1].strip() for x in master_status if 'Log' in x]
    master_pos = [x.split(':')[1].strip() for x in master_status if 'Pos' in x]
    master_gtid_temp = "".join(master_status[4:separate_pos])
    master_GTID = master_gtid_temp.replace("\n","").replace("\t","").replace(" ","")
    #master_GTID = [x.split(':')[1].strip()
    #               for x in master_status if 'GTID' in x]
    master_info = '|'.join(master_log + master_pos) + "|" + master_GTID
    #master_info = ','.join(master_log + master_pos + master_GTID)

    slave_status = content[separate_pos + 1:]
    if not 'Finished' in slave_status[0]:
        slave_separate_pos = slave_status.index('\n')
        slave_log = [x.split(':')[1].strip() for x in slave_status if 'Log' in x]
        slave_pos = [x.split(':')[1].strip() for x in slave_status if 'Pos' in x]
        slave_gtid_temp = "".join(slave_status[4:slave_separate_pos])
        slave_GTID = slave_gtid_temp.replace("\n","").replace("\t","").replace(" ","")
        #slave_GTID = [x.split(':')[1].strip() for x in slave_status if 'GTID' in x]
        slave_info = '|'.join(slave_log + slave_pos ) + "|" + slave_GTID
        #slave_info = ','.join(slave_log + slave_pos + slave_GTID)
        return master_info, slave_info
    else:
        return master_info, 'Not a slave'

def compress_backup(bk_dir):
    compress_status = ''
    bk_time = time.strftime("%Y%m%d%H%M%S", time.localtime()) 
    compress_cmd = "tar -zcvf %s/%s.tar.gz %s/* --remove-files" %(bk_dir,bk_time,bk_dir)
    os.popen(compress_cmd).read()
    if os.path.exists(bk_dir+'/'+bk_time+'.tar.gz'):
        compress_status = 1  
    else:
        compress_status = 0
    return (compress_status)

def safeCommand(cmd):
    '''移除bk_command中的密码'''
    cmd_list = cmd.split(' ')
    passwd = [x.split('=')[1] for x in cmd_list if 'password' in x][0]
    safe_command = cmd.replace(passwd, 'supersecrect')
    return safe_command


def getVersion(db):
    '''获取mydumper 版本和 mysql版本'''
    child = subprocess.Popen('mydumper --version',
                             shell=True, stdout=subprocess.PIPE)
    child.wait()
    mydumper_version = child.communicate()[0].strip()
    mysql_version = db.version()
    return mydumper_version, mysql_version


#def ftp(bk_dir, address):
def ftp(bk_dir):
    '''ftp, bk_dir为备份所在目录,address为使用的网卡'''
    cmd = 'lftp -u {},{}  -e "set net:limit-rate 5000k:5000k; mirror -R  --only-newer --verbose {} {} ; bye" {}'.format(ftp_user, ftp_password, bk_dir, dest, ftp_address)
    start_time = datetime.datetime.now()
    logging.info('Start ftp')
    logging.info(cmd)
    print(str(start_time) + ' Start ftp')
    child = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    while child.poll() == None:
        stdout_line = child.stdout.readline().strip()
        if stdout_line:
            logging.info(stdout_line)
    logging.info(child.stdout.read().strip())
    state = child.returncode
    logging.info('ftp state:'+str(state))
    if state != 0:
        end_time = datetime.datetime.now()
        logging.critical('ftp Failed!')
        print(str(end_time) + ' ftp Failed!')
        is_complete = 'N'
    else:
        end_time = datetime.datetime.now()
        logging.info('ftp complete')
        print(str(end_time) + ' ftp complete')
        is_complete = 'Y'
    elapsed_time = (end_time - start_time).total_seconds()
    return start_time, end_time, elapsed_time, is_complete


def markDel(delete_host,delete_port,delete_bk_id,local_delete,remote_delete,targetdb):
    if delete_host == 'localhost':
        delete_host = getIP()
    if delete_bk_id:
        del_sql = "select * from user_backup where bk_id = '%s' and (is_local_deleted='N' or is_remote_deleted='N')" %(delete_bk_id)
    else:
        del_sql = "select * from user_backup where inst_ip='%s' and inst_port='%s' and (is_local_deleted='N' or is_remote_deleted='N')" %(delete_host,delete_port)
        
    dl_result = targetdb.dql(del_sql)
    del_local_day = datetime.datetime.now() + datetime.timedelta(-int(local_delete))
    del_remote_day = datetime.datetime.now() + datetime.timedelta(-int(remote_delete))
    if dl_result:
        for dl_i in dl_result:
            if dl_i[25] == 'N' and dl_i[26] == 'N':
                if dl_i[13]:
                    del_old_day = dl_i[13]
                else:
                    del_old_day = dl_i[6]
                   
                if del_old_day < del_local_day:
                     
                    del_bk_dir = dl_i[11].replace('/'+dl_i[1]+'/','')  
                    if os.path.exists(dl_i[11]):
                        shutil.rmtree(dl_i[11])
                    if not os.path.exists(dl_i[11]):
                        ud_del_sql = "update user_backup set is_local_deleted ='Y',local_delete_time=now() where id=%s" %(dl_i[0])
                        targetdb.dml(ud_del_sql)
                        targetdb.commit()
                        if del_old_day < del_remote_day:
                            del_bk_dir = dl_i[11].replace('/'+dl_i[1]+'/','')  
                            section_name = 'ftp'
                            ftp_user  = cf.get(section_name, "user")
                            ftp_password  = cf.get(section_name, "password")
                            ftp_address  = cf.get(section_name, "address")
                            #password_file = cf.get(section_name, "password_file")
                            dest = cf.get(section_name, "dest")
                            #address = cf.get(section_name, "address")
                            #if not address:
                            #    del_cmd = 'ftp -auv --bwlimit=5000  --password-file=' + password_file + ' --delete ' + del_bk_dir + ' ftp://' + dest
                            #else:
                            #    del_cmd = 'ftp -auv --bwlimit=5000  --address=' + address +  ' --password-file=' + password_file + ' --delete ' + del_bk_dir + ' ftp://' + dest

                            #del_cmd = 'lftp -u devops-pg,VElqneUmlYQm27gX  -e "set net:limit-rate 5000k:5000k; mirror -R --delete  --only-newer --verbose {} {} ; bye" 10.10.20.40:2121'.format(del_bk_dir, dest)
                            del_cmd = 'lftp -u {},{}  -e "set net:limit-rate 5000k:5000k; mirror -R --delete  --only-newer --verbose {} {} ; bye" {}'.format(ftp_user, ftp_password, del_bk_dir, dest, ftp_address)
                            os.system(del_cmd)
                            ud_del_sql = "update user_backup set is_remote_deleted ='Y',remote_delete_time=now() where id=%s" %(dl_i[0])
                            targetdb.dml(ud_del_sql)
                            targetdb.commit()
            if dl_i[25] == 'Y' and dl_i[26] == 'N':
                if dl_i[13]:
                    del_old_day = dl_i[13]
                else:
                    del_old_day = dl_i[6]
                if del_old_day < del_remote_day:
                    del_bk_dir = dl_i[11].replace('/'+dl_i[1]+'/','')  
                    section_name = 'ftp'
                    ftp_user  = cf.get(section_name, "user")
                    ftp_password  = cf.get(section_name, "password")
                    ftp_address  = cf.get(section_name, "address")
                    #password_file = cf.get(section_name, "password_file")
                    dest = cf.get(section_name, "dest")
                    #address = cf.get(section_name, "address")
                    #if not address:
                    #    del_cmd = 'ftp -auv --bwlimit=5000  --password-file=' + password_file + ' --delete ' + del_bk_dir + ' ftp://' + dest
                    #else:
                    #    del_cmd = 'ftp -auv --bwlimit=5000  --address=' + address +  ' --password-file=' + password_file + ' --delete ' + del_bk_dir + ' ftp://' + dest
                    #del_cmd = 'lftp -u devops-pg,VElqneUmlYQm27gX  -e "set net:limit-rate 5000k:5000k; mirror -R --delete  --only-newer --verbose {} {} ; bye" 10.10.20.40:2121'.format(del_bk_dir, dest)
                    del_cmd = 'lftp -u {},{}  -e "set net:limit-rate 5000k:5000k; mirror -R --delete  --only-newer --verbose {} {} ; bye" {}'.format(ftp_user, ftp_password, del_bk_dir, dest, ftp_address)
                    os.system(del_cmd)
                    ud_del_sql = "update user_backup set is_remote_deleted ='Y',remote_delete_time=now() where id=%s" %(dl_i[0])
                    targetdb.dml(ud_del_sql)
                    targetdb.commit()
                
                
    targetdb.close()


def validateBackup(bk_id=None):
    sql = (
    "select a.id, a.bk_id, a.tag, date(start_time), real_path"
    "  from user_backup a,user_backup_path b"
    " where a.tag = b.tag"
    "   and is_complete not like '%N%'"
    "   and is_deleted != 'Y'"
    "   and transfer_complete = 'Y'"
    "   and a.tag = '{}'"
    "   and validate_status != 'passed'"
    "   and start_time >= DATE_SUB(CURDATE(), INTERVAL 1 MONTH)"
    " order by rand() limit 1"
    )

    sql2 = (
    "select a.id, a.bk_id, a.tag, date(start_time), real_path"
    "  from user_backup a,user_backup_path b"
    " where a.tag = b.tag"
    "   and a.bk_id = '{}'"
    )

    start_time, end_time, recover_status, db_list, backup_paths, bk_ids, tags = [], [], [], [], [], [], []
    for tag in bk_list:
        print(datetime.datetime.now())
        print(tag)
        logging.info('-='*20)
        logging.info('开始恢复: ' + tag)
        catalogdb = Fandb(cata_host, cata_port, cata_user, cata_passwd, cata_use)
        if bk_id:
            dql_res = catalogdb.dql(sql2.format(bk_id))
        else:
            dql_res = catalogdb.dql(sql.format(tag))
        result = dql_res[0] if dql_res else None
        if result:
            res_bk_id, res_tag, res_start_time, real_path = result[1], result[2], result[3], result[4]
            catalogdb.close()
            backup_path = real_path + str(res_start_time) + '/' + res_bk_id + '/'
            logging.info('Backup path: '+ backup_path )
            dbs = [ directory for directory in os.listdir(backup_path) if os.path.isdir(backup_path+directory) and directory != 'mysql' ]
            if dbs:
                for db in dbs:
                    '''
                    ([datetime.datetime(2017, 12, 25, 15, 11, 36, 480263), datetime.datetime(2017, 12, 25, 15, 33, 17, 292924), datetime.datetime(2017, 12, 25, 17, 10, 38, 226598), datetime.datetime(2017, 12, 25, 17, 10, 39, 374409)], [datetime.datetime(2017, 12, 25, 15, 33, 17, 292734), datetime.datetime(2017, 12, 25, 17, 10, 38, 226447), datetime.datetime(2017, 12, 25, 17, 10, 38, 855657), datetime.datetime(2017, 12, 25, 17, 10, 39, 776067)], [0, 0, 0, 0], [u'dadian', u'sdkv2', u'dopack', u'catalogdb'], [u'/data2/backup/db_backup/120.55.74.93/2017-12-23/b22694c4-e752-11e7-9370-00163e0007f1/', u'/data2/backup/db_backup/106.3.130.84/2017-12-16/12cb7486-e229-11e7-b172-005056b15d9c/'], [u'b22694c4-e752-11e7-9370-00163e0007f1', u'12cb7486-e229-11e7-b172-005056b15d9c'], ['\xe5\x9b\xbd\xe5\x86\x85sdk\xe4\xbb\x8e1', '\xe6\x96\xb0\xe5\xa4\x87\xe4\xbb\xbd\xe6\x9c\xba'])
                    insert into user_recover_info(tag, bk_id, backup_path, db, start_time, end_time, elapsed_time, recover_status) values (国内sdk从1,b22694c4-e752-11e7-9370-00163e0007f1,/data2/backup/db_backup/120.55.74.93/2017-12-23/b22694c4-e752-11e7-9370-00163e0007f1/,dadian,2017-12-25 15:11:36.480263,2017-12-25 15:33:17.292734,1300.812471,sucess)
                    insert into user_recover_info(tag, bk_id, backup_path, db, start_time, end_time, elapsed_time, recover_status) values (新备份机,12cb7486-e229-11e7-b172-005056b15d9c,/data2/backup/db_backup/106.3.130.84/2017-12-16/12cb7486-e229-11e7-b172-005056b15d9c/,sdkv2,2017-12-25 15:33:17.292924,2017-12-25 17:10:38.226447,5840.933523,sucess)

                    1 个 bk_id 对应3个备份,1 个 bk_id 对应1个备份 ,但是tag只append 了俩, 应该内个库append一次,或者改成字典
                    '''
                    tags.append(tag)
                    backup_paths.append(backup_path)
                    bk_ids.append(res_bk_id)
                    db_list.append(db)
                    full_backup_path = backup_path + db + '/'
                    #print(full_backup_path)
                    load_cmd = 'myloader -d {} --user=root --password=fanboshi --overwrite-tables --verbose=3 --threads=3'.format(full_backup_path)
                    print(load_cmd)
                    start_time.append(datetime.datetime.now())
                    logging.info('Start recover '+ db )
                    child = subprocess.Popen(load_cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
                    while child.poll() == None:
                        stdout_line = child.stdout.readline().strip()
                        if stdout_line:
                            logging.info(stdout_line)
                    logging.info(child.stdout.read().strip())
                    state = child.returncode
                    recover_status.append(state)
                    logging.info('Recover state:'+str(state))
                    end_time.append(datetime.datetime.now())
                    if state != 0:
                        logging.info('Recover {} Failed'.format(db))
                    elif state == 0:
                        logging.info('Recover {} complete'.format(db))
            else:
                load_cmd = 'myloader -d {} --user=root --password=fanboshi --overwrite-tables --verbose=3 --threads=3'.format(backup_path)
                print(load_cmd)
                tags.append(tag)
                start_time.append(datetime.datetime.now())
                logging.info('Start recover')
                child = subprocess.Popen(load_cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
                while child.poll() == None:
                    stdout_line = child.stdout.readline().strip()
                    if stdout_line:
                        logging.info(stdout_line)
                logging.info(child.stdout.read().strip())
                state = child.returncode
                recover_status.append(state)
                logging.info('Recover state:'+str(state))
                end_time.append(datetime.datetime.now())
                if state != 0:
                    logging.info('Recover Failed')
                elif state == 0:
                    logging.info('Recover complete')
                db_list.append('N/A')
                backup_paths.append(backup_path)
                bk_ids.append(res_bk_id)
    return start_time, end_time, recover_status, db_list, backup_paths, bk_ids, tags


if __name__ == '__main__':
    '''
    参数解析
    '''
    pybackup_version = 'pybackup 0.10.13.0'
    arguments = docopt(__doc__, version=pybackup_version)
    print(arguments)

    '''
    解析配置文件获取参数
    '''
    if arguments['mydumper'] and ('help' in arguments['ARG_WITH_NO_--'][0]):
        subprocess.call('mydumper --help', shell=True)
        sys.exit(1)
    inst_port = arguments['--inst-port']
    cf = ConfigParser.ConfigParser()
    cf.read(os.path.split(os.path.realpath(__file__))[0] + '/pybackup_'+ inst_port + '.conf')
#    print(os.getcwd())
#    print(os.path.split(os.path.realpath(__file__))[0])
    section_name = 'CATALOG'
    cata_host = cf.get(section_name, "db_host")
    cata_port = cf.get(section_name, "db_port")
    cata_user = cf.get(section_name, "db_user")
    cata_passwd = cf.get(section_name, "db_passwd")
    cata_use = cf.get(section_name, "db_use")

    if not arguments['validate-backup'] and not arguments['mark-del']:
        section_name = 'TDB'
        tdb_host = cf.get(section_name, "db_host")
        tdb_port = cf.get(section_name, "db_port")
        tdb_user = cf.get(section_name, "db_user")
        tdb_passwd = cf.get(section_name, "db_passwd")
        tdb_use = cf.get(section_name, "db_use")
        tdb_list = cf.get(section_name, "db_list")
        try:
            global db_consistency
            db_consistency = cf.get(section_name, "db_consistency")
        except ConfigParser.NoOptionError,e:
            db_consistency = 'False'
            print('没有指定db_consistency参数,默认采用--database循环备份db_list中指定的数据库,数据库之间不保证一致性')

        if cf.has_section('ftp'):
            section_name = 'ftp'
            ftp_user  = cf.get(section_name, "user")
            ftp_password  = cf.get(section_name, "password")
            ftp_address  = cf.get(section_name, "address")
            #password_file = cf.get(section_name, "password_file")
            dest = cf.get(section_name, "dest")
            #address = cf.get(section_name, "address")
            if dest[-1] != '/':
                dest += '/'
            ftp_enable = True
        else:
            ftp_enable = False
            print("没有在配置文件中指定ftp区块,备份后不传输")

        section_name = 'pybackup'
        tag = cf.get(section_name, "tag")
    elif arguments['validate-backup']:
        section_name = 'Validate'
        if arguments['--bk_id']:
            bk_list=list(arguments['--bk_id'])
        else:
            bk_list = cf.get(section_name, "bk_list").split(',')

    if arguments['mydumper'] and ('help' in arguments['ARG_WITH_NO_--'][0]):
        subprocess.call('mydumper --help', shell=True)
    elif arguments['only-ftp']:
        bk_date = time.strftime("%Y%m%d", time.localtime())
        bk_id = arguments['--bk_id']
        confLog(bk_date,bk_id)
        backup_dir = arguments['--backup-dir']
        if arguments['--bk-id']:
            #transfer_start, transfer_end, transfer_elapsed, transfer_complete = ftp(backup_dir, address)
            transfer_start, transfer_end, transfer_elapsed, transfer_complete = ftp(backup_dir)
            catalogdb = Fandb(cata_host, cata_port, cata_user, cata_passwd, cata_use)
            sql = 'update user_backup set transfer_start=%s, transfer_end=%s, transfer_elapsed=%s, transfer_complete=%s, remote_dest=concat(%s,substr(bk_dir,34)) where bk_id=%s'
            catalogdb.dml(sql, (transfer_start, transfer_end, transfer_elapsed, transfer_complete, dest,  arguments['--bk-id']))
            catalogdb.commit()
            catalogdb.close()
        else:
            #ftp(backup_dir,address)
            ftp(backup_dir)
    elif arguments['mark-del']:
        section_name = 'delete'
        local_delete = cf.get(section_name, "local_delete")
        remote_delete = cf.get(section_name, "remote_delete")
        section_name = 'TDB'
        tdb_host = cf.get(section_name, "db_host")
        tdb_port = cf.get(section_name, "db_port")
        delete_bk_id = arguments['--bk-id']
        catalogdb = Fandb(cata_host, cata_port, cata_user, cata_passwd, cata_use)
        markDel(tdb_host,tdb_port,delete_bk_id,local_delete,remote_delete,catalogdb)
    elif arguments['validate-backup']:
        bk_date = time.strftime("%Y%m%d", time.localtime()) 
        confLog(bk_date,bk_id)
        if arguments['--bk_id']:
            start_time, end_time, recover_status, db_list, backup_paths, bk_ids, tags = validateBackup(arguments['--bk_id'])
        else:
            start_time, end_time, recover_status, db_list, backup_paths, bk_ids, tags = validateBackup()
        print(start_time, end_time, recover_status, db_list, backup_paths, bk_ids, tags)
        if bk_ids:
            catalogdb = Fandb(cata_host, cata_port, cata_user, cata_passwd, cata_use)
            sql1 = "insert into user_recover_info(tag, bk_id, backup_path, db, start_time, end_time, elapsed_time, recover_status) values (%s,%s,%s,%s,%s,%s,%s,%s)"
            sql2 = "update user_backup set validate_status=%s where bk_id=%s"
            logging.info(zip(start_time, end_time, recover_status, db_list))
            for stime, etime, rstatus, db ,backup_path, bk_id, tag in zip(start_time, end_time, recover_status, db_list, backup_paths, bk_ids, tags):
                if rstatus == 0:
                    status = 'sucess'
                    failed_flag = False
                else:
                    status = 'failed'
                    failed_flag = True
#                print(sql1 % (tag.decode('utf-8'), bk_id, backup_path, db, stime, etime, (etime - stime).total_seconds(), status))
                logging.info(sql1 % (tag.decode('utf-8'), bk_id, backup_path, db, stime, etime, (etime - stime).total_seconds(), status))
                catalogdb.dml(sql1,(tag, bk_id, backup_path, db, stime, etime, (etime - stime).total_seconds(), status))
                if not failed_flag:
                    catalogdb.dml(sql2,('passed', bk_id))
                catalogdb.commit()
            catalogdb.close()
            logging.info('恢复完成')
        else:
            logging.info('没有可用备份')
            print('没有可用备份')
    else:
        bk_date = time.strftime("%Y%m%d", time.localtime()) 
        bk_id = str(uuid.uuid1())
        confLog(bk_date,bk_id)
        if arguments['mydumper']:
            mydumper_args = ['--' + x for x in arguments['ARG_WITH_NO_--']]
            is_ftp = True
            is_history = True
            if arguments['--no-ftp']:
                is_ftp = False
            if arguments['--no-history']:
                is_history = False
            if arguments['--only-backup']:
                is_history = False
                is_ftp = False
            print('is_ftp,is_history: ',is_ftp,is_history)
        bk_dir = [x for x in arguments['ARG_WITH_NO_--'] if 'outputdir' in x][0].split('=')[1]
        bk_dir = os.path.join(bk_dir,bk_date)
        if not os.path.exists(bk_dir) :
            
            os.mkdir(bk_dir)
        os.chdir(bk_dir)
        targetdb = Fandb(tdb_host, tdb_port, tdb_user, tdb_passwd, tdb_use)
        mydumper_version, mysql_version = getVersion(targetdb)
        start_time, end_time, elapsed_time, is_complete, bk_command, backuped_db, last_outputdir, last_outputdir_compress,  backup_type = runBackup(targetdb)
         
        safe_command = safeCommand(bk_command)

        if 'N' not in is_complete:
            master_info, slave_info = getMetadata(last_outputdir)
            compress_backup(last_outputdir_compress)
            #compress_backup(last_outputdir)
            bk_size = getBackupSize(last_outputdir_compress)
            if ftp_enable:
                if is_ftp:
                    #transfer_start, transfer_end, transfer_elapsed, transfer_complete_temp = ftp(bk_dir, address)
                    transfer_start, transfer_end, transfer_elapsed, transfer_complete_temp = ftp(bk_dir)
                    transfer_complete = transfer_complete_temp
                    transfer_count = 0
                    while transfer_complete_temp != 'Y' and transfer_count < 3:
                        #transfer_start_temp, transfer_end, transfer_elapsed_temp, transfer_complete_temp = ftp(bk_dir, address)
                        transfer_start_temp, transfer_end, transfer_elapsed_temp, transfer_complete_temp = ftp(bk_dir)
                        transfer_complete = transfer_complete + ',' + transfer_complete_temp
                        transfer_count += 1
                    transfer_elapsed = ( transfer_end - transfer_start ).total_seconds()
                    dest = dest + '/' + bk_date + '/' + bk_id

                else:
                    transfer_start, transfer_end, transfer_elapsed, transfer_complete = None,None,None,'N/A (local backup)'
                    dest = 'N/A (local backup)'
        else:
            bk_size = 'N/A'
            master_info, slave_info = 'N/A', 'N/A'
            transfer_start, transfer_end, transfer_elapsed, transfer_complete = None,None,None,'Backup failed'
            dest = 'Backup failed'


        if is_history:
            bk_server = getIP()
            catalogdb = Fandb(cata_host, cata_port, cata_user, cata_passwd, cata_use)
            sql = 'insert into user_backup(bk_id,inst_ip,inst_port,bk_server,start_time,end_time,elapsed_time,backuped_db,is_complete,bk_size,bk_dir,transfer_start,transfer_end,transfer_elapsed,transfer_complete,remote_dest,master_status,slave_status,tool_version,server_version,pybackup_version,bk_command,tag) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
            #sql = 'insert into user_backup(bk_id,bk_server,start_time,end_time,elapsed_time,backuped_db,is_complete,bk_size,bk_dir,transfer_start,transfer_end,transfer_elapsed,transfer_complete,remote_dest,master_status,slave_status,tool_version,server_version,pybackup_version,bk_command,tag) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
            if backup_type == 'for database':
                last_outputdir = os.path.abspath(os.path.join(last_outputdir,'..'))
            if tdb_host == 'localhost':
                tdb_host = bk_server
            print(bk_id, tdb_host, tdb_port, bk_server, start_time, end_time, elapsed_time, backuped_db, is_complete, bk_size, last_outputdir, transfer_start, transfer_end,transfer_elapsed, transfer_complete, dest, master_info, slave_info, mydumper_version, mysql_version, pybackup_version, safe_command)
            #print(bk_id, tdb_host, tdb_port, bk_server, start_time, end_time, elapsed_time, backuped_db, is_complete, bk_size, last_outputdir, transfer_start, transfer_end,transfer_elapsed, transfer_complete, dest, master_info, slave_info, mydumper_version, mysql_version, pybackup_version, safe_command)
            catalogdb.dml(sql, (bk_id,tdb_host, tdb_port, bk_server, start_time, end_time, elapsed_time, backuped_db, is_complete, bk_size, last_outputdir, transfer_start, transfer_end,
                              transfer_elapsed, transfer_complete, dest, master_info, slave_info, mydumper_version, mysql_version, pybackup_version, safe_command, tag))
            catalogdb.commit()
            catalogdb.close()

        section_name = 'delete'
        local_delete = cf.get(section_name, "local_delete")
        remote_delete = cf.get(section_name, "remote_delete")
        section_name = 'TDB'
        tdb_host = cf.get(section_name, "db_host")
        tdb_port = cf.get(section_name, "db_port")
        delete_bk_id = arguments['--bk-id']
        catalogdb = Fandb(cata_host, cata_port, cata_user, cata_passwd, cata_use)
        markDel(tdb_host,tdb_port,delete_bk_id,local_delete,remote_delete,catalogdb)
