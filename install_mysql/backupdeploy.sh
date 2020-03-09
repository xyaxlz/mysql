#!/bin/bash
#for i in {1,2,3,4}
for i in {3,4,5,6,7,8,9,10,11,12,13}
#for i in {3,4,5,6,7,9,10,11,12,13}
do 
	echo $i
	ssh db${i}v.infra.bjac.pdtv.it  "mkdir -p /data/scripts"
	scp innobackupex.sh  db${i}v.infra.bjac.pdtv.it:/data/scripts
	ssh db${i}v.infra.bjac.pdtv.it  "chmod +x  /data/scripts/innobackupex.sh"
	ssh db${i}v.infra.bjac.pdtv.it  "sed -i '/###数据库备份/d'  /var/spool/cron/root"
	ssh db${i}v.infra.bjac.pdtv.it  "sed -i '/innobackupex.sh/d' /var/spool/cron/root"
	ssh db${i}v.infra.bjac.pdtv.it  "echo \"###数据库备份\" >> /var/spool/cron/root"
	
	ssh db${i}v.infra.bjac.pdtv.it  "echo \"0 5 * * * /data/scripts/innobackupex.sh >/dev/null 2>&1\" >> /var/spool/cron/root "
	#mysql -h db${i}v.infra.bjac.pdtv.it -ubackup -pbackup -e "GRANT RELOAD, LOCK TABLES, REPLICATION CLIENT, CREATE TABLESPACE, SUPER ON *.* TO 'dbback'@'localhost' identified by 'dbback' "
done
