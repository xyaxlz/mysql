for i in `seq 12`
do 
#scp 030.host.default  mdb${i}v.infra.bjac.pdtv.it:/home/server_config/iptables_rules/
#ssh mdb${i}v.infra.bjac.pdtv.it "/etc/init.d/iptables restart"
	ssh db${i}v.infra.bjza.pdtv.it "mkdir -p /data/install"
	scp -r  mysqlali db${i}v.infra.bjza.pdtv.it:/data/install
	#ssh db${i}v.infra.bjza.pdtv.it  "yum install http://www.percona.com/downloads/percona-release/redhat/0.1-3/percona-release-0.1-3.noarch.rpm  -y"
	#ssh db${i}v.infra.bjza.pdtv.it  "yum install percona-toolkit  -y"
	#ssh db${i}v.infra.bjza.pdtv.it  "yum install percona-xtrabackup -y"
done
