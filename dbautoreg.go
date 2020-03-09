package main

import (
	"bytes"
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"net"
	"os/exec"
	"regexp"
	"strings"
)

//定义连接127.0.0.1 数据库账号密码
var (
	userName = "xxx"
	password = "xxx"
	//ip     = "127.0.0.1"
	port   = "3388"
	dbName = "mysql"
)

//定义连接远程mysql元数据的账号和密码
var (
	regUserName = "dbreg"
	regPassword = "xxx"
	regIp     = "xxxxx"
	regDbName = "db_asset"
	regPort   = "3388"
)

var DB *sql.DB

func getDBList(port string) (hostIp string, hostPort string, mIp string, mPort string, flag string, schemas []string, comment []string) {
	//获取本机地址
	hostPort = port
	hostIp = localIp()
	//定义数据库连接路径
	//path := strings.Join([]string{userName, ":", password, "@tcp(", ip, ":", port, ")/", dbName, "?charset=utf8"}, "")
	path := strings.Join([]string{userName, ":", password, "@tcp(", hostIp, ":", port, ")/", dbName, "?charset=utf8"}, "")
	//fmt.Println(path)
	//连接数据库
	DB, _ = sql.Open("mysql", path)

	//判断数据库是否连接成功，如果失败进入函数
	if err := DB.Ping(); err != nil {
		fmt.Printf("open %s %s database fail\n", hostIp, port)
		//返回 变量，目前都为空，退出
		hostPort = ""
		hostIp = ""
		return hostIp, hostPort, mIp, mPort, flag, schemas, comment
	}

	//获取数据库名
	rows2, _ := DB.Query("select SCHEMA_NAME  from information_schema.SCHEMATA  where SCHEMA_NAME not in ('information_schema','mysql','performance_schema','test','mysql_identity')")
	defer rows2.Close()
	//把数据库名放入shcema数组
	for rows2.Next() {
		var schema string
		err := rows2.Scan(&schema)
		checkErr(err)
		if len(schema) != 0 {
			schemas = append(schemas, schema)

		}
	}

	rows, _ := DB.Query("show slave status")
	columns, _ := rows.Columns()
	scanArgs := make([]interface{}, len(columns))
	values := make([]interface{}, len(columns))
	for i := range values {
		scanArgs[i] = &values[i]
	}

	for rows.Next() {
		//将行数据保存到record字典
		err := rows.Scan(scanArgs...)
		checkErr(err)

		record := make(map[string]string)
		for i, col := range values {
			if col != nil {
				record[columns[i]] = string(col.([]byte))
				//fmt.Println(columns[i])
				if columns[i] == "Master_Host" {
					//fmt.Println(columns[i])
					//fmt.Println(string(col.([]byte)))
					mIp = string(col.([]byte))
				}
				if columns[i] == "Master_Port" {
					//fmt.Println(columns[i])
					//fmt.Println(string(col.([]byte)))
					mPort = string(col.([]byte))
				}
				if columns[i] == "Replicate_Do_DB" && string(col.([]byte)) != "" {
					comment = append(comment, "Replicate_Do_DB:"+string(col.([]byte)))
				}
				if columns[i] == "Replicate_Ignore_DB" && string(col.([]byte)) != "" {
					comment = append(comment, "Replicate_Ignore_DB:"+string(col.([]byte)))
				}
				if columns[i] == "Replicate_Do_Table" && string(col.([]byte)) != "" {
					comment = append(comment, "Replicate_Do_Table:"+string(col.([]byte)))
				}
				if columns[i] == "Replicate_Ignore_Table" && string(col.([]byte)) != "" {
					comment = append(comment, "Replicate_Ignore_Table:"+string(col.([]byte)))
				}
				if columns[i] == "Replicate_Wild_Do_Table" && string(col.([]byte)) != "" {
					comment = append(comment, "Replicate_Wild_Do_Table:"+string(col.([]byte)))
				}
				if columns[i] == "Replicate_Wild_Ignore_Table" && string(col.([]byte)) != "" {
					comment = append(comment, "Replicate_Wild_Ignore_Table:"+string(col.([]byte)))
				}
			}
		}
		//	fmt.Println(record)
	}
	if mIp == "" {
		flag = "0"
	} else {
		flag = "1"

	}

	//fmt.Println(hostIp + " " + port + " " + mIp + " " + mPort + " " + flag)
	//返回从库ip数组 数据库名数组 和dump进程或者io进程数量
	return hostIp, hostPort, mIp, mPort, flag, schemas, comment

}

//把获取的从库ip数组，和数据库名数组，插入到远程元数据数据库
func regDB(hostIp string, hostPort string, mIp string, mPort string, flag string, schemas []string, comments []string) {
	//把数据库名数组转化为空格分隔的字符串
	dbsName := strings.Join(schemas, " ")
	comment := strings.Join(comments, " ")
	//fmt.Println(dbsName)
	//生成数据库连接地址
	regPath := strings.Join([]string{regUserName, ":", regPassword, "@tcp(", regIp, ":", regPort, ")/", regDbName, "?charset=utf8"}, "")
	//fmt.Println(regPath)
	//连接数据库
	regDB, _ := sql.Open("mysql", regPath)
	//判断数据库是否连接成功，如果失败打印日志到中断，并退出
	if err := regDB.Ping(); err != nil {
		fmt.Printf("open %s %s database fail\n", regIp, regPort)
		return
	}

	//prepare insert 语句
	stmt, err := regDB.Prepare(`replace into mysql_asset (ip, port, mip, mport, flag, dbs, comment) values (?, ?, ?, ?, ?, ?, ?)`)
	checkErr(err)

	_, err = stmt.Exec(hostIp, hostPort, mIp, mPort, flag, dbsName, comment)
	checkErr(err)
	fmt.Printf("replace into mysql_asset (ip, port, mip, mport, flag, dbs, comment) values (%s, %s, %s, %s, %s, %s, %s) \n", hostIp, hostPort, mIp, mPort, flag, dbsName, comment)
}

//校验ip是否正确函数，本程序没使用，为后期做准备
func checkIp(ip string) bool {
	addr := strings.Trim(ip, " ")
	regStr := `^(([1-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])\.)(([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])\.){2}([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])$`
	if match, _ := regexp.MatchString(regStr, addr); match {
		fmt.Println("true")
		return true
	}
	fmt.Println("false")
	return false
}

// 判断ip是否在16位网段中
func NetContainIP(netIP string, IP string) bool {
	//生产16位子网掩码。
	mask := net.IPv4Mask(byte(255), byte(255), byte(255), byte(0))
	//生成网段地址
	netMask := &net.IPNet{net.ParseIP(netIP), mask}
	//判断ip是否在网段中
	return netMask.Contains(net.ParseIP(IP))

}

//获取本机ip 排除回环地址 dock网卡 排除子网掩码是32位的vip
func localIp() string {
	var ipAddr []string
	//获取所有的网卡。
	netInterfaces, err := net.Interfaces()
	if err != nil {
		fmt.Println("net.Interfaces failed, err", err.Error())
	}
	for i := 0; i < len(netInterfaces); i++ {
		// 排除没有启动的网卡，和以dock开头的网卡（docker启动的网卡）。
		if (netInterfaces[i].Flags&net.FlagUp) != 0 && !strings.Contains(netInterfaces[i].Name, "dock") {
			// 获取网卡地址。
			addrs, _ := netInterfaces[i].Addrs()
			for _, address := range addrs {
				// 排除本机回环地址 和子网掩码为32位的vip
				if ipnet, ok := address.(*net.IPNet); ok && !ipnet.IP.IsLoopback() && !strings.EqualFold(ipnet.Mask.String(), "ffffffff") {
					// 过滤不能转换位ipv4 和192.168.0.0/24 网段的地址。
					//if ipnet.IP.To4() != nil && !NetContainIP("192.168.0.0", ipnet.IP.String()) {
					if ipnet.IP.To4() != nil && (strings.Contains(ipnet.IP.String(), "192.168") || strings.Contains(ipnet.IP.String(), "10.100")) && !NetContainIP("192.168.0.0", ipnet.IP.String()) {
						//if ipnet.IP.To4() != nil {
						ipAddr = append(ipAddr, ipnet.IP.String())
					}
				}
			}

		}
	}
	return strings.Join(ipAddr, ",")
}

//go 调用shell命令函数，返回shell命令返回值
func execShell(s string) (string, error) {
	cmd := exec.Command("/bin/bash", "-c", s)
	var out bytes.Buffer
	cmd.Stdout = &out
	err := cmd.Run()
	checkErr(err)
	return out.String(), err

}

//校验err是否为空如果为空输出错误panic退出
func checkErr(err error) {

	if err != nil {
		fmt.Println(err)
		panic(err)

	}

}

//主函数
func main() {
	//获取本机ip地址
	//hostIp := localIp()
	//获取mysqld进程监听端口
	mysqlPorts, err := execShell("netstat  -lntp |grep -w mysqld|awk '{print $4}'|awk -F ':' '{print $NF}' |sort|uniq")
	//mysqlPorts, err := execShell("netstat  -lntp |grep -w mysqld|awk '{print $4}'|awk -F ':' '{print $NF}' |sort|uniq|grep '3388'")
	checkErr(err)
	//将mysqld端口转换为数组
	mysqlPortsArr := strings.Fields(mysqlPorts)
	//fmt.Println(mysqlPortsArr)
	//循环mysqld端口
	for _, port := range mysqlPortsArr {
		//fmt.Println(port)
		//获取数据库从库ip，数据库名字，dump进程或者slaveio进程数
		hostIp, hostPort, mIp, mPort, flag, schemas, comment := getDBList(port)
		//fmt.Println(hostIp + " " + hostPort + " " + mIp + " " + mPort + " " + flag + " " + strings.Join(schemas, " "))
		if hostIp != "" {
			fmt.Println(comment)
			regDB(hostIp, hostPort, mIp, mPort, flag, schemas, comment)
		}

	}

}
