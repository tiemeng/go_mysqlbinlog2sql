package main

import (
	"database/sql"
	"flag"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"io/ioutil"
	"os"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"sync"
)

var DB *sql.DB
var err error

func InitDB(userName string, password string, ip string, port string, dbName string) (error) {
	//构建连接："用户名:密码@tcp(IP:端口)/数据库?charset=utf8"
	path := strings.Join([]string{userName, ":", password, "@tcp(", ip, ":", port, ")/", dbName, "?charset=utf8"}, "")

	//打开数据库,前者是驱动名，所以要导入： _ "github.com/go-sql-driver/mysql"
	DB, err = sql.Open("mysql", path)
	if err != nil {
		return err
	}

	//设置数据库最大连接数
	DB.SetConnMaxLifetime(100)
	//设置上数据库最大闲置连接数
	DB.SetMaxIdleConns(10)
	//验证连接
	if err := DB.Ping(); err != nil {
		return err
	}
	return nil
}

func getFields(tablename string, dbname string) ([]string) {
	var a []string
	rows, err := DB.Query("SELECT COLUMN_NAME FROM COLUMNS WHERE TABLE_NAME = ? and TABLE_SCHEMA=?", tablename, dbname)
	if err != nil {
		fmt.Println("查询出错了")
	}
	for rows.Next() {
		var feild string
		rows.Scan(&feild)
		a = append(a, feild)
	}
	return a
}

func getBinLogPath() (string) {
	var BinLogPath string
	var Name string
	err = DB.QueryRow("SHOW VARIABLES LIKE \"log_bin_basename\"").Scan(&Name, &BinLogPath)
	if err != nil {
		panic(err)
	}
	BinLogPath = BinLogPath[0 : strings.LastIndex(BinLogPath, "/")+1]
	return BinLogPath
}

func ExecCommand(strCommand string) (string, error) {
	cmd := exec.Command("/bin/bash", "-c", strCommand)

	stdout, _ := cmd.StdoutPipe()
	if err := cmd.Start(); err != nil {
		fmt.Println("Execute failed when Start:" + err.Error())
		return "", err
	}

	out_bytes, _ := ioutil.ReadAll(stdout)
	stdout.Close()

	if err := cmd.Wait(); err != nil {
		fmt.Println("Execute failed when Wait:" + err.Error())
		return "", err
	}
	return string(out_bytes), nil
}

func writeFile(data string, f os.File, f1 os.File, reg string, flag string) {
	dataReg := regexp.MustCompile(reg)
	dataArr := dataReg.FindAllString(data, -1)
	fmt.Println(flag+" 正在处理中。。。。。。")
	var sqlSentence string
	var s1 string
	for index, v := range dataArr {
		if flag == "insert" || flag == "delete" {
			if flag == "insert" {
				sqlSentence = strings.Replace(v, "\n### SET\n", " SET ", -1)

			} else {
				sqlSentence = strings.Replace(v, "\n### WHERE\n", " WHERE ", -1)
			}
			sqlSentence = strings.Replace(sqlSentence, "###", "", -1)
			sqlSentence = strings.Replace(sqlSentence, "# at", "", -1)
			if flag == "insert" {

				sqlSentence = strings.Replace(sqlSentence, "\n", ",", -1)
				sqlSentence = strings.Replace(sqlSentence, ", INSERT INTO", "; INSERT INTO", -1)
			} else {

				sqlSentence = strings.Replace(sqlSentence, "\n", " AND ", -1)
				sqlSentence = strings.Replace(sqlSentence, "AND  DELETE FROM", "; DELETE FROM", -1)
				sqlSentence = sqlSentence[0 : len(sqlSentence)-4]
			}
			sqlSentence = regexp.MustCompile(`\s+`).ReplaceAllString(sqlSentence, " ")
			//sqlSentence = strings.Replace(v, "DELETE FROM", "\nDELETE FROM", -1)
			sqlSentence = sqlSentence[0 : len(sqlSentence)-1]
			t := regexp.MustCompile("(`\\w+`\\.`\\w+`*?)")
			dbTable := strings.Replace(string(t.Find([]byte(sqlSentence))), "`", "", -1)
			db := strings.Split(dbTable, ".")[0]
			table := strings.Split(dbTable, ".")[1]
			arr := getFields(table, db)
			length := len(arr)
			if length <= 0 {
				continue
			}
			for k, _ := range arr {
				sqlSentence = strings.Replace(sqlSentence, "@"+strconv.Itoa(length-k), arr[length-k-1], -1)
			}
			if flag == "delete" {
				s1 = strings.Replace(sqlSentence, "DELETE FROM", "INSERT INTO", -1)
				s1 = strings.Replace(s1, "WHERE", "SET", -1)
				s1 = strings.Replace(s1, "AND", ",", -1)
				//sqlSentence += "\nDELETE反向解析后的sql==========\n" + s1 + "\n=============\n"
			} else {
				s1 = strings.Replace(sqlSentence, "INSERT INTO", "DELETE FROM", -1)
				s1 = strings.Replace(s1, "SET", "WHERE", -1)
				s1 = strings.Replace(s1, ",", " AND", -1)
				//sqlSentence += "\nINSERT反向解析后的sql==========\n" + s1 + "\n=============\n"
			}

		} else if flag == "update" {
			sqlSentence = strings.Replace(v, "###", "", -1)
			sqlSentence = strings.Replace(sqlSentence, "# at", "", -1)
			sqlSentence = strings.Replace(sqlSentence, "/*!*/;", "", -1)
			where := regexp.MustCompile("\\s+").ReplaceAllString(strings.Replace(strings.Replace(sqlSentence[strings.LastIndex(sqlSentence, "WHERE"):strings.LastIndex(sqlSentence, "SET")], "WHERE\n", "WHERE", -1), "\n", " AND", -1), " ")
			where = where[0 : len(where)-4]
			where1 := regexp.MustCompile("\\s+").ReplaceAllString(strings.Replace(strings.Replace(sqlSentence[strings.LastIndex(sqlSentence, "WHERE"):strings.LastIndex(sqlSentence, "SET")], "WHERE\n", "SET", -1), "\n", " ,", -1), " ")
			where1 = where1[0 : len(where1)-2]
			upTable := sqlSentence[0:strings.LastIndex(sqlSentence, "WHERE")]
			set := regexp.MustCompile("\\s+").ReplaceAllString(strings.Replace(strings.Replace(sqlSentence[strings.LastIndex(sqlSentence, "SET"):], "SET\n", "SET", -1), "\n", ",", -1), " ")
			set = set[0 : len(set)-1]
			set1 := regexp.MustCompile("\\s+").ReplaceAllString(strings.Replace(strings.Replace(sqlSentence[strings.LastIndex(sqlSentence, "SET"):], "SET\n", "WHERE", -1), "\n", " AND", -1), " ")
			set1 = set1[0 : len(set1)-4]
			sqlSentence = upTable + " " + set + " " + where
			s1 = upTable + " " + where1 + " " + set1
			t := regexp.MustCompile("`.+`\\.`.+`*?")
			dbTable1 := strings.Replace(string(t.Find([]byte(sqlSentence))), "`", "", -1)
			db1 := strings.Split(dbTable1, ".")[0]
			table1 := strings.Split(dbTable1, ".")[1]
			arr := getFields(table1, db1)
			lenth := len(arr)
			if lenth <= 0 {
				continue
			}
			for k1, _ := range arr {
				sqlSentence = strings.Replace(sqlSentence, "@"+strconv.Itoa(lenth-k1), arr[lenth-k1-1], -1)
				s1 = strings.Replace(s1, "@"+strconv.Itoa(lenth-k1), arr[lenth-k1-1], -1)
			}
		} else if flag == "create" {
			sqlSentence = strings.Replace(v, "/*!*/;", "", -1)
			s1 = ""
		} else {
			sqlSentence = strings.Replace(v, "\n", "", -1)
			sqlSentence = strings.Replace(sqlSentence, "/*!*/;", ";", -1)
			s1 = ""
		}
		f1.WriteString("(" + flag + strconv.Itoa(index+1) + ")\t" + s1)
		f1.WriteString("\n")
		f.WriteString("(" + flag + strconv.Itoa(index+1) + ")\t" + sqlSentence)
		f.WriteString("\n")

	}
	wg.Done()

}

var wg sync.WaitGroup

func main() {
	var binLogPath string
	var binLogName string
	var mysqlHost string
	var mysqlPort string
	var mysqlUser string
	var mysqlPass string
	var saveFile string
	flag.StringVar(&binLogName, "bin-log-name", "", "--bin-log-name bin-log文件名称")
	flag.StringVar(&mysqlPort, "P", "3306", "-P 数据库端口号")
	flag.StringVar(&mysqlHost, "h", "127.0.0.1", "-h 数据库IP")
	flag.StringVar(&mysqlUser, "u", "root", "-u 数据库用户名")
	flag.StringVar(&mysqlPass, "p", "123456", "-p 数据库密码")
	flag.StringVar(&saveFile, "save-path", "./bin2sql.sql", "-save-path 保存解析后的文件")
	flag.Parse()
	if binLogName == "" {
		fmt.Println("请输入binlog文件名")
		os.Exit(-1)
	}
	connectStatus := InitDB(mysqlUser, mysqlPass, mysqlHost, mysqlPort, "information_schema")
	if connectStatus != nil {
		fmt.Println(connectStatus)
		os.Exit(-3)
	}
	binLogPath = getBinLogPath() + binLogName
	if _, err := os.Stat(binLogPath); os.IsNotExist(err) {
		fmt.Println(binLogPath + "：文件不存在")
		os.Exit(-2)
	}
	strData, err1 := ExecCommand("mysqlbinlog " + binLogPath + " -v")
	if err1 != nil {
		fmt.Println(err1)
		os.Exit(-4)
	}
	f, err := os.Create(saveFile)
	f1, err1 := os.Create("./tun.sql")
	defer f.Close()
	defer f1.Close()
	if err != nil {
		fmt.Println(err)
	}
	if err1 != nil {
		fmt.Println(err1)
	}
	sqlArr := map[string]string{
		"create": `(?msi:CREATE TABLE [a-z]+[a-z].*?/*!*/;)+`,
		"insert": `(?msi:INSERT [a-z]+[a-z].*?# at)+`,
		"delete": `(?msi:DELETE [a-z]+[a-z].*?# at)+`,
		"update": "(?msi:UPDATE `.[a-z].*?# at)+",
		"alter":  `(?msi:ALTER [a-z]+((\s+))[a-z].*?/*!*/;)+`,
	}
	for k, v := range sqlArr {
		wg.Add(1)
		go writeFile(strData, *f, *f1, v, k)
	}
	wg.Wait()
	fmt.Println("解析完成，binlog解析sql保存文件为：" + saveFile)
	fmt.Println("\t 反解析后的文件保存为：：./tun.sql")
}
