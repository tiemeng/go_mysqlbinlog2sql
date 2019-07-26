package main

import (
	"fmt"
	"io/ioutil"
	"regexp"
	"strings"
)

//import (
//	"database/sql"
//	"fmt"
//	"strings"
//	_ "github.com/go-sql-driver/mysql"
//)
//
////数据库配置
//const (
//	userName = "root"
//	password = "123456"
//	ip = "127.0.0.1"
//	port = "3306"
//	dbName = "information_schema"
//)
////Db数据库连接池
//var DB *sql.DB
//var err error
//
////注意方法名大写，就是public
//func InitDB()  {
//	//构建连接："用户名:密码@tcp(IP:端口)/数据库?charset=utf8"
//	path := strings.Join([]string{userName, ":", password, "@tcp(",ip, ":", port, ")/", dbName, "?charset=utf8"}, "")
//
//	//打开数据库,前者是驱动名，所以要导入： _ "github.com/go-sql-driver/mysql"
//	DB, err = sql.Open("mysql", path)
//	if err!=nil{
//		fmt.Println(err)
//		return
//	}
//
//	//设置数据库最大连接数
//	DB.SetConnMaxLifetime(100)
//	//设置上数据库最大闲置连接数
//	DB.SetMaxIdleConns(10)
//	//验证连接
//	if err := DB.Ping(); err != nil{
//		fmt.Println("opon database fail")
//		return
//	}
//	//fmt.Println("connnect success")
//}
//
//func SelectUserById(tablename string, dbname string) ([]string) {
//	var a[]string
//	rows,err := DB.Query("SELECT COLUMN_NAME FROM COLUMNS WHERE TABLE_NAME = ? and TABLE_SCHEMA=?", tablename,dbname)
//	if err != nil{
//		fmt.Println("查询出错了")
//	}
//	for rows.Next(){
//		var feild string
//		rows.Scan(&feild)
//		a = append(a, feild)
//	}
//	return a
//}


func ReadFile(filePath string) []byte{
	content, err := ioutil.ReadFile(filePath)
	if err != nil {
		fmt.Println("Read error")
	}
	return content
}


func main() {
	//var File string
	//var Binlog_Do_DB string
	//var Binlog_Ignore_DB string
	//var Position string
	//var Executed_Gtid_Set string
	//var varName string
	//var value string
	//InitDB()
	//DB.QueryRow("SHOW master status").Scan(&File,&Position,&Binlog_Do_DB,&Binlog_Ignore_DB,&Executed_Gtid_Set)
	//DB.QueryRow("show variables like \"log_bin_basename\"").Scan(&varName,&value)
	//fmt.Println(File)
	//fmt.Println(value)
	////= DB.Query("show variables like \"log_bin_basename\"")
	//rows,err :=DB.Exec("show variables like \"log_bin_basename\"")
	//if err!=nil{
	//	fmt.Println(err)
	//}
	//for rows.Next(){
	//	var field interface{}
	//	rows.Scan(&field)
	//	fmt.Println(field)
	//}
	//fmt.Printf("%v",rows)
	//var name string
	//flag.StringVar(&name,"name","123","--name name")
	//flag.Parse()
	//str := name[1:strings.LastIndex(name,"/")+1]
	//fmt.Println(str)
	//str := `UPDATE table
 //WHERE
 //  id=1
 //  username='admin'
 //  password='dd94709528bb1c83d08f3088d4043f4742891f4f'
 //  nickname='Admin管理员'
 //  lastlogin='2016-10-23 12:51:22'
 //  lastip='192.168.244.2'
 //  blocking=0
 //  createtime='2016-09-18 22:13:40'
 //  lastupdate=1477227087
 //SET
 //  id=1
 //  username='admin'
 //  password='dd94709528bb1c83d08f3088d4043f4742891f4f'
 //  nickname='Admin管理员'
 //  lastlogin='2019-07-08 14:33:44'
 //  lastip='127.0.0.1'
 //  blocking=0
 //  createtime='2016-09-18 22:13:40'
 //  lastupdate=1562567624`
	//fmt.Println(str[0:strings.LastIndex(str,"WHERE")])
	//fmt.Println(regexp.MustCompile("\\s+").ReplaceAllString(strings.Replace(strings.Replace(str[strings.LastIndex(str,"WHERE"):strings.LastIndex(str,"SET")],"WHERE\n","WHERE",-1),"\n"," AND",-1)," "))
	//fmt.Println(regexp.MustCompile("\\s+").ReplaceAllString(strings.Replace(strings.Replace(str[strings.LastIndex(str,"SET"):],"SET\n","SET ",-1),"\n",",",-1)," "))
	//re3, _ := regexp.Compile(`\n`)
	//rep := re3.ReplaceAllString(str, ",")
	//fmt.Println(rep);
	str := ReadFile("/tmp/t.log")
	reg := regexp.MustCompile(`(?msi:DELETE [a-z]+[a-z].*?# at)+`)
	arr := reg.FindAllString(string(str),-1)
	for _,v := range arr{
		v = strings.Replace(v, "\n### WHERE\n", " WHERE ", -1)
		v = strings.Replace(v,"###", "", -1)
		v = strings.Replace(v,"# at", "", -1)
		t := regexp.MustCompile("(`.+`\\.`.+`)")
		dbTable := strings.Replace(string(t.Find([]byte(v))), "`", "", -1)
		v = strings.Replace(v, "\n", " AND ", -1)
		v = strings.Replace(v, " AND  DELETE FROM ", " \n DELETE FROM ", -1)
		fmt.Println(dbTable)
		fmt.Println(v)
		fmt.Println("===========")
	}
}
