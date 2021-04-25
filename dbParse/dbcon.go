package dbParse

import (
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"go_canal_client/confParse"
	"log"
)

type Db struct {
	DB *sql.DB
}

// InitDB Init connect
func (d *Db) InitDB() {
	var yaml confParse.Yaml
	yaml.InitBassConf()
	dsnS := fmt.Sprintf("%v:%v@tcp(%s:%s)/%s?charset=utf8mb4&parseTime=True", yaml.Sources.Username, yaml.Sources.Password, yaml.Sources.Address, yaml.Sources.Port, yaml.Sources.DataName)
	dsnP := fmt.Sprintf("%v:%v@tcp(%s:%s)/%s?charset=utf8mb4&parseTime=True", yaml.Product.Username, yaml.Product.Password, yaml.Product.Address, yaml.Product.Port, yaml.Product.DataName)

	dbS, err := sql.Open("mysql", dsnS)
	dbP, err := sql.Open("mysql", dsnP)

	if err != nil {
		log.Fatalf("init db is failed %v\n", err)
		panic(err)
	}
	//test connect
	err = dbS.Ping()
	err = dbP.Ping()
	fmt.Println("connect success")
	if err != nil {
		panic(err)
	}
	d.DB = dbP
}

// ProcessData 数据写入
func (d *Db) ProcessData(sqlStr string, params []string) error {
	fmt.Printf("sql:--%s  ;params:%s\n",sqlStr,params)
	param:=make([]interface{},0)
	for _, val := range params {
		param=append(param,val)
	}
	_, err := d.DB.Exec(sqlStr, param...)
	if err != nil {
		fmt.Printf("sql exec is failed --%s\n",err)
		return err
	}
	return err
}
