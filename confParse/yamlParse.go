package confParse

import (
	"fmt"
	yaml "gopkg.in/yaml.v2"
	"io/ioutil"
	"log"
	"os"
)

type Yaml struct {
	Server struct { //客户端配置
		Address   string `yaml:"address"`   //主机地址
		Port      int    `yaml:"port"`      //端口
		Subscribe string `yaml:"subscribe"` //监听正则关系
	}
	Sources struct { //源数据库配置
		Address  string `yaml:"address"`  //源数据库地址
		Port     string `yaml:"port"`     //端口
		DataName string `yaml:"dataName"` //数据库
		Username string `yaml:"username"` //账号
		Password string `yaml:"password"` //密码
	}
	Product struct { //目标数据库配置
		Address  string `yaml:"address"`  //主机地址
		Port     string `yaml:"port"`     //端口
		DataName string `yaml:"dataName"` //数据库
		Username string `yaml:"username"` //用户账号
		Password string `yaml:"password"` //密码
	}
}

func parseYaml(filePath string, ob interface{}) (obj interface{}, err error) {
	if filePath == "" {
		return nil, nil
	}
	file, err := ioutil.ReadFile(filePath)
	if err != nil {
		return nil, err
	}
	err = yaml.Unmarshal(file, ob)
	if err != nil {
		log.Fatalf("unmarshal is failed :%v\n", err)
		return nil, err
	}
	return ob, err
}
func (y *Yaml) InitBassConf() {
	obj, err := parseYaml("./application.yaml", y)
	if err != nil {
		log.Fatalf("conf init is failed :%v\n", err)
		panic(err)
	}
	toy := obj.(*Yaml)
	y = toy
}

type RdbConfig struct {
	Mapping struct {
		Database      string            `yaml:"database"`      //源数据库
		Table         string            `yaml:"table"`         //源表
		TargetTable   string            `yaml:"targetTable"`   //目标表
		TargetPK      map[string]string `yaml:"targetPK"`      // 主键映射
		MapAll        bool              `yaml:"mapAll"`        //是否全表字段映射
		TargetColumns map[string]string `yaml:"targetColumns"` //字段映射
	}
}

// InitConf 初始化解析配置
func (r *RdbConfig) InitRDBConf() []*RdbConfig {
	//获取指定目录下的所有文件
	dir, err := ioutil.ReadDir("./rdb")
	if err != nil {
		panic(err)
	}
	PthSep := string(os.PathSeparator)
	rdbConfs := make([]*RdbConfig, 0)
	for _, fi := range dir {
		file := "./rdb" + PthSep + fi.Name()
		fmt.Printf("parse %s\n", file)
		r = new(RdbConfig)
		obj, err := parseYaml(file, r)
		if err != nil {
			continue
		}
		toR := obj.(*RdbConfig)
		rdbConfs = append(rdbConfs, toR)
	}
	return rdbConfs
}
