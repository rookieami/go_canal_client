package dbParse

import (
	"fmt"
	"go_canal_client/confParse"
)

//解析映射关系，生成sql语句

type Table struct {
	MapAll map[string][]*ToTable
}
type ToTable struct {
	PKMap     map[string]string //主键映射
	ColumnMap map[string]string //字段映射关系
	Data      map[int]SqlData   //数据操作语句
}
type SqlData struct {
	Sql    string   //数据操作语句 sql:params
	Params []string //填充参数
	Values []string //参数对应数值
}

func (t *Table) InitMap() {
	var r confParse.RdbConfig
	ri := r.InitRDBConf()
	t.MapAll = make(map[string][]*ToTable, 0)
	for _, v := range ri {
		t1 := new(ToTable)
		rest := t1.Init(v)
		table := v.Mapping.Database + "." + v.Mapping.Table
		t.MapAll[table] = append(t.MapAll[table], rest)
	}
}
func (t1 *ToTable) Init(config *confParse.RdbConfig) *ToTable {
	t1.PKMap = config.Mapping.TargetPK
	t1.ColumnMap = config.Mapping.TargetColumns
	table := config.Mapping.TargetTable
	//提取操作字段
	t1.Data = make(map[int]SqlData, 0)
	keys, vals := parseMap(t1.ColumnMap)
	columns, params ,update:= getColumns(keys)
	t1.getInsert(table, columns, params,vals,update)
	uCol := getColumnsSet(keys)    //更新字段
	pks, pkv := parseMap(t1.PKMap) //主键
	uPk := getColumnsSet(pks)
	vals = append(vals, pkv...) //更新字段+主键
	t1.getUpdate(table, uCol, uPk, vals)
	t1.getDel(table, uPk, pkv)
	return t1
}

//提取映射关系键值,将源数据操作解析出的字段映射写入到目标表字段中
func parseMap(maps map[string]string) ([]string, []string) {
	keys := make([]string, 0)
	vals := make([]string, 0) //同步取出字段，保证位置一致
	for k, v := range maps {
		keys = append(keys, k)
		vals = append(vals, v)
	}
	return keys, vals
}

//提取主键映射关系
func parsePK(maps map[string]interface{}) {
	return
}

//提取字段映射关系
func parseColumn(maps map[string]interface{}) {
	return
}

//生成sql操作字段
func getColumns(keys []string) (string, string,string) {
	str := ""
	i := 0
	params := ""
	updateStr:=""
	for _, key := range keys {
		if i > 0 {
			str += ","
			params += ","
			updateStr+=","
		}
		str += fmt.Sprintf(" `%s`", key)
		params += "?"
		updateStr+=fmt.Sprintf(" `%s`=values(`%s`)", key,key)
		i += 1
	}
	return str, params,updateStr
}

//生成字段设置关系
func getColumnsSet(keys []string) string {
	str := ""
	i := 0
	for _, key := range keys {
		if i > 0 {
			str += ","
		}
		str += fmt.Sprintf(" `%s`=?", key)
		i += 1
	}
	return str
}

func (t1 *Table) getSql(map[string]interface{}) {

}

//生成插入语句
func (t1 *ToTable) getInsert(tableName string, columns string,params string,vals []string,update string, ) {
	sql := fmt.Sprintf("insert into %s (%s)values(%s) on duplicate key update %s", tableName, columns, params,update)
	var sqlData SqlData
	sqlData.Sql = sql
	sqlData.Params = vals
	t1.Data[1] = sqlData
}

//生成更新语句
func (t1 *ToTable) getUpdate(tableName string, columns string, params string, vals []string) {
	sql := fmt.Sprintf("update %s set %s where %s", tableName, columns, params)
	var sqlData SqlData
	sqlData.Sql = sql
	sqlData.Params = vals
	t1.Data[2] = sqlData
}

//生成删除语句
func (t1 *ToTable) getDel(tableName string, params string, vals []string) {
	sql := fmt.Sprintf("delete from %s where %s", tableName, params)
	var sqlData SqlData
	sqlData.Sql = sql
	sqlData.Params = vals
	t1.Data[3] = sqlData
}
