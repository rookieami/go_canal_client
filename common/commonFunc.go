package common

import "fmt"

//一些公共使用的方法

// ParseMapping 提取键值对，保证位置对应
func ParseMapping(maps map[string]string) ([]string, []string) {
	keys := make([]string, 0)
	vals := make([]string, 0) //同步取出字段，保证位置一致
	for k, v := range maps {
		keys = append(keys, k)
		vals = append(vals, v)
	}
	return keys, vals
}
func GeneraSql(columns []string,where []string)(sql string,whereSql string){
	sql ="select "
	index:=0
	for _, col:= range columns {
		if index>0 {
			sql+=","
		}
		sql+= fmt.Sprintf("`%s`", col)
		index+=1
	}
	index=0
	whereSql=" where "
	for _, w := range where {
		if index>0 {
			whereSql+=" and "
		}
		whereSql+=fmt.Sprintf("`%s`=?",w)
	}
	return sql,whereSql
}

// GetColumns 生成sql操作字段
func GetColumns(keys []string) (string, string,string) {
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

// GetColumnsSet 生成字段设置关系
func GetColumnsSet(keys []string) string {
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

// AddTableNameToColumn 字段前添加表名
func AddTableNameToColumn(name string,columns[]string)(tableColumns []string){
	for _, column := range columns {
		newCol:=fmt.Sprintf("%s.%s",name,column)
		tableColumns = append(tableColumns, newCol)
	}
	return
}