package confParse

import (
	"fmt"
	"go_canal_client/common"
)

// ToTable 单表映射配置解析
type ToTable struct {
	PKMap     map[string]string //主键映射
	ColumnMap map[string]string //字段映射关系
	Data      map[int]SqlData   //数据操作语句
	IsQuery bool //是否查询第三方表
	Query
}
type SqlData struct {
	Sql    string   //数据操作语句 sql:params
	Params []string //填充参数
	Values []interface{} //参数对应数值
}
// Query 需要到第三方查询的信息
type Query struct {
	Target []string //目标表写入字段
	TargetMapping []string//目标表映射字段
	Values []interface{} //第三方表字段查询得到的值
	Sql string   //第三方表查询语句
	SourceColumn []string  //查询时，源表字段
}
// GenerateSql 生成查询sql语句
func (q *Query) GenerateSql(config *RdbConfig)  {
	tableName:=config.Mapping.QueryTable  //获取查询表名
	columns:=config.Mapping.QueryColumns  //查询字段映射
	whereColumns:=config.Mapping.WhereColumns //限制字段映射
	//解析map，得到需要查询字段
	targetColumn, query := common.ParseMapping(columns) //目标写入字段 ，查询字段
	//解析map,得到查询条件字段
	w, source := common.ParseMapping(whereColumns) //查询where字段 ，源字段
	sql, whereSql := common.GeneraSql(query, w)
	q.Sql=sql+"from "+tableName+whereSql
	q.SourceColumn=source
	q.Target=targetColumn
	q.TargetMapping=common.AddTableNameToColumn(tableName,w)

}

func (t1 *ToTable) Init(config *RdbConfig)  {
	t1.PKMap = config.Mapping.TargetPK
	t1.ColumnMap = config.Mapping.TargetColumns
	table := config.Mapping.TargetTable
	//提取操作字段
	t1.Data = make(map[int]SqlData, 0)
	keys, vals := common.ParseMapping(t1.ColumnMap)
	//是否需要查询第三方表,来保证当前目标表指定字段数据与第三方表字段数据一致
	isQuery := config.Mapping.IsQuery
	if isQuery { //需要查询
		t1.IsQuery=true
		t1.Query.GenerateSql(config)
		keys= append(keys, t1.Query.Target...)
		vals=append(vals,t1.TargetMapping...)
	}

	columns, params ,update:= common.GetColumns(keys)
	t1.getInsert(table, columns, params,vals,update)
	uCol := common.GetColumnsSet(keys)    //更新字段
	pks, pkv :=common.ParseMapping(t1.PKMap) //主键
	uPk := common.GetColumnsSet(pks)
	vals = append(vals, pkv...) //更新字段+主键
	t1.getUpdate(table, uCol, uPk, vals)
	t1.getDel(table, uPk, pkv)
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


type Table struct {
	MapAll map[string][]*ToTable
}
func (t *Table) InitMap() {
	var r RdbConfig
	ri := r.InitRDBConf()
	t.MapAll = make(map[string][]*ToTable, 0)
	for _, v := range ri {
		t1 := new(ToTable)
		t1.Init(v)
		table := v.Mapping.Database + "." + v.Mapping.Table
		t.MapAll[table] = append(t.MapAll[table], t1)
	}
}