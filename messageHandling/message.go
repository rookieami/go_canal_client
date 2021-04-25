package messageHandling

import (
	"fmt"
	"github.com/golang/protobuf/proto"
	pbe "github.com/withlin/canal-go/protocol/entry"
	"go_canal_client/confParse"
	"go_canal_client/dbParse"
	"os"
)

type MessageHand struct {
	confParse.Yaml
	dbParse.Db
	dbParse.Table
	TableName string            //源数据表
	EventType int               //操作类型
	Data      map[string]string //解析得到的字段数据
}

func (m *MessageHand) Init() {
	m.InitBassConf()
	m.InitDB()
	m.InitMap()
}
func (m *MessageHand) ParseEntry(entrys []pbe.Entry) error {
	for _, entry := range entrys {
		if entry.GetEntryType() == pbe.EntryType_TRANSACTIONBEGIN || entry.GetEntryType() == pbe.EntryType_TRANSACTIONEND {
			continue
		}
		rowChange := new(pbe.RowChange)
		err := proto.Unmarshal(entry.GetStoreValue(), rowChange)
		checkError(err)
		if rowChange != nil {
			eventType := rowChange.GetEventType()
			header := entry.GetHeader()

			fmt.Println(fmt.Sprintf("================> binlog[%s : %d],name[%s,%s], eventType: %s", header.GetLogfileName(), header.GetLogfileOffset(), header.GetSchemaName(), header.GetTableName(), header.GetEventType()))
			m.TableName = header.GetSchemaName() + "." + header.GetTableName()
			m.EventType = int(eventType)
			for _, rowData := range rowChange.GetRowDatas() {
				if eventType == pbe.EventType_DELETE {
					m.ParseColumn(rowData.GetBeforeColumns())
					m.operandData()
				}  else if eventType == pbe.EventType_INSERT {
					m.ParseColumn(rowData.GetAfterColumns())
					m.operandData()
				} else {
					fmt.Println("-------> after")
					m.ParseColumn(rowData.GetAfterColumns())
					m.operandData()
				}
			}
		}
	}
	return nil
}
func checkError(err error) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "Fatal error: %s", err.Error())
		os.Exit(1)
	}
}

// ParseColumn 解析接收到到源表变化操作
func (m *MessageHand) ParseColumn(columns []*pbe.Column) {
	m.Data =*new(map[string]string)//重新初始化
	values := make(map[string]string, 0)
	for _, col := range columns {
		fmt.Println(fmt.Sprintf("%s : %s  update= %t", col.GetName(), col.GetValue(), col.GetUpdated()))
		values[col.GetName()] = col.GetValue()
	}
	m.Data = values
	m.getTableMap()
}

//根据配置字段映射关系，将接收到的值赋值给目标表字段
func (m *MessageHand) getTableMap() {
	//values:=make(map[string]string,0)
	//1. 查询映射关系
	// 1.1 查询源表映射的目标表列表

	//fmt.Println(m.Data)
	if _,ok :=m.MapAll[m.TableName];ok{ //接收到的表名在rdb配置中
		//获取配置中的字段映射关系
		tables := m.MapAll[m.TableName]
		for index, table := range tables { //遍历目标表，获取各自配置映射字段
			//for key, value := range table.ColumnMap {	//遍历映射字段
			//	if _,ok:=m.Data[value];ok { //如果配置映射的源表字段存在于解析接收到的字段表中
			//		//将映射格式 字段：源表字段 更新为 字段：值
			//		//赋值
			//		table.ColumnMap[key]=m.Data[value]
			//	}
			//}
			for key, value := range table.Data { //将sql数据中的填充参数替换为对应的值
				//params:=make([]string,0)
				value.Values=make([]string,len(value.Params))
				for i, param := range value.Params { //遍历填充参数
					if _,ok:=m.Data[param];ok{//填充参数在解析数据中
						//根据替换参数将值传递
						//fmt.Println(m.Data)
						value.Values[i]=m.Data[param]
					}

				}
				//sql参数替换为值的形式
				table.Data[key]=value
			}
			tables[index]=table
		}
		m.MapAll[m.TableName]=tables
	}
}
//操作数据,写入数据库
func (m *MessageHand) operandData() {
	eventType:=m.EventType
	for _, table := range m.MapAll[m.TableName] {
		sql:=table.Data[eventType].Sql
		values:=table.Data[eventType].Values
		err := m.Db.ProcessData(sql,values)
		if err != nil {
			fmt.Println("operand data is failed",err)
			continue
		}
	}

}