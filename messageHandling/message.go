package messageHandling

import (
	"fmt"
	"github.com/golang/protobuf/proto"
	pbe "github.com/withlin/canal-go/protocol/entry"
	"go_canal_client/confParse"
	"go_canal_client/dbParse"
	"os"
	"sync"
	"time"
)

type MessageHand struct {
	confParse.Yaml
	dbParse.Db
	confParse.Table
	TableName string            //源数据表
	EventType int               //操作类型
	Data      map[string]interface{} //解析得到的字段数据
}

func (m *MessageHand) Init() {
	m.InitBassConf()
	m.InitDB()
	m.InitMap()
}

var wg =sync.WaitGroup{}
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
					err := m.ParseColumn(rowData.GetBeforeColumns())
					if err != nil {
						continue
					}
					err = m.operandData()
					if err != nil {
						continue
					}
				}  else if eventType == pbe.EventType_INSERT {
					err := m.ParseColumn(rowData.GetAfterColumns())
					if err != nil {
						continue
					}
					err = m.operandData()
					if err != nil {
						continue
					}
				} else {
					fmt.Println("-------> after")
					err := m.ParseColumn(rowData.GetAfterColumns())
					if err != nil {
						continue
					}
					err = m.operandData()
					if err != nil {
						continue
					}
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
func (m *MessageHand) ParseColumn(columns []*pbe.Column) error {
	m.Data =*new(map[string]interface{})//重新初始化
	values := make(map[string]interface{}, 0)
	for _, col := range columns {
		fmt.Println(fmt.Sprintf("%s : %s  update= %t", col.GetName(), col.GetValue(), col.GetUpdated()))
		values[col.GetName()] = col.GetValue()
	}
	m.Data = values
	err := m.getTableMap()
	if err != nil {
		return err
	}
	return nil
}

//根据获取的值，执行查询语句
//func (m *MessageHand)getQueryValue()  {
//
//}
//根据配置字段映射关系，将接收到的值赋值给目标表字段
func (m *MessageHand) getTableMap() error{
	//values:=make(map[string]string,0)
	//1. 查询映射关系
	// 1.1 查询源表映射的目标表列表

	//fmt.Println(m.Data)
	if _,ok :=m.MapAll[m.TableName];ok{ //接收到的表名在rdb配置中
		//获取配置中的字段映射关系
		tables := m.MapAll[m.TableName]
		for index, table := range tables { //遍历目标表，获取各自配置映射字段
			wg.Add(1)
			go func(ta *confParse.ToTable,ind int) {
				if ta.IsQuery{ //是否需要查询第三方表的字段
							//将 SourceColumn 中的源表字段 ，同位置替换为值
					params:=make([]interface{},len(ta.SourceColumn))
					for i, sc := range ta.SourceColumn {
						//字段是否在解析获取的值当中
						if _,ok:=m.Data[sc];ok{
							params[i]=m.Data[sc]
						}else {
							fmt.Println("字段设置错误，查询第三方表未发现查询限制映射字段")
							continue
						}
					}
					//执行查询语句
					values, err := m.Db.QueryOtherTable(ta.Sql, params)
					if err != nil || values==nil{
						fmt.Println("查询第三方表没有返回结果")
						//休眠1秒
						time.Sleep(1*time.Second)
						//再次查询
						values, err = m.Db.QueryOtherTable(ta.Sql, params)
						if err!=nil||values==nil {
							fmt.Println("再次查询依然没有")
							return
						}
					}
					ta.Values=values //记录查询到的值
					//将查询到的值添加到 m.Data中，供后续填充参数查值替换
					for i, s := range ta.TargetMapping {
						m.Data[s]=values[i]
					}
				}

				for key, value := range ta.Data { //将sql数据中的填充参数替换为对应的值
					//params:=make([]string,0)
					value.Values=make([]interface{},len(value.Params))
					for i, param := range value.Params { //遍历填充参数
						if _,ok:=m.Data[param];ok{//填充参数在解析数据中
							//根据替换参数将值传递
							//fmt.Println(m.Data)
							value.Values[i]=m.Data[param]
						}
					}
					//sql参数替换为值的形式
					ta.Data[key]=value
				}
				tables[ind]=ta
				wg.Wait()
			}(table,index)
		}
		m.MapAll[m.TableName]=tables
	}
	wg.Done()
	return nil
}
//操作数据,写入数据库
func (m *MessageHand) operandData()error {
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
	return nil
}