package messageHandling

import (
	"fmt"
	"github.com/withlin/canal-go/client"
	"log"
	"os"
	"testing"
	"time"
)

func TestMessage(t *testing.T) {
	var message MessageHand
	message.Init()
	address := message.Server.Address
	port := message.Server.Port
	// example 替换成-e canal.destinations=example 你自己定义的名字
	connector := client.NewSimpleCanalConnector(address, port, "", "", "example", 60000, 60*60*1000)
	err := connector.Connect()
	if err != nil {
		log.Println(err)
		os.Exit(1)
	}

	// https://github.com/alibaba/canal/wiki/AdminGuide
	//mysql 数据解析关注的表，Perl正则表达式.
	//
	//多个正则之间以逗号(,)分隔，转义符需要双斜杠(\\)
	//
	//常见例子：
	//
	//  1.  所有表：.*   or  .*\\..*
	//	2.  canal schema下所有表： canal\\..*
	//	3.  canal下的以canal打头的表：canal\\.canal.*
	//	4.  canal schema下的一张表：canal\\.test1
	//  5.  多个规则组合使用：canal\\..*,mysql.test1,mysql.test2 (逗号分隔)

	//err = connector.Subscribe(".*\\..*")
	subscribe := message.Server.Subscribe
	connector.Subscribe(subscribe)
	if err != nil {
		log.Println(err)
		os.Exit(1)
	}

	for {

		m, err := connector.Get(100, nil, nil)
		if err != nil {
			log.Println(err)
			os.Exit(1)
		}
		batchId := m.Id
		if batchId == -1 || len(m.Entries) <= 0 {
			time.Sleep(300 * time.Millisecond)
			fmt.Println("===没有数据了===")
			continue
		}

		message.ParseEntry(m.Entries)
		fmt.Printf("%v",message.MapAll)
	}
}
