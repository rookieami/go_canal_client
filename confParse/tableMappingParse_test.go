package confParse

import (
	"fmt"
	"testing"
)

func TestColumn(t *testing.T) {
	var r Table
	r.InitMap()
	fmt.Printf("%v", r.MapAll)
	for key, tables := range r.MapAll {
		fmt.Println(key)
		for _, table := range tables {
			fmt.Println(table)

		}

	}
}
