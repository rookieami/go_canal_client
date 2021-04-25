package confParse

import (
	"fmt"
	"testing"
)

func TestInit(t *testing.T) {
	var c Yaml
	c.InitBassConf()
	fmt.Println(c.Sources.Username)

	//var r RdbConfig
	//ri := r.InitConf()
	//for _, v := range ri {
	//	fmt.Println(v.Mapping)
}
