//record.go

package pkg_wmg 

import (
	"encoding/json"
	"fmt"
	//"io"
	"io/ioutil"
	"os"
)

type Partition struct {
	First   int64
	Engine int64
	Cache int64
	Last    int64
	Weight  int
    Stop bool
}


var wafVds [2]map[string]Partition

var Waf map[string]Partition
var Vds map[string]Partition
var Ptr *map[string]Partition

func Read(file string) {
	Waf = make(map[string]Partition, 1000)
	Vds = make(map[string]Partition, 1000)
	fileHdl, err := os.OpenFile(file, os.O_RDONLY, 0666)
	if nil != err {
		fmt.Println("openfileerror")
	}

	defer fileHdl.Close()

	bytes, err := ioutil.ReadAll(fileHdl)
	if nil != err {
	}

	err = json.Unmarshal(bytes, &wafVds)
	if nil != err {
	}
}

func InitWafVds() {
	Waf = wafVds[0]
	Vds = wafVds[1]
}

