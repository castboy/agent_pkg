//record.go

package pkg_wmg 

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"os"
)

type Partition struct {
	First   int64
	Current int64
	Last    int64
	Weight  int
    Stop bool
}

var wafVds [2]map[string]Partition

var Waf map[string]Partition
var Vds map[string]Partition
var wafWeightTotal int = 0
var vdsWeightTotal int = 0

func Read(file string) {
	Waf = make(map[string]Partition, 1000)
	Vds = make(map[string]Partition, 1000)
	fileHdl, err := os.OpenFile(file, os.O_RDONLY, 0666)
	if nil != err {
		fmt.Println("openfileError")
	}

	defer fileHdl.Close()

	bytes, err := ioutil.ReadAll(fileHdl)
	if nil != err {
	}

	err = json.Unmarshal(bytes, &wafVds)
	if nil != err {
	}

	Waf = wafVds[0]
	Vds = wafVds[1]

}

func Write(file string) {
	fileHdl, _ := os.OpenFile(file, os.O_WRONLY, 0666)
	bytes, _ := json.Marshal(wafVds)
	io.WriteString(fileHdl, string(bytes))

	fileHdl.Close()
}

func InitData () {
    for _, v := range Waf {
        wafWeightTotal += v.Weight  
    }
    fmt.Println(wafWeightTotal)
    for _, v := range Vds {
        vdsWeightTotal += v.Weight  
    }
}
