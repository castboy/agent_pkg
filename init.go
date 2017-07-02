//init.go

package agent_pkg

import (
	"encoding/json"
	"fmt"
)

type Status struct {
	First   int64
	Engine int64
	Cache int64
	Last    int64
	Weight  int
}

var WafVds [2]map[string]Status

var Waf map[string]Status
var Vds map[string]Status

func InitWafVds() {
	Waf = make(map[string]Status, 1000)
	Vds = make(map[string]Status, 1000)
    
    wafTopic := AgentConf.Topic[0]
    vdsTopic := AgentConf.Topic[1]

    Waf[wafTopic] = Status{0, 0, 0, 0, 10}
    Vds[vdsTopic] = Status{0, 0, 0, 0, 10}

    WafVds[0] = Waf
    WafVds[1] = Vds

    fmt.Println("Init-Status : ", WafVds)
}

func UpdateWafVds(status []byte) {
	Waf = make(map[string]Status, 1000)
	Vds = make(map[string]Status, 1000)
    err := json.Unmarshal(status, &WafVds)
    if err != nil {
        fmt.Println("UpdateWafVds Err")
    }

	Waf = WafVds[0]
	Vds = WafVds[1]

    Waf["xdrHttp"] = Status{403279,403279,403279,403279, 1}
    Vds["xdrFile"] = Status{132271,132271,132271,132271, 1}

    PrintUpdateStatus()
}

func PrintUpdateStatus() {
    fmt.Println("\n\nUpdateStatus:")

    fmt.Println("Waf")
    for key, val := range Waf {
        fmt.Println(key, "     ", val)
    }

    fmt.Println("\nVds")
    for key, val := range Vds {
        fmt.Println(key, "     ", val)
    }
}
