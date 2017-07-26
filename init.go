//init.go

package agent_pkg

import (
	"encoding/json"
	"fmt"
)

type Status struct {
	First  int64
	Engine int64
	Err    int64
	Cache  int64
	Last   int64
	Weight int
}

var WafVds [3]map[string]Status

var status = make(map[string]map[string]Status)

func InitWafVds() {
	wafTopic := AgentConf.Topic[0]
	vdsTopic := AgentConf.Topic[1]

	InitStatusMap()

	status["waf"][wafTopic] = Status{0, 0, 0, 0, -1, 1}

	status["vds"][vdsTopic] = Status{0, 0, 0, 0, -1, 1}

	fmt.Println("Init-Status : ", WafVds)
}

func InitStatusMap() {
	status["waf"] = make(map[string]Status)
	status["vds"] = make(map[string]Status)
	status["rule"] = make(map[string]Status)
}

func UpdateWafVds(bytes []byte) {
	err := json.Unmarshal(bytes, &WafVds)
	if err != nil {
		fmt.Println("UpdateWafVds Err")
	}

	InitStatusMap()

	status["waf"] = WafVds[0]
	status["vds"] = WafVds[1]
	status["rule"] = WafVds[2]

	PrintUpdateStatus()
}

func PrintUpdateStatus() {
	fmt.Println("\n\nUpdateStatus:")

	fmt.Println("Waf")
	for key, val := range status["waf"] {
		fmt.Println(key, "     ", val)
	}

	fmt.Println("\nVds")
	for key, val := range status["vds"] {
		fmt.Println(key, "     ", val)
	}
}
