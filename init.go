//init.go

package agent_pkg

import (
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/widuu/goini"
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
var Waf = make(map[string]Status)
var Vds = make(map[string]Status)

var status = make(map[string]map[string]Status)

func InitWafVds() {
	wafTopic := AgentConf.Topic[0]
	vdsTopic := AgentConf.Topic[1]

	OnlineWeightAndOffset(wafTopic, vdsTopic, "initOnlineWeight",
		"initOnlineWeight", "initOnlineOffset", "initOnlineOffset")

	fmt.Println("Init-Status : ", WafVds)
}

func UpdateWafVds(status []byte) {
	err := json.Unmarshal(status, &WafVds)
	if err != nil {
		fmt.Println("UpdateWafVds Err")
	}

	//	status["waf"] = WafVds[0]
	//	status["vds"] = WafVds[1]

	conf := goini.SetConfig("conf.ini")
	wafTopic := conf.GetValue("onlineTopic", "waf")
	vdsTopic := conf.GetValue("onlineTopic", "vds")

	OnlineWeightAndOffset(wafTopic, vdsTopic, "updateOnlineWeight",
		"updateOnlineWeight", "updateOnlineOffset", "updateOnlineOffset")

	PrintUpdateStatus()
}

func OnlineWeightAndOffset(wafTopic, vdsTopic, wafWeight, vdsWeight, wafOffset, vdsOffset string) {
	conf := goini.SetConfig("conf.ini")
	wafOnlineWeight, _ := strconv.Atoi(conf.GetValue(wafWeight, "waf"))
	vdsOnlineWeight, _ := strconv.Atoi(conf.GetValue(vdsWeight, "vds"))

	wafOnlineOffset, _ := strconv.ParseInt(conf.GetValue(wafOffset, "waf"), 10, 64)
	vdsOnlineOffset, _ := strconv.ParseInt(conf.GetValue(vdsOffset, "vds"), 10, 64)

	status["waf"] = make(map[string]Status)
	status["vds"] = make(map[string]Status)
	status["rule"] = make(map[string]Status)

	if -1 == wafOnlineOffset {
		status["waf"][wafTopic] = Status{0, 0, 0, 0, 0, wafOnlineWeight}
	} else {
		status["waf"][wafTopic] = Status{0, wafOnlineOffset, 0, 0, 0, wafOnlineWeight}
	}

	if -1 == vdsOnlineOffset {
		status["vds"][vdsTopic] = Status{0, 0, 0, 0, 0, vdsOnlineWeight}
	} else {
		status["vds"][vdsTopic] = Status{0, vdsOnlineOffset, 0, 0, 0, vdsOnlineWeight}
	}

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
