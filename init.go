//init.go

package agent_pkg

import (
	"encoding/json"
	"fmt"
)

type Status struct {
	First  int64
	Engine int64
	Err    int
	Cache  int64
	Last   int64
	Weight int
}

type StatusFromEtcd struct {
	ReceivedOfflineMsgOffset int64
	Status                   [3]map[string]Status
}

var statusFromEtcd StatusFromEtcd
var receivedOfflineMsgOffset int64 = 4
var status = make(map[string]map[string]Status)

func InitStatus() {
	wafTopic := AgentConf.Topic[0]
	vdsTopic := AgentConf.Topic[1]

	InitStatusMap()

	status["waf"][wafTopic] = Status{0, 0, 0, 0, -1, 1}

	status["vds"][vdsTopic] = Status{0, 0, 0, 0, -1, 1}

	fmt.Println("InitStatus : ", status)
	fmt.Println("ReceivedOfflineMsgOffset : ", receivedOfflineMsgOffset)
}

func InitStatusMap() {
	for _, v := range reqTypes {
		status[v] = make(map[string]Status)
	}
}

func GetStatusFromEtcd(bytes []byte) {
	err := json.Unmarshal(bytes, &statusFromEtcd)
	if err != nil {
		fmt.Println("GetStatusFromEtcd Err")
	}

	InitStatusMap()

	status["waf"] = statusFromEtcd.Status[0]
	status["vds"] = statusFromEtcd.Status[1]
	status["rule"] = statusFromEtcd.Status[2]

	receivedOfflineMsgOffset = statusFromEtcd.ReceivedOfflineMsgOffset

	fmt.Println("GetStatusFromEtcd : ", status)
	fmt.Println("ReceivedOfflineMsgOffset : ", receivedOfflineMsgOffset)
}
