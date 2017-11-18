package agent_pkg

import (
	"encoding/json"
	"fmt"
	"log"

	"github.com/widuu/goini"
)

func GetConf() {
	Log("INF", "%s", "GetConf")

	EtcdNodes = goini.SetConfig("conf.ini").ReadList()[1]["etcd"]

	InitEtcdCli()

	getConf, ok := EtcdGet("apt/agent/conf")
	if !ok {
		log.Fatal("configurations does not exist")
	}

	ParseConf(getConf)
	GetLocalhost()
	GetPartition()
}
func ParseConf(bytes []byte) {
	err := json.Unmarshal(bytes, &AgentConf)
	if err != nil {
		errLog := fmt.Sprintf("ParseConf Error: %s", err.Error())
		Log("Err", errLog)
		log.Fatalf(errLog)
	} else {
		fmt.Println("Cluster-Conf: ", AgentConf)
	}
}
