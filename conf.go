package agent_pkg

import (
	"encoding/json"
	"fmt"
	"log"

	"github.com/widuu/goini"
)

func GetConf() {
	EtcdNodes = goini.SetConfig("conf.ini").ReadList()[1]["etcd"]

	InitEtcdCli()

	conf, ok := EtcdGet("apt/agent/conf")
	if !ok {
		Log("CRT", "%s", `Get Conf From Etcd Failed`)
	}
	Log("INF", "%s", `Get Conf From Etcd Ok`)

	ParseConf(conf)
	GetLocalhost()
	GetPartition()
}
func ParseConf(bytes []byte) {
	if nil != json.Unmarshal(bytes, &AgentConf) {
		Log("CRT", "%s", "ParseConf Failed")
		log.Fatalf(exit)
	}

	Log("TRC", "%s: %v", "Agent Conf", AgentConf)

	fmt.Printf("Agent Conf: %v", AgentConf)
}
