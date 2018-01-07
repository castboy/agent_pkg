package agent_pkg

import (
	"encoding/json"
	"fmt"

	"github.com/widuu/goini"
)

func GetConf() {
	EtcdNodes = goini.SetConfig("conf.ini").ReadList()[1]["etcd"]

	InitEtcdCli()

	conf, ok := EtcdGet("apt/agent/conf")
	if !ok {
		LogCrt("%s", `Get Conf From Etcd Failed`)
	}
	Log.Info("%s", `Get Conf From Etcd Ok`)

	ParseConf(conf)
	GetLocalhost()
	GetPartition()
}
func ParseConf(bytes []byte) {
	if nil != json.Unmarshal(bytes, &AgentConf) {
		LogCrt("%s", "ParseConf Failed")
	}
	Log.Info("Agent Conf: %v", AgentConf)

	fmt.Println("Agent Conf Below:")
	fmt.Println("EngineReqPort: ", AgentConf.EngineReqPort)
	fmt.Println("MaxCache: ", AgentConf.MaxCache)
	fmt.Println("Partition: ", AgentConf.Partition)
	fmt.Println("Topic: ", AgentConf.Topic)
	fmt.Println("Offset: ", AgentConf.Offset)
	fmt.Println("HdfsNameNode: ", AgentConf.HdfsNameNode)
	fmt.Println("WebServerReqIp: ", AgentConf.WebServerReqIp)
	fmt.Println("WebServerReqPort: ", AgentConf.WebServerReqPort)
	fmt.Println("WafInstanceSrc: ", AgentConf.WafInstanceSrc)
	fmt.Println("WafInstanceDst: ", AgentConf.WafInstanceDst)
	fmt.Println("OfflineMsgTopic: ", AgentConf.OfflineMsgTopic)
	fmt.Println("OfflineMsgPartion: ", AgentConf.OfflineMsgPartion)
	fmt.Println("ClearHdfsHdlInterval: ", AgentConf.ClearHdfsHdl)
	fmt.Println("GetOfflineMsgInterval: ", AgentConf.GetOfflineMsg)
}
