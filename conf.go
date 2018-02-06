package agent_pkg

import (
	"encoding/json"

	"github.com/widuu/goini"
)

func GetConf() {
	defer func() {
		if err := recover(); nil != err {
			LogCrt("PANIC in GetConf(), %v", err)
		}
	}()

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
}
