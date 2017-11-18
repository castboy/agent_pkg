//etcd.go

package agent_pkg

import (
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/coreos/etcd/clientv3"
	"golang.org/x/net/context"
)

type Conf struct {
	EngineReqPort         int
	MaxCache              int
	Partition             map[string]int32
	Topic                 []string
	Offset                []int64
	HdfsNameNode          string
	WebServerReqIp        string
	WebServerReqPort      int
	WafInstanceSrc        string
	WafInstanceDst        string
	OfflineMsgTopic       string
	OfflineMsgPartion     int
	OfflineMsgStartOffset int
}

var EtcdCli *clientv3.Client

var AgentConf Conf

var EtcdNodes = make(map[string]string)

func Record() {
	var s StatusFromEtcd
	s.ReceivedOfflineMsgOffset = receivedOfflineMsgOffset
	s.Status[0] = status["waf"]
	s.Status[1] = status["vds"]
	s.Status[2] = status["rule"]

	byte, _ := json.Marshal(s)
	EtcdSet("apt/agent/status/"+Localhost, string(byte))
}

func InitEtcdCli() {
	Log("INF", "%s", "InitEtcdCli")

	nodes := make([]string, 0)
	for _, val := range EtcdNodes {
		elmt := val + ":2379"
		nodes = append(nodes, elmt)
	}

	cfg := clientv3.Config{
		Endpoints:   nodes,
		DialTimeout: 5 * time.Second,
	}

	EtcdCli, err = clientv3.New(cfg)
	if err != nil {
		Log("CRT", "InitEtcdCli: %s", err.Error())
		log.Fatalf(exit)
	}

	Log("INF", "%s", "InitEtcdCli Ok")
}

func EtcdSet(k, v string) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	_, err := EtcdCli.Put(ctx, k, v)
	cancel()
	if err != nil {
		errLog := fmt.Sprintf("EtcdSet Err: %s", err.Error())
		Log("Err", errLog)
	} else {
	}
}

func EtcdGet(key string) (bytes []byte, ok bool) {
	defer func() {
		if r := recover(); r != nil {
			bytes = []byte{}
			ok = false
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	resp, _ := EtcdCli.Get(ctx, key)
	cancel()

	return resp.Kvs[0].Value, true
}
