//etcd.go

package agent_pkg

import (
	"encoding/json"
	"time"

	EtcdClient "github.com/coreos/etcd/client"
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

//var EtcdCli *clientv3.Client
var EtcdApi EtcdClient.KeysAPI

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
	//	Log("INF", "%s", "InitEtcdCli")

	//	nodes := make([]string, 0)
	//	for _, val := range EtcdNodes {
	//		elmt := val + ":2379"
	//		nodes = append(nodes, elmt)
	//	}

	//	cfg := clientv3.Config{
	//		Endpoints:   nodes,
	//		DialTimeout: 5 * time.Second,
	//	}

	//	EtcdCli, err = clientv3.New(cfg)
	//	if err != nil {
	//		Log("CRT", "Init Etcd Client failed: %s", err.Error())
	//	}

	//	Log("INF", "%s", "Init Etcd Client Ok")

	nodes := make([]string, 0)
	for _, val := range EtcdNodes {
		elmt := "http://" + val + ":2379"
		nodes = append(nodes, elmt)
	}

	cfg := EtcdClient.Config{
		Endpoints:               nodes,
		Transport:               EtcdClient.DefaultTransport,
		HeaderTimeoutPerRequest: time.Second,
	}
	c, err := EtcdClient.New(cfg)
	if err != nil {
		Log("CRT", "Init Etcd Client failed: %s", err.Error())
	}
	EtcdApi = EtcdClient.NewKeysAPI(c)
}

func EtcdSet(k, v string) {
	_, err := EtcdApi.Set(context.Background(), k, v, nil)
	if err != nil {
		Log("ERR", "set etcd key err: %s", k, v)
	}
}

func EtcdGet(key string) (bytes []byte, ok bool) {
	defer func() {
		if r := recover(); r != nil {
			Log("ERR", "%s PANIC", "EtcdGet")
			bytes = []byte{}
			ok = false
		}
	}()

	resp, _ := EtcdApi.Get(context.Background(), key, nil)

	return []byte(resp.Node.Value), true
}
