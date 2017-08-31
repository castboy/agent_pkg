//etcd.go

package agent_pkg

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/coreos/etcd/clientv3"
	"golang.org/x/net/context"
)

type Conf struct {
	EngineReqPort         int
	MaxCache              int
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
	KafkaHost             string
	KafkaPartition        int32
}

var EtcdCli *clientv3.Client

var AgentConf Conf

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

func Record() {
	var s StatusFromEtcd
	s.ReceivedOfflineMsgOffset = receivedOfflineMsgOffset
	s.Status[0] = status["waf"]
	s.Status[1] = status["vds"]
	s.Status[2] = status["rule"]

	byte, _ := json.Marshal(s)
	EtcdSet("apt/agent/status/"+Localhost, string(byte))
}

func InitEtcdCli(endpoints map[string]string) {
	endpoint := make([]string, 0)
	for _, val := range endpoints {
		elmt := "http://" + val + ":2379"
		endpoint = append(endpoint, elmt)
	}

	cfg := clientv3.Config{
		Endpoints:   endpoint,
		DialTimeout: 5 * time.Second,
	}
	var err error = errors.New("this is a new error")
	EtcdCli, err = clientv3.New(cfg)
	if err != nil {
		errLog := fmt.Sprintf("InitEtcdCli Err: %s", err.Error())
		Log("Err", errLog)
		log.Fatalf(errLog)
	}
	//defer EtcdCli.Close()
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
