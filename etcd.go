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
	EngineReqPort int
	MaxCache      int
	Partition     map[string]int32
	Topic         []string
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

func Record(seconds int) {
	for {
		WafVds[0] = status["waf"]
		WafVds[1] = status["vds"]
		WafVds[2] = status["rule"]

		byte, _ := json.Marshal(WafVds)
		EtcdSet("apt/agent/status/"+Localhost, string(byte))

		time.Sleep(time.Duration(seconds) * time.Second)
	}
}

func InitEtcdCli(endpoint string) {
	cfg := clientv3.Config{
		Endpoints:   []string{"http://" + endpoint + ":2379"},
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
