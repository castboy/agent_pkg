package agent_pkg

import (
    "encoding/json"
    "fmt"
    "log"
    "time"
    "errors"
    "golang.org/x/net/context"
    "github.com/coreos/etcd/clientv3"
)

type Conf struct {
    EngineReqPort int
    MaxCache int
    Partition map[string]int
    Topic []string    
}

var EtcdCli *clientv3.Client

var AgentConf Conf

func SetConf() string {
    partitions := make(map[string]int)
    partitions["192.168.0.10"] = 0
    partitions["192.168.0.11"] = 1
    partitions["192.168.0.12"] = 2
    partitions["192.168.0.13"] = 4

    topic := []string{"xdrHttp", "xdrFile"}

    conf := Conf{8081, 100, partitions, topic} 

    byte, _ := json.Marshal(conf)
    
    return string(byte)
}

func ParseConf(bytes []byte) {
    err := json.Unmarshal(bytes, &AgentConf)
    if err != nil {
        
    } else {
        fmt.Println(AgentConf)     
    }
}

func InitEtcdCli() {
    cfg := clientv3.Config{
        Endpoints:               []string{"http://10.80.6.8:2379"},
        DialTimeout: 5 * time.Second,
    }
    var err error = errors.New("this is a new error")
    EtcdCli, err = clientv3.New(cfg)
    if err != nil {
        log.Fatal(err)
    }
    //defer EtcdCli.Close()
}

func EtcdSet(k, v string) {
    ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
    _, err := EtcdCli.Put(ctx, k, v)
    cancel()
    if err != nil {
        fmt.Println("EtcdSetErr")
    } else {
        //fmt.Println(string(resp.Kvs[0].Value))    
        //fmt.Println(resp)    
    }
}

func EtcdGet(key string) []byte {
    ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
    resp, err := EtcdCli.Get(ctx, key)
    cancel()
    if err != nil {
            // handle error!
    }
    bytes := resp.Kvs[0].Value   
    
    return bytes
}
