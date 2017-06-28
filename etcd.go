//etcd.go

package agent_pkg

import (
    "os"
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
    Partition map[string]int32
    Topic []string    
}

var EtcdCli *clientv3.Client

var AgentConf Conf

func ParseConf(bytes []byte) {
    err := json.Unmarshal(bytes, &AgentConf)
    if err != nil {
        fmt.Println("ParseConf Error") 
        errLog := "ParseConf Error"
        Log("Err", errLog)
    } else {
        fmt.Println("Cluster-Conf: ", AgentConf)     
    }
}

func Record(seconds int) {
    for {
        byte, _ := json.Marshal(WafVds)
        EtcdSet("apt/agent/status/"+Localhost, string(byte))
        
        time.Sleep(time.Duration(seconds) * time.Second)
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
        errLog := "InitEtcdCli Err"
        Log("Err", errLog)
    }
    //defer EtcdCli.Close()
}

func EtcdSet(k, v string) {
    ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
    resp, err := EtcdCli.Put(ctx, k, v)
    cancel()
    if err != nil {
        fmt.Println("EtcdSetErr")
        errLog := "EtcdSetErr"
        Log("Err", errLog)
    } else {
        //fmt.Println(string(resp.Kvs[0].Value))    
        //fmt.Println("set ", k, "success. times:", resp.Header.Revision)    
        infoLog := "set " + k + "success. times:"+ string(resp.Header.Revision)
        Log("Info", infoLog)
    }
}

func EtcdGet(key string) []byte {
    defer func()  {
        if r := recover(); r != nil {
            errInfo := "configuration item: " + key + " does not exist!"
            fmt.Println(errInfo)    
            Log("Err", errInfo)
            os.Exit(0)
        }    
    }()
    
    ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
    resp, err := EtcdCli.Get(ctx, key)
    cancel()
    if err != nil {
        panic("")
    }
    bytes := resp.Kvs[0].Value   
    
    return bytes
}
