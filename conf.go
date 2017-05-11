package agent_pkg

import (
    "os"
    //"fmt"
    "encoding/json"
    "io/ioutil"
)

type V2Conf struct {
    Host string
    Partition int32
    MaxCache int    
}

var MyConf V2Conf = V2Conf{}

func InitConf(file string) {
    hdl, err := os.Open(file)    
    if err != nil {
        
    }
    byte, err := ioutil.ReadAll(hdl)

    err = json.Unmarshal(byte, &MyConf)
}
