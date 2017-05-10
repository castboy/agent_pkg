package agent_pkg

import (
    "os"
    //"fmt"
    "encoding/json"
    "io/ioutil"
)

type Conf struct {
    Host string
    Partition int32
    MaxCache int    
}

var MyConf Conf = Conf{}

func InitConf(file string) {
    hdl, err := os.Open(file)    
    if err != nil {
        
    }
    byte, err := ioutil.ReadAll(hdl)

    err = json.Unmarshal(byte, &MyConf)
}
