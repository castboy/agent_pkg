//record.go

package agent_pkg

import (
    "encoding/json"
    "time"
    "io/ioutil"
)

func Record(file string) {
    for {
        byte, _ := json.Marshal(wafVds)
        ioutil.WriteFile(file, byte, 0644)

        time.Sleep(1 * time.Second)
    }    
}
