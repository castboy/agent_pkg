//http.go

package pkg_wmg

import (
    "fmt"
    "io"
    "net/http"
    "strconv"
)

type ManageMsg struct {
    EnginePtr *map[string]Partition
    Count int
    HandleIndex int
}

type PrefetchResMsg struct {
   Topic string
   PrefetchOffset int64
   PrefetchDataPtr *[][]byte
}

var HandleCh = make([]chan *[][]byte, 0)
var ManageCh = make(chan ManageMsg, 100)

var PrefetchResCh = make(chan PrefetchResMsg)

var EnginePtr *map[string]Partition

func Handle(w http.ResponseWriter, r *http.Request) {
    r.ParseForm()
    Engine := r.Form["type"][0]
    Count, _ := strconv.Atoi(r.Form["count"][0])
    
    var EnginePtr *map[string]Partition
    if Engine == "waf" {
        EnginePtr = &Waf 
        consumerPtr = &wafConsumers
        CacheInfoMapPtr = &WafCacheInfoMap
    } else {
        EnginePtr = &Vds    
        consumerPtr = &vdsConsumers
        CacheInfoMapPtr = &VdsCacheInfoMap
    }

    HandleCh = append(HandleCh, make(chan *[][]byte))
    handleIndex := len(HandleCh) - 1

    ManageCh <- ManageMsg{EnginePtr, Count, handleIndex}

    Data := <-HandleCh[handleIndex]
    
    fmt.Println("Handle", *Data)

    io.WriteString(w, "Response")
}

func Manage() {
    for {
        select {
            case req := <-ManageCh:   
                fmt.Println("req", req)
                DisposeReq(req)
                
            case res := <-PrefetchResCh:
                DisposeRes(res)
        }
    }    
}

func Listen() {
     http.HandleFunc("/", Handle)  
     http.ListenAndServe("localhost:8081", nil)  
}
