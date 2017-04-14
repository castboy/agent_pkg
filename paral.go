package pkg_wmg

import (
    "fmt"
    "net/http"
    "strconv"
)

type ManageChMsg struct {
    Engine string
    Count int
    HandleIndex int
}
type CacheRes struct {
    Topic string
    Count int
}

var ManageCh = make(chan ManageChMsg, 100)
var HandleCh = make([]chan *[][]byte)

var CacheCh = make(chan CacheRes)

var HandleIndex int = 0

func Handle(w http.ResponseWriter, r *http.Request) {
    r.ParseForm()
    Engine, _ := strconv.Atoi(r.Form["type"][0])
    Count, _ := strconv.Atoi(r.Form["count"][0])

    ManageCh <- ManageChMsg{Engine, Count, HandleIndex}

    HandleIndex++
    HandleCh[HandleIndex] = make(chan *[][]byte)
    Data := <-HandleCh[HandleIndex]
}

func Manage() {
    for {
        select {
            case Msg := <-ManageCh:   
                data := ReadCache(Msg.Engine, Msg.Count)
                HandleCh[Msg.HandleIndex] <- &data 

            case cache := <-CacheCh:
                SetCacheOffset(cache.Topic, cache.Num) 
        }
    }    
}

func Listen() {
     http.HandleFunc("/", Handle)  
     http.ListenAndServe("localhost:8081", nil)  
}
