//http.go

package agent_pkg

import (
    "fmt"
    "io"
    "net/http"
    "strconv"
    "encoding/json"
    "regexp"
)

type ManageMsg struct {
    Engine string
    Count int
    HandleCh chan *[][]byte
}

type HttpRes struct {
    Code int
    Data interface{}    
    Num int
}

type StartOfflineMsg struct {
    Engine string
    Topic string
    Weight int    
}

type StopOfflineMsg struct {
    Engine string
    Topic string
}

type PrefetchResMsg struct {
    Engine string
    Topic string
    DataPtr *[][]byte
}

var ManageCh = make(chan ManageMsg, 10000)

var PrefetchResCh = make(chan PrefetchResMsg)

var StartOfflineCh = make(chan StartOfflineMsg)
var StopOfflineCh = make(chan StopOfflineMsg)

func Handle(w http.ResponseWriter, r *http.Request) {
    
    match, _ := regexp.MatchString("/?type=(waf|vds)&count=([0-9]+$)", r.RequestURI)
    if !match {
        io.WriteString(w, "Usage of: http://ip:port/?type=waf/vds&count=num")     
        return
    }

    r.ParseForm()
    Engine := r.Form["type"][0]
    Count, _ := strconv.Atoi(r.Form["count"][0])
    

    HandleCh := make(chan *[][]byte)

    ManageCh <- ManageMsg{Engine, Count, HandleCh}
    fmt.Println("Length of ManageCh:", len(ManageCh))

    Data := <-HandleCh
    fmt.Println("http.go <-HandleCh")
    
    var data interface{}
    var dataSlice = make([]interface{}, 0)
    dataLen := len(*Data)
    for i := 0; i < dataLen; i++ {
        err := json.Unmarshal((*Data)[i], &data)    
        if err != nil {
            fmt.Println("Unmarshal Error")    
        }
        dataSlice = append(dataSlice, data) 
    }

    res := HttpRes{
        Code: 10000,
        Data: dataSlice,
        Num: dataLen,
    }

    if dataLen == 0 {
        res = HttpRes{
            Code: 10000,
            Data: nil,
            Num: dataLen,
        }
    }

    byte, _ := json.Marshal(res)

    io.WriteString(w, string(byte))
}

func Manage() {
    for {
        select {
            case req := <-ManageCh:   
                fmt.Println("req", req)
                DisposeReq(req)
                
            case res := <-PrefetchResCh:
                fmt.Println("prefetch res", res)
                RdHdfs(res)

            case start := <- StartOfflineCh:
                StartOffline(start)

            case stop := <- StopOfflineCh:
                StopOffline(stop)
        }
    }    
}

func OfflineStartHandle(w http.ResponseWriter, r *http.Request) {
    r.ParseForm()
    engine := r.Form["type"][0]
    topic := r.Form["topic"][0]
    weight, _ := strconv.Atoi(r.Form["weight"][0])
    
    fmt.Println("OfflineStart:  ", "engine:", engine, " topic:", topic, "weight:", weight)
    startOfflineMsg := StartOfflineMsg {
        Engine: engine,
        Topic: topic,
        Weight: weight,    
    }

    StartOfflineCh <- startOfflineMsg
}

func OfflineStopHandle(w http.ResponseWriter, r *http.Request) {
    r.ParseForm()
    engine := r.Form["type"][0]
    topic := r.Form["topic"][0]
    
    fmt.Println("OfflineStop:  ", "engine:", engine, " topic:", topic)
    stopOfflineMsg := StopOfflineMsg {
        Engine: engine,
        Topic: topic,
    }

    StopOfflineCh <- stopOfflineMsg
}

func ListenReq(url string) {
     http.HandleFunc("/", Handle)  
     http.HandleFunc("/start", OfflineStartHandle)  
     http.HandleFunc("/stop", OfflineStopHandle)  
     http.ListenAndServe(url, nil)  
}
