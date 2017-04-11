//signal.go

package pkg_wmg

import (
//	"encoding/json"
    "net/http"
//	"fmt"
    "strconv"
    "io"
)

func StartOffline (w http.ResponseWriter, r *http.Request) {
    r.ParseForm()
    pType := r.Form["type"][0]
    topic := r.Form["topic"][0]
    weight, _ := strconv.Atoi(r.Form["weight"][0])

    startOffset, _ := Offset(topic, localhostPartition)
    
    if "waf" == pType {
        Waf[topic] = Partition{startOffset, 0, -1, weight, false}     
        wafConsumers[topic] = InitConsumer(topic, localhostPartition, startOffset)
    }

}

func GetLastOffset (w http.ResponseWriter, r *http.Request) {
    r.ParseForm()
    topic := r.Form["topic"][0] 
    pType := r.Form["type"][0] 

    _, endOffset := Offset(topic, localhostPartition) 

    if "waf" == pType {
        Waf[topic] = Partition{Waf[topic].First, Waf[topic].Current, endOffset, Waf[topic].Weight, true}     
    } else {
        
    }
}

func ReadWaf (w http.ResponseWriter, r *http.Request) {
    r.ParseForm() 
    reqNum, _ := strconv.Atoi(r.Form["reqNum"][0])

    Ptr = &Waf
    PtrBak = &WafBak
    consumerPtr = &wafConsumers

    io.WriteString(w, Distri(reqNum))
}

func ReadVds (w http.ResponseWriter, r *http.Request) {
    r.ParseForm() 
    reqNum, _ := strconv.Atoi(r.Form["reqNum"][0])

    Ptr = &Vds
    PtrBak = &VdsBak
    consumerPtr = &vdsConsumers

    io.WriteString(w, Distri(reqNum))
}

