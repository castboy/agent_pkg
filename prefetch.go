//prefetch.go

package agent_pkg

import (
    "fmt"
)
type PrefetchMsg struct {
    Engine string
    Topic string
    Count int
}

var PrefetchMsgSwitchMap = make(map[string] bool)
var PrefetchChMap = make(map[string] chan PrefetchMsg)

func InitPrefetchMsgSwitchMap() {
    for topic, _ := range Waf {
        PrefetchMsgSwitchMap[topic] = true    
    } 

    for topic, _ := range Vds {
        PrefetchMsgSwitchMap[topic] = true    
    } 
}

func ReadKafka(prefetchMsg PrefetchMsg, data *[][]byte) {
    defer func()  {
        if r := recover(); r != nil {
            fmt.Printf("consume err: %v", r)    
        }    
    }()

    if prefetchMsg.Engine == "waf" {
        for i := 0; i < prefetchMsg.Count; i++ {
            msg, err := wafConsumers[prefetchMsg.Topic].Consume() 
            if err != nil {
                panic("no data in: " + prefetchMsg.Topic)    
            }
            *data = append(*data, msg.Value)
        }
    } else {
        for i := 0; i < prefetchMsg.Count; i++ {
            msg, err := vdsConsumers[prefetchMsg.Topic].Consume() 
            if err != nil {
                panic("no data in: " + prefetchMsg.Topic)    
            }
            *data = append(*data, msg.Value)
        }
    }
}

func Prefetch(prefetchCh chan PrefetchMsg) {
    for {
        prefetchMsg := <-prefetchCh   
        fmt.Println("received PrefetchMsg:", prefetchMsg)

        var Data [][]byte
        ReadKafka(prefetchMsg, &Data)
        dataPtr := &Data

        res := PrefetchResMsg{
            Engine: prefetchMsg.Engine,
            Topic: prefetchMsg.Topic,
            DataPtr: dataPtr,    
        }

        PrefetchResCh <- res 
                
    }
}

func InitPrefetch() {
    for topic, _ :=  range Waf {
        PrefetchChMap[topic] = make(chan PrefetchMsg, 100) 
        go Prefetch(PrefetchChMap[topic])
    }
    for topic, _ :=  range Vds {
        PrefetchChMap[topic] = make(chan PrefetchMsg, 100) 
        go Prefetch(PrefetchChMap[topic])
    }
}
