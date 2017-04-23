//prefetch.go

package pkg_wmg

import (
    "fmt"
)
type PrefetchMsg struct {
    Topic string
    Count int
}

var PrefetchMsgSwitchMap = make(map[string] bool)
var PrefetchChMap = make(map[string] chan PrefetchMsg)
var PrefetchDataCount = make(map[string] int)

func InitPrefetchMsgSwitchMap() {
    for topic, _ := range Waf {
        PrefetchMsgSwitchMap[topic] = true    
    } 

    for topic, _ := range Vds {
        PrefetchMsgSwitchMap[topic] = true    
    } 
}

func ReadKafka(prefetchMsg PrefetchMsg) {
    defer func()  {
        if r := recover(); r != nil {
            fmt.Printf("consume err: %v", r)    
        }    
    }()

    PrefetchDataCount[prefetchMsg.Topic] = 0
    for i := 0; i < prefetchMsg.Count; i++ {
        msg, err := (*consumerPtr)[prefetchMsg.Topic].Consume() 
        if err != nil {
            panic("no data in: " + prefetchMsg.Topic)    
        }
        PrefetchDataCount[prefetchMsg.Topic]++ 
        CacheDataMap[prefetchMsg.Topic] = append(CacheDataMap[prefetchMsg.Topic], msg.Value)
    }
}

func Prefetch(prefetchCh chan PrefetchMsg) {
    for {
        prefetchMsg := <-prefetchCh   
        fmt.Println("received PrefetchMsg:", prefetchMsg)

        ReadKafka(prefetchMsg)
        count := PrefetchDataCount[prefetchMsg.Topic]
        res := PrefetchResMsg{
            Topic: prefetchMsg.Topic,
            Count: count,    
        }

        if PrefetchDataCount[prefetchMsg.Topic] != 0 {
            PrefetchResCh <- res 
        }
                
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
