//prefetch.go

package pkg_wmg

import (
    "fmt"
)
type PrefetchMsg struct {
    Topic string
    Count int
}

var PrefetchChMap = make(map[string] chan PrefetchMsg)

func ReadKafka(prefetchMsg PrefetchMsg) (int64, *[][]byte) {
    PrefetchData := make([][]byte, 0)
    var PrefetchOffset int64 = 0

    defer func() {
        if r := recover(); r != nil {
            fmt.Printf("consume err: %v", r)    
            //return PrefetchOffset, &PrefetchData 
        }    
    }()

    for i := 0; i < prefetchMsg.Count; i++ {
        msg, err := (*consumerPtr)[prefetchMsg.Topic].Consume() 
        if err != nil {
            panic("no data in: " + prefetchMsg.Topic)    
        }
        PrefetchOffset = msg.Offset
        PrefetchData = append(PrefetchData, msg.Value)
    }

    return PrefetchOffset, &PrefetchData 
}

func Prefetch(prefetchCh chan PrefetchMsg) {
    for {
        prefetchMsg := <-prefetchCh   
        fmt.Println("received PrefetchMsg:", prefetchMsg)
        if WafCacheInfoMap[prefetchMsg.Topic].End == WafCacheInfoMap[prefetchMsg.Topic].Current {
            prefetchOffset, prefetchDataPtr := ReadKafka(prefetchMsg)
            if prefetchDataPtr != nil {
                res := PrefetchResMsg{prefetchMsg.Topic, prefetchOffset, prefetchDataPtr}
                PrefetchResCh <- res 
            }
        }
    }
}

func InitPrefetch() {
    for topic, _ :=  range Waf {
        PrefetchChMap[topic] = make(chan PrefetchMsg, 100) 
        go Prefetch(PrefetchChMap[topic])
    }
}
