//offline.go

package agent_pkg

import (
    "fmt"
)

func StartOffline(msg StartOfflineMsg) {
    if msg.Engine == "waf" {
        startOffset, endOffset := Offset(msg.Topic, MyConf.Partition)
        wafConsumers[msg.Topic] = InitConsumer(msg.Topic, MyConf.Partition, startOffset)
        Waf[msg.Topic] = Partition{startOffset, startOffset, startOffset, endOffset, msg.Weight, false}    
       
        PrefetchMsgSwitchMap[msg.Topic] = true

        PrefetchChMap[msg.Topic] = make(chan PrefetchMsg, 100) 
        go Prefetch(PrefetchChMap[msg.Topic])

        WafCacheInfoMap[msg.Topic] = CacheInfo{0, 0}
    } else {
        startOffset, endOffset := Offset(msg.Topic, MyConf.Partition)
        fmt.Println("startOffset", startOffset)
        vdsConsumers[msg.Topic] = InitConsumer(msg.Topic, MyConf.Partition, startOffset)
        Vds[msg.Topic] = Partition{startOffset, startOffset, startOffset, endOffset, msg.Weight, false}    

        PrefetchMsgSwitchMap[msg.Topic] = true
       
        PrefetchChMap[msg.Topic] = make(chan PrefetchMsg, 100) 
        go Prefetch(PrefetchChMap[msg.Topic])

        VdsCacheInfoMap[msg.Topic] = CacheInfo{0, 0}
        
    }
}

func StopOffline(msg StopOfflineMsg) {
    startOffset, endOffset := Offset(msg.Topic, MyConf.Partition)
    if msg.Engine == "waf" {
        Waf[msg.Topic] = Partition{startOffset, Waf[msg.Topic].Engine, Waf[msg.Topic].Cache, endOffset, Waf[msg.Topic].Weight, true}    
    } else {
        Vds[msg.Topic] = Partition{startOffset, Vds[msg.Topic].Engine, Vds[msg.Topic].Cache, endOffset, Vds[msg.Topic].Weight, true}    
    } 
}
