//offline.go

package agent_pkg

import (
)

func StartOffline(msg StartOfflineMsg) {
    if msg.Engine == "waf" {
        startOffset, _ := Offset(msg.Topic, Partition)
        wafConsumers[msg.Topic] = InitConsumer(msg.Topic, Partition, startOffset)
        Waf[msg.Topic] = Status{startOffset, startOffset, startOffset, -1, msg.Weight}    
       
        PrefetchMsgSwitchMap[msg.Topic] = true

        PrefetchChMap[msg.Topic] = make(chan PrefetchMsg, 100) 
        go Prefetch(PrefetchChMap[msg.Topic])

        WafCacheInfoMap[msg.Topic] = CacheInfo{0, 0}
    } else {
        startOffset, _ := Offset(msg.Topic, Partition)
        vdsConsumers[msg.Topic] = InitConsumer(msg.Topic, Partition, startOffset)
        Vds[msg.Topic] = Status{startOffset, startOffset, startOffset, -1, msg.Weight}    

        PrefetchMsgSwitchMap[msg.Topic] = true
       
        PrefetchChMap[msg.Topic] = make(chan PrefetchMsg, 100) 
        go Prefetch(PrefetchChMap[msg.Topic])

        VdsCacheInfoMap[msg.Topic] = CacheInfo{0, 0}
        
    }
}

func StopOffline(msg StopOfflineMsg) {
    startOffset, endOffset := Offset(msg.Topic, Partition)
    if msg.Engine == "waf" {
        Waf[msg.Topic] = Status{startOffset, Waf[msg.Topic].Engine, Waf[msg.Topic].Cache, endOffset, Waf[msg.Topic].Weight}    
    } else {
        Vds[msg.Topic] = Status{startOffset, Vds[msg.Topic].Engine, Vds[msg.Topic].Cache, endOffset, Vds[msg.Topic].Weight}    
    } 
}
