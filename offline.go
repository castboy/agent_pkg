//offline.go

package agent_pkg

import (
    "fmt"
)

func StartOffline(msg StartOfflineMsg) {
	if msg.Engine == "waf" {
		startOffset, _, startErr, _ := Offset(msg.Topic, Partition)
		consumer, err := InitConsumer(msg.Topic, Partition, startOffset)
		if nil == startErr && nil == err {
            fmt.Println(startErr, err)
			wafConsumers[msg.Topic] = consumer
			Waf[msg.Topic] = Status{startOffset, startOffset, 0, startOffset, -1, msg.Weight}

			PrefetchMsgSwitchMap[msg.Topic] = true

			PrefetchChMap[msg.Topic] = make(chan PrefetchMsg, 100)
			go Prefetch(PrefetchChMap[msg.Topic])

			WafCacheInfoMap[msg.Topic] = CacheInfo{0, 0}
		}

	} else {
		startOffset, _, startErr, _ := Offset(msg.Topic, Partition)
		consumer, err := InitConsumer(msg.Topic, Partition, startOffset)
		if nil == startErr && nil == err {
			vdsConsumers[msg.Topic] = consumer
			Vds[msg.Topic] = Status{startOffset, startOffset, 0, startOffset, -1, msg.Weight}

			PrefetchMsgSwitchMap[msg.Topic] = true

			PrefetchChMap[msg.Topic] = make(chan PrefetchMsg, 100)
			go Prefetch(PrefetchChMap[msg.Topic])

			VdsCacheInfoMap[msg.Topic] = CacheInfo{0, 0}
		}
	}
}

func StopOffline(msg OtherOfflineMsg) {
	startOffset, endOffset, startErr, endErr := Offset(msg.Topic, Partition)
	if nil == startErr && nil == endErr {
		if msg.Engine == "waf" {
			Waf[msg.Topic] = Status{startOffset, Waf[msg.Topic].Engine, Waf[msg.Topic].Err, Waf[msg.Topic].Cache, endOffset, Waf[msg.Topic].Weight}
		} else {
			Vds[msg.Topic] = Status{startOffset, Vds[msg.Topic].Engine, Vds[msg.Topic].Err, Vds[msg.Topic].Cache, endOffset, Vds[msg.Topic].Weight}
		}
	}

}

func ShutdownOffline(msg OtherOfflineMsg) {
	if msg.Engine == "waf" {
		delete(wafConsumers, msg.Topic)
		delete(Waf, msg.Topic)
		delete(PrefetchMsgSwitchMap, msg.Topic)
		delete(WafCacheInfoMap, msg.Topic)

		_, exist := PrefetchChMap[msg.Topic]
		if exist {
			PrefetchChMap[msg.Topic] <- PrefetchMsg{"", "", 0, true}
		}

		delete(PrefetchChMap, msg.Topic)
	} else {
		delete(vdsConsumers, msg.Topic)
		delete(Vds, msg.Topic)
		delete(PrefetchMsgSwitchMap, msg.Topic)
		delete(VdsCacheInfoMap, msg.Topic)

		_, exist := PrefetchChMap[msg.Topic]
		if exist {
			PrefetchChMap[msg.Topic] <- PrefetchMsg{"", "", 0, true}
		}

		delete(PrefetchChMap, msg.Topic)
	}
}

func CompleteOffline(msg OtherOfflineMsg) {
	ShutdownOffline(msg)
}
