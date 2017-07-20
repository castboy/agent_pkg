//offline.go

package agent_pkg

//	"fmt"

func StartOffline(msg Start) {
	if msg.Base.Engine == "waf" {
		startOffset, _, startErr, _ := Offset(msg.Base.Topic, Partition)
		consumer, err := InitConsumer(msg.Base.Topic, Partition, startOffset)
		if nil == startErr && nil == err {
			wafConsumers[msg.Base.Topic] = consumer
			Waf[msg.Base.Topic] = Status{startOffset, startOffset, 0, startOffset, -1, msg.Weight}

			PrefetchMsgSwitchMap[msg.Base.Topic] = true

			PrefetchChMap[msg.Base.Topic] = make(chan PrefetchMsg, 100)
			go Prefetch(PrefetchChMap[msg.Base.Topic])

			WafCacheInfoMap[msg.Base.Topic] = CacheInfo{0, 0}
		}

	} else {
		startOffset, _, startErr, _ := Offset(msg.Base.Topic, Partition)
		consumer, err := InitConsumer(msg.Base.Topic, Partition, startOffset)
		if nil == startErr && nil == err {
			vdsConsumers[msg.Base.Topic] = consumer
			Vds[msg.Base.Topic] = Status{startOffset, startOffset, 0, startOffset, -1, msg.Weight}

			PrefetchMsgSwitchMap[msg.Base.Topic] = true

			PrefetchChMap[msg.Base.Topic] = make(chan PrefetchMsg, 100)
			go Prefetch(PrefetchChMap[msg.Base.Topic])

			VdsCacheInfoMap[msg.Base.Topic] = CacheInfo{0, 0}
		}
	}
}

func StopOffline(msg Base) {
	startOffset, endOffset, startErr, endErr := Offset(msg.Topic, Partition)
	if nil == startErr && nil == endErr {
		if msg.Engine == "waf" {
			Waf[msg.Topic] = Status{startOffset, Waf[msg.Topic].Engine, Waf[msg.Topic].Err, Waf[msg.Topic].Cache, endOffset, Waf[msg.Topic].Weight}
		} else {
			Vds[msg.Topic] = Status{startOffset, Vds[msg.Topic].Engine, Vds[msg.Topic].Err, Vds[msg.Topic].Cache, endOffset, Vds[msg.Topic].Weight}
		}
	}

}

func ShutdownOffline(msg Base) {
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

func CompleteOffline(msg Base) {
	ShutdownOffline(msg)
}
