//offline.go

package agent_pkg

func StartOffline(msg StartOfflineMsg) {
	if msg.Engine == "waf" {
		startOffset, _ := Offset(msg.Topic, Partition)
		wafConsumers[msg.Topic] = InitConsumer(msg.Topic, Partition, startOffset)
		Waf[msg.Topic] = Status{startOffset, startOffset, 0, startOffset, -1, msg.Weight}

		PrefetchMsgSwitchMap[msg.Topic] = true

		PrefetchChMap[msg.Topic] = make(chan PrefetchMsg, 100)
		go Prefetch(PrefetchChMap[msg.Topic])

		WafCacheInfoMap[msg.Topic] = CacheInfo{0, 0}
	} else {
		startOffset, _ := Offset(msg.Topic, Partition)
		vdsConsumers[msg.Topic] = InitConsumer(msg.Topic, Partition, startOffset)
		Vds[msg.Topic] = Status{startOffset, startOffset, 0, startOffset, -1, msg.Weight}

		PrefetchMsgSwitchMap[msg.Topic] = true

		PrefetchChMap[msg.Topic] = make(chan PrefetchMsg, 100)
		go Prefetch(PrefetchChMap[msg.Topic])

		VdsCacheInfoMap[msg.Topic] = CacheInfo{0, 0}

	}
}

func StopOffline(msg OtherOfflineMsg) {
	startOffset, endOffset := Offset(msg.Topic, Partition)
	if msg.Engine == "waf" {
		Waf[msg.Topic] = Status{startOffset, Waf[msg.Topic].Engine, Waf[msg.Topic].Err, Waf[msg.Topic].Cache, endOffset, Waf[msg.Topic].Weight}
	} else {
		Vds[msg.Topic] = Status{startOffset, Vds[msg.Topic].Engine, Vds[msg.Topic].Err, Vds[msg.Topic].Cache, endOffset, Vds[msg.Topic].Weight}
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
