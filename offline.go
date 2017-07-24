//offline.go

package agent_pkg

//	"fmt"

func StartOffline(msg Start) {

	startOffset, _, startErr, _ := Offset(msg.Base.Topic, Partition)
	consumer, err := InitConsumer(msg.Base.Topic, Partition, startOffset)
	if nil == startErr && nil == err {
		Consumers[msg.Base.Engine][msg.Base.Topic] = consumer
		if msg.Base.Engine == "waf" {
			Waf[msg.Base.Topic] = Status{startOffset, startOffset, 0, startOffset, -1, msg.Weight}
		} else {
			Vds[msg.Base.Topic] = Status{startOffset, startOffset, 0, startOffset, -1, msg.Weight}
		}

		PrefetchMsgSwitchMap[msg.Base.Topic] = true

		PrefetchChMap[msg.Base.Topic] = make(chan PrefetchMsg, 100)
		go Prefetch(PrefetchChMap[msg.Base.Topic])

		CacheInfoMap[msg.Base.Engine][msg.Base.Topic] = CacheInfo{0, 0}
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
	delete(Consumers[msg.Engine], msg.Topic)
	if msg.Engine == "waf" {
		delete(Waf, msg.Topic)
	} else {
		delete(Vds, msg.Topic)
	}
	delete(PrefetchMsgSwitchMap, msg.Topic)
	delete(CacheInfoMap[msg.Engine], msg.Topic)

	_, exist := PrefetchChMap[msg.Topic]
	if exist {
		PrefetchChMap[msg.Topic] <- PrefetchMsg{"", "", 0, true}
	}

	delete(PrefetchChMap, msg.Topic)
}

func CompleteOffline(msg Base) {
	ShutdownOffline(msg)
}
