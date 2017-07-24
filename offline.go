//offline.go

package agent_pkg

//	"fmt"

func StartOffline(msg Start) {

	startOffset, _, startErr, _ := Offset(msg.Base.Topic, Partition)
	consumer, err := InitConsumer(msg.Base.Topic, Partition, startOffset)
	if nil == startErr && nil == err {
		Consumers[msg.Base.Engine][msg.Base.Topic] = consumer
		status[msg.Base.Engine][msg.Base.Topic] = Status{startOffset, startOffset, 0, startOffset, -1, msg.Weight}

		PrefetchMsgSwitchMap[msg.Base.Topic] = true

		PrefetchChMap[msg.Base.Topic] = make(chan PrefetchMsg, 100)
		go Prefetch(PrefetchChMap[msg.Base.Topic])

		CacheInfoMap[msg.Base.Engine][msg.Base.Topic] = CacheInfo{0, 0}
	}
}

func StopOffline(msg Base) {
	startOffset, endOffset, startErr, endErr := Offset(msg.Topic, Partition)
	if nil == startErr && nil == endErr {
		status[msg.Engine][msg.Topic] = Status{startOffset, status[msg.Engine][msg.Topic].Engine,
			status[msg.Engine][msg.Topic].Err, status[msg.Engine][msg.Topic].Cache,
			endOffset, status[msg.Engine][msg.Topic].Weight}
	}

}

func ShutdownOffline(msg Base) {
	delete(Consumers[msg.Engine], msg.Topic)
	delete(status[msg.Engine], msg.Topic)
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
