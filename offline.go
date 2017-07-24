//offline.go

package agent_pkg

//	"fmt"

func StartOffline(msg Start) {
	var start interface{} = msg
	var engine, topic string

	if val, ok := start.(Start); ok {
		engine = val.Base.Engine
		topic = val.Base.Topic
	} else {
		engine = msg.Engine
		topic = msg.Topic
	}

	startOffset, _, startErr, _ := Offset(topic, Partition)
	consumer, err := InitConsumer(topic, Partition, startOffset)
	if nil == startErr && nil == err {
		Consumers[engine][topic] = consumer
		status[engine][topic] = Status{startOffset, startOffset, 0, startOffset, -1, msg.Weight}

		PrefetchMsgSwitchMap[topic] = true

		PrefetchChMap[topic] = make(chan PrefetchMsg, 100)
		go Prefetch(PrefetchChMap[topic])

		CacheInfoMap[engine][topic] = CacheInfo{0, 0}
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
