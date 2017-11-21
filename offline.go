//offline.go

package agent_pkg

//	"fmt"

func StartOffline(msg Start) {
	Log("INF", "%s: %v", "start offline", msg)
	var start interface{} = msg
	var engine, topic string

	if val, ok := start.(Start); ok {
		engine = val.Base.Engine
		topic = val.Base.Topic
	} else {
		engine = msg.Engine
		topic = msg.Topic
	}

	if 0 == msg.Weight {
		msg.Weight = 1
	}

	startOffset, _, startErr, _ := Offset(topic, Partition)
	consumer, err := InitConsumer(topic, Partition, startOffset)
	if nil == startErr && nil == err {
		consumers[engine][topic] = consumer
		status[engine][topic] = Status{startOffset, startOffset, 0, startOffset, -1, msg.Weight}

		PrefetchMsgSwitchMap[topic] = true

		PrefetchChMap[topic] = make(chan PrefetchMsg, 100)
		go Prefetch(PrefetchChMap[topic])

		bufStatus[engine][topic] = BufStatus{0, 0}
	}
}

func StopOffline(msg Base) {
	Log("INF", "Stop Offline %v", msg)
	startOffset, endOffset, startErr, endErr := Offset(msg.Topic, Partition)
	if nil == startErr && nil == endErr {
		s := status[msg.Engine][msg.Topic]
		status[msg.Engine][msg.Topic] = Status{startOffset, s.Engine, s.Err, s.Cache, endOffset, s.Weight}
	}

}

func ShutdownOffline(msg Base) {
	Log("INF", "Shutdown Offline %v", msg)
	delete(consumers[msg.Engine], msg.Topic)
	delete(status[msg.Engine], msg.Topic)
	delete(PrefetchMsgSwitchMap, msg.Topic)
	delete(bufStatus[msg.Engine], msg.Topic)

	_, exist := PrefetchChMap[msg.Topic]
	if exist {
		PrefetchChMap[msg.Topic] <- PrefetchMsg{"", "", 0, true}
	}

	delete(PrefetchChMap, msg.Topic)
}

func CompleteOffline(msg Base) {
	ShutdownOffline(msg)
}
