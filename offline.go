//offline.go

package agent_pkg

import (
	"fmt"
	"time"
)

func StartOffline(msg Start) {
	Log.Info("start offline: %v", msg)
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

	time.Sleep(time.Duration(5) * time.Second) //can delete this

	startOffset, _, startErr, _ := Offset(topic, Partition)

	consumer, err := InitConsumer(topic, Partition, startOffset)

	if nil != err {
		Log.Error("create offline-task topic consumer err, topic: %s", topic)
	}

	if nil == startErr && nil == err {
		consumers[engine][topic] = consumer
		status[engine][topic] = Status{startOffset, startOffset, 0, startOffset, -1, msg.Weight}
		fmt.Println("status", status)

		PrefetchMsgSwitchMap[topic] = true

		PrefetchChMap[topic] = make(chan PrefetchMsg, 100)
		go Prefetch(PrefetchChMap[topic])

		bufStatus[engine][topic] = BufStatus{0, 0}
	}

	OfflineMsgExedCh <- 1
}

func StopOffline(msg Base) {
	Log.Info("Stop Offline %v", msg)
	startOffset, endOffset, startErr, endErr := Offset(msg.Topic, Partition)
	if nil == startErr && nil == endErr {
		s := status[msg.Engine][msg.Topic]
		if s.Weight == 0 {
			s.Weight = 5
		}
		status[msg.Engine][msg.Topic] = Status{startOffset, s.Engine, s.Err, s.Cache, endOffset, s.Weight}
	}

	OfflineMsgExedCh <- 1
}

func ShutdownOffline(msg Base) {
	Log.Info("Shutdown Offline %v", msg)
	delete(consumers[msg.Engine], msg.Topic)
	delete(status[msg.Engine], msg.Topic)
	delete(PrefetchMsgSwitchMap, msg.Topic)
	delete(bufStatus[msg.Engine], msg.Topic)

	_, exist := PrefetchChMap[msg.Topic]
	if exist {
		PrefetchChMap[msg.Topic] <- PrefetchMsg{"", "", 0, true}
	}

	delete(PrefetchChMap, msg.Topic)

	OfflineMsgExedCh <- 1
}

func CompleteOffline(msg Base) {
	ShutdownOffline(msg)
}
