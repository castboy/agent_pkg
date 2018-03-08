//offline.go

package agent_pkg

import (
	"time"

	"github.com/optiopay/kafka"
)

func ExeOfflineMsg(msg OfflineMsg) {
	NextOfflineMsg = false

	switch msg.SignalType {
	case "start":
		StartOffline(msg)
		if "rule" == msg.Engine {
			go NewWafInstance(AgentConf.WafInstanceSrc, AgentConf.WafInstanceDst,
				msg.Topic, AgentConf.WebServerReqIp, AgentConf.WebServerReqPort)
		}

	case "stop":
		StopOffline(msg)

	case "shutdown", "error", "complete":
		ClearOffline(msg)
		if "rule" == msg.Engine {
			go KillWafInstance(AgentConf.WafInstanceDst, msg.Topic)
		}
	}

	OfflineMsgExedCh <- 1
	NextOfflineMsg = true

	Log.Info("offline-%s: %v", msg.SignalType, msg)
}

func StartOffline(msg OfflineMsg) {
	var engine, topic string
	var err error
	var consumer kafka.Consumer

	engine = msg.Engine
	topic = msg.Topic

	if 0 == msg.Weight {
		msg.Weight = 1
	}

	if _, ok := status[engine][topic]; !ok {
		startOffset, _, startErr, _ := Offset(topic, Partition)

		for i := 0; i < 10; i++ {
			consumer, err = InitConsumer(topic, Partition, startOffset)
			if nil != err {
				time.Sleep(time.Duration(5) * time.Second)

				continue
			} else {
				break
			}
		}

		if nil != err {
			Log.Error("create offline-task topic consumer err, topic: %s", topic)
		}

		if nil == startErr && nil == err {
			consumers[engine][topic] = consumer
			status[engine][topic] = Status{startOffset, startOffset, 0, startOffset, -1, msg.Weight}

			PrefetchMsgSwitchMap[topic] = true

			PrefetchChMap[topic] = make(chan PrefetchMsg, 100)
			go Prefetch(PrefetchChMap[topic])

			bufStatus[engine][topic] = BufStatus{0, 0}
		}
	}
}

func StopOffline(msg OfflineMsg) {
	startOffset, endOffset, startErr, endErr := Offset(msg.Topic, Partition)
	if nil == startErr && nil == endErr {
		s := status[msg.Engine][msg.Topic]
		if s.Weight == 0 {
			s.Weight = 5
		}
		status[msg.Engine][msg.Topic] = Status{startOffset, s.Engine, s.Err, s.Cache, endOffset, s.Weight}
	}
}

func ClearOffline(msg OfflineMsg) {
	delete(consumers[msg.Engine], msg.Topic)
	delete(status[msg.Engine], msg.Topic)
	delete(PrefetchMsgSwitchMap, msg.Topic)
	delete(bufStatus[msg.Engine], msg.Topic)
	delete(buffers, msg.Topic)

	_, exist := PrefetchChMap[msg.Topic]
	if exist {
		PrefetchChMap[msg.Topic] <- PrefetchMsg{"", "", 0, true}
		close(PrefetchChMap[msg.Topic])
		delete(PrefetchChMap, msg.Topic)
	}
}
