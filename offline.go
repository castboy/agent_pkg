//offline.go

package agent_pkg

import (
	"fmt"
)

func ExeOfflineMsg(msg OfflineMsg) {
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
		ClearOffline(msg, msg.SignalType)
		if "rule" == msg.Engine {
			go KillWafInstance(AgentConf.WafInstanceDst, msg.Topic)
		}
	}
}

func StartOffline(msg OfflineMsg) {
	var engine, topic string

	engine = msg.Engine
	topic = msg.Topic

	if 0 == msg.Weight {
		msg.Weight = 1
	}

	if _, ok := status[engine][topic]; !ok {
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
	}

	OfflineMsgExedCh <- 1
	NextOfflineMsg = true

	Log.Info("offline-start: %v", msg)

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

	OfflineMsgExedCh <- 1
	NextOfflineMsg = true

	Log.Info("offline-stop: %v", msg)
}

func ClearOffline(msg OfflineMsg, t string) {
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
	NextOfflineMsg = true

	Log.Info("offline-%s: %v", t, msg)
}
