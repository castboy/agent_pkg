package agent_pkg

import (
	"encoding/json"
	"fmt"
	"log"
	"time"
)

type OfflineMsg struct {
	Engine     string `json: "Engine"`
	Topic      string `json: "Topic"`
	Weight     int    `json: "Weight"`
	SignalType string `json: "SignalType"`
}

var signals = []string{"start", "stop", "complete", "shutdown"}
var types = []string{"waf", "vds", "rule"}

func TimingGetOfflineMsg(second int) {
	consumer, err := InitConsumer(AgentConf.OfflineMsgTopic, int32(AgentConf.OfflineMsgPartion), receivedOfflineMsgOffset+1)
	if nil != err {
		info := "init TimingGetOfflineMsg consumer err"
		Log("Err", info)
		log.Fatalln(info)
	}

	var msg OfflineMsg

	for {
		kafkaMsg, err := consumer.Consume()
		if nil != err {

		} else {
			err := json.Unmarshal(kafkaMsg.Value, &msg)
			if nil != err {
				Log("Err", "wrong offline msg: "+string(kafkaMsg.Value))
			} else {
				OfflineHandle(msg)
				receivedOfflineMsgOffset++
			}
		}

		time.Sleep(time.Duration(second) * time.Second)
	}
}

func SetStatus() {
	msgs := LoadOfflineMsg()
	msgs = ExtractValidOfflineMsg(msgs)

	SendOfflineMsg(msgs)
}

func LoadOfflineMsg() (offlineMsgs []OfflineMsg) {
	var msg OfflineMsg

	consumer, err := InitConsumer(AgentConf.OfflineMsgTopic, int32(AgentConf.OfflineMsgPartion), receivedOfflineMsgOffset+1)
	if nil != err {
		info := "init TimingGetOfflineMsg consumer err"
		Log("Err", info)
		log.Fatalln(info)
	}

	for {
		kafkaMsg, err := consumer.Consume()
		fmt.Println(string(kafkaMsg.Value))
		if nil != err {
			break
		} else {
			err := json.Unmarshal(kafkaMsg.Value, &msg)
			if nil != err {
				Log("Err", "wrong offline msg: "+string(kafkaMsg.Value))
			} else {
				offlineMsgs = append(offlineMsgs, msg)
			}
		}

		receivedOfflineMsgOffset++
	}

	fmt.Println("all msg:", offlineMsgs)
	return offlineMsgs
}

func ExtractValidOfflineMsg(offlineMsgs []OfflineMsg) []OfflineMsg {
	var invalidOfflineTask []string
	var invalidOfflineTaskId []int
	var validOfflineMsg []OfflineMsg

	for _, v := range offlineMsgs {
		if "shutdown" == v.SignalType {
			invalidOfflineTask = append(invalidOfflineTask, v.Topic)
		}
	}

	for i, j := range offlineMsgs {
		for _, v := range invalidOfflineTask {
			if j.Topic == v {
				invalidOfflineTaskId = append(invalidOfflineTaskId, i)
			}
		}
	}

	for k, _ := range offlineMsgs {
		valid := true
		for _, j := range invalidOfflineTaskId {
			if j == k {
				valid = false
			}
		}
		if valid {
			validOfflineMsg = append(validOfflineMsg, offlineMsgs[k])
		}
	}

	fmt.Println("valid msg:", validOfflineMsg)
	return validOfflineMsg

}

func SendOfflineMsg(validOfflineMsg []OfflineMsg) {
	for _, v := range validOfflineMsg {
		OfflineHandle(v)
	}
}

func OfflineHandle(msg OfflineMsg) {
	if ok := paramsCheck(msg.SignalType, signals); !ok {
		Log("Err", "offline msg signalType err: "+msg.SignalType)
		return
	}

	if ok := paramsCheck(msg.Engine, types); !ok {
		Log("Err", "offline msg engine err: "+msg.Engine)
		return
	}

	start := Start{}
	other := Base{msg.Engine, msg.Topic}

	if "start" == msg.SignalType && "rule" == msg.Engine {
		start = Start{Base{"rule", msg.Topic}, -1}
	}
	if "start" == msg.SignalType && "rule" != msg.Engine {
		start = Start{Base{msg.Engine, msg.Topic}, msg.Weight}
	}

	switch msg.SignalType {
	case "start":
		fmt.Println("start:", start)
		StartOfflineCh <- start

	case "stop":
		StopOfflineCh <- other

	case "shutdown":
		ShutdownOfflineCh <- other

	case "complete":
		CompleteOfflineCh <- other
	}
}

func paramsCheck(param string, options []string) bool {
	var ok bool
	for _, val := range options {
		if param == val {
			ok = true
			break
		}
	}

	return ok
}
