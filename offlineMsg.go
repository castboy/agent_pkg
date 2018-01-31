package agent_pkg

import (
	"encoding/json"
	//	"log"
	"strconv"
	"time"
)

type OfflineMsg struct {
	Engine     string `json: "Engine"`
	Topic      string `json: "Topic"`
	Weight     int    `json: "Weight"`
	SignalType string `json: "SignalType"`
}

var signals = []string{"start", "stop", "complete", "shutdown", "error"}
var types = []string{"waf", "vds", "rule"}

var OfflineMsgExedCh = make(chan int, 100)
var ZeroOfflineMsgCh = make(chan int)

var (
	IsBoot      bool = true
	msgTotalNum int
	msgValidNum int
	msgExedNum  int
)

func OfflineMsgOffsetRecord() {
	for {
		select {
		case <-OfflineMsgExedCh:
			if IsBoot {
				msgExedNum++
				if msgValidNum == msgExedNum {
					receivedOfflineMsgOffset += msgTotalNum
					EtcdSet("apt/agent/offset/"+Localhost, strconv.Itoa(receivedOfflineMsgOffset))
					IsBoot = false
					Log.Info("OfflineMsgOffsetRecord, OfflineMsgExedCh, IsBoot = false, receivedOfflineMsgOffset = %d", receivedOfflineMsgOffset)
				}
			} else {
				receivedOfflineMsgOffset++
				EtcdSet("apt/agent/offset/"+Localhost, strconv.Itoa(receivedOfflineMsgOffset))
				Log.Info("OfflineMsgOffsetRecord, OfflineMsgExedCh, receivedOfflineMsgOffset = %d", receivedOfflineMsgOffset)
			}

		case <-ZeroOfflineMsgCh:
			receivedOfflineMsgOffset += msgTotalNum
			EtcdSet("apt/agent/offset/"+Localhost, strconv.Itoa(receivedOfflineMsgOffset))
			Log.Info("OfflineMsgOffsetRecord, ZeroOfflineMsgCh, receivedOfflineMsgOffset = %d", receivedOfflineMsgOffset)
		}
	}
}

func TimingGetOfflineMsg(second int) {
	defer func() {
		if err := recover(); nil != err {
			LogCrt("PANIC in TimingGetOfflineMsg(), %v", err)
		}
	}()

	consumer, err := InitConsumer(AgentConf.OfflineMsgTopic, int32(AgentConf.OfflineMsgPartion), int64(receivedOfflineMsgOffset+1))
	if nil != err {
		Log.Error("init consumer of %s failed", AgentConf.OfflineMsgTopic)
	}

	var msg OfflineMsg

	for {
		kafkaMsg, err := consumer.Consume()
		if nil != err {

		} else {
			err := json.Unmarshal(kafkaMsg.Value, &msg)
			if nil != err {
				Log.Error("wrong offline msg: %s", string(kafkaMsg.Value))
			} else {
				OfflineHandle(msg)
				//				receivedOfflineMsgOffset++
			}
		}

		time.Sleep(time.Duration(second) * time.Second)
	}
}

func CompensationOfflineMsg() {
	defer func() {
		if err := recover(); nil != err {
			LogCrt("PANIC in CompensationOfflineMsg(), %v", err)
		}
	}()

	msgs := LoadOfflineMsg()
	msgs = ExtractValidOfflineMsg(msgs)

	SendOfflineMsg(msgs)
}

func LoadOfflineMsg() (offlineMsgs []OfflineMsg) {
	var msg OfflineMsg

	consumer, err := InitConsumer(AgentConf.OfflineMsgTopic, int32(AgentConf.OfflineMsgPartion), int64(receivedOfflineMsgOffset+1))
	if nil != err {
		Log.Error("init consumer of %s failed", AgentConf.OfflineMsgTopic)
	}

	for {
		kafkaMsg, err := consumer.Consume()
		if nil != err {
			break
		} else {
			err := json.Unmarshal(kafkaMsg.Value, &msg)
			if nil != err {
				Log.Error("wrong offline msg: %s", string(kafkaMsg.Value))
			} else {
				offlineMsgs = append(offlineMsgs, msg)
			}
		}

		msgTotalNum++
	}

	Log.Info("all offline msg %v", offlineMsgs)
	return offlineMsgs
}

func ExtractValidOfflineMsg(offlineMsgs []OfflineMsg) []OfflineMsg {
	var invalidOfflineTask []string
	var invalidOfflineTaskId []int
	var validOfflineMsg []OfflineMsg

	for _, v := range offlineMsgs {
		if "shutdown" == v.SignalType || "error" == v.SignalType {
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

	Log.Info("valid offline msg: %v", validOfflineMsg)

	msgValidNum = len(validOfflineMsg)

	if 0 == msgValidNum {
		ZeroOfflineMsgCh <- 1
	}

	return validOfflineMsg

}

func SendOfflineMsg(validOfflineMsg []OfflineMsg) {
	for _, v := range validOfflineMsg {
		OfflineHandle(v)
	}
}

func OfflineHandle(msg OfflineMsg) {
	if ok := paramsCheck(msg.SignalType, signals); !ok {
		Log.Error("offline msg signalType err: %s", msg.SignalType)
		return
	}

	if ok := paramsCheck(msg.Engine, types); !ok {
		Log.Error("offline msg engine err: %s", msg.Engine)
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
		StartOfflineCh <- start

	case "stop":
		StopOfflineCh <- other

	case "error":
		ErrorOfflineCh <- other

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
