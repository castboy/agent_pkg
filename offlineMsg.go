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

var (
	IsBoot      bool = true
	msgTotalNum int
	msgExedNum  int
)

func OfflineMsgOffsetRecord() {
	for {
		<-OfflineMsgExedCh
		receivedOfflineMsgOffset++
		EtcdSet("apt/agent/offset/"+Localhost, strconv.Itoa(receivedOfflineMsgOffset))

		msgExedNum++
		if msgTotalNum == msgExedNum {
			break
		}
	}
}

func CollectOfflineMsgExedRes() {
	for {
		<-OfflineMsgExedCh
		receivedOfflineMsgOffset++
		EtcdSet("apt/agent/offset/"+Localhost, strconv.Itoa(receivedOfflineMsgOffset))
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

	if 0 != msgTotalNum {
		SendOfflineMsg(msgs)
		OfflineMsgOffsetRecord()
	}

	Log.Info("CompensationOfflineMsg complete, current receivedOfflineMsgOffset: %d", receivedOfflineMsgOffset)
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

	Log.Info("LoadOfflineMsg, msgTotalNum: %d,  msgs %v", msgTotalNum, offlineMsgs)
	return offlineMsgs
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

	OfflineMsgCh <- msg

	Log.Info("OfflineMsgCh <- msg, msg %v", msg)
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
