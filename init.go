//init.go

package agent_pkg

import (
	"encoding/json"
	"errors"
	"strconv"
)

type Status struct {
	First  int64
	Engine int64
	Err    int
	Cache  int64
	Last   int64
	Weight int
}

type StatusFromEtcd struct {
	//	ReceivedOfflineMsgOffset int64
	Status [3]map[string]Status
}

var statusFromEtcd StatusFromEtcd
var receivedOfflineMsgOffset int
var status = make(map[string]map[string]Status)
var err error
var wafTopic, vdsTopic string
var wafStartOffset, wafEndOffset, vdsStartOffset, vdsEndOffset int64

func InitVars() {
	wafTopic = AgentConf.Topic[0]
	vdsTopic = AgentConf.Topic[1]
	wafStartOffset, _ = broker.OffsetEarliest(wafTopic, int32(Partition))
	wafEndOffset, _ = broker.OffsetLatest(wafTopic, int32(Partition))
	vdsStartOffset, _ = broker.OffsetEarliest(vdsTopic, int32(Partition))
	vdsEndOffset, _ = broker.OffsetLatest(vdsTopic, int32(Partition))
}

func InitStatus() {
	Log.Info("%s", "init status")

	GetOfflineMsgOffset()

	Log.Info("Received Offline Task Msg Offset: %d", receivedOfflineMsgOffset)

	status["waf"][wafTopic] = Status{0, wafStartOffset, 0, 0, -1, 1}
	status["vds"][vdsTopic] = Status{0, vdsStartOffset, 0, 0, -1, 1}

	if (-1 != AgentConf.Offset[0]) && (wafStartOffset < AgentConf.Offset[0]) && (AgentConf.Offset[0] < wafEndOffset) {
		Log.Trace("waf offset is reset to: %d", AgentConf.Offset[0])
		status["waf"][wafTopic] = Status{0, AgentConf.Offset[0], 0, 0, -1, 1}
	}
	if (-1 != AgentConf.Offset[1]) && (vdsStartOffset < AgentConf.Offset[1]) && (AgentConf.Offset[1] < vdsEndOffset) {
		Log.Trace("vds offset is reset to: %s", AgentConf.Offset[1])
		status["vds"][vdsTopic] = Status{0, AgentConf.Offset[1], 0, 0, -1, 1}
	}
}

func InitStatusMap() {
	for _, v := range reqTypes {
		status[v] = make(map[string]Status)
	}
}

func GetStatusFromEtcd() error {
	Log.Info("%s", "get status from etcd")

	bytes, ok := EtcdGet("apt/agent/status/" + Localhost)
	if !ok {
		Log.Warn("%s", "can not get status from etcd")
		return errors.New("err")
	}

	err := json.Unmarshal(bytes, &statusFromEtcd)
	if err != nil {
		Log.Warn("%s", "parse status from etcd err")
		return errors.New("err")
	}

	status["waf"] = statusFromEtcd.Status[0]
	status["vds"] = statusFromEtcd.Status[1]
	status["rule"] = statusFromEtcd.Status[2]

	if (wafStartOffset > status["waf"][wafTopic].Engine) || (wafEndOffset < status["waf"][wafTopic].Engine) {
		Log.Trace("waf offset is reset to %d", wafStartOffset)
		status["waf"][wafTopic] = Status{0, wafStartOffset, 0, 0, -1, 1}
	}
	if (vdsStartOffset > status["vds"][vdsTopic].Engine) || (vdsEndOffset < status["vds"][vdsTopic].Engine) {
		Log.Trace("vds offset is reset to %d", vdsStartOffset)
		status["vds"][vdsTopic] = Status{0, vdsStartOffset, 0, 0, -1, 1}
	}

	GetOfflineMsgOffset()

	return nil
}

func GetOfflineMsgOffset() {
	bytes, ok := EtcdGet("apt/agent/offset/" + Localhost)
	if !ok {
		Log.Warn("%s", "can not get offset from etcd")
		receivedOfflineMsgOffset = -1
	} else {
		offset, err := strconv.Atoi(string(bytes))
		if nil != err {
			receivedOfflineMsgOffset = -1 //TODO
		} else {
			receivedOfflineMsgOffset = offset
		}
	}
	Log.Info("Received Offline Task Msg Offset is %d", receivedOfflineMsgOffset)
}

func RightStatus() {
	defer func() {
		if err := recover(); nil != err {
			LogCrt("PANIC in RightStatus(), %v", err)
		}
	}()

	InitVars()
	InitStatusMap()
	err = GetStatusFromEtcd()
	if nil != err {
		InitStatus()
	}
}
