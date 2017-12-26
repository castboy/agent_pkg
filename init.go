//init.go

package agent_pkg

import (
	"encoding/json"
	"errors"
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
	ReceivedOfflineMsgOffset int64
	Status                   [3]map[string]Status
}

var statusFromEtcd StatusFromEtcd
var receivedOfflineMsgOffset int64
var status = make(map[string]map[string]Status)
var err error

func InitStatus() {
	Log("INF", "%s", "init status")

	wafTopic := AgentConf.Topic[0]
	vdsTopic := AgentConf.Topic[1]

        start, _ := broker.OffsetEarliest(AgentConf.OfflineMsgTopic, int32(AgentConf.OfflineMsgPartion))

	receivedOfflineMsgOffset = int64(start-1)

	Log("INF", "Received Offline Task Msg Offset: %d", receivedOfflineMsgOffset)

	InitStatusMap()

        wafStartOffset, _ := broker.OffsetEarliest(wafTopic, int32(Partition))
        wafEndOffset, _ := broker.OffsetLatest(wafTopic, int32(Partition))
        vdsStartOffset, _ := broker.OffsetEarliest(vdsTopic, int32(Partition))
        vdsEndOffset, _ := broker.OffsetLatest(vdsTopic, int32(Partition))

	status["waf"][wafTopic] = Status{0, wafStartOffset, 0, 0, -1, 1}

	status["vds"][vdsTopic] = Status{0, vdsStartOffset, 0, 0, -1, 1}

	if (-1 != AgentConf.Offset[0]) && (wafStartOffset < AgentConf.Offset[0]) && (AgentConf.Offset[0] < wafEndOffset) {
		Log("TRC", "waf offset is reset to: %d", AgentConf.Offset[0])
		status["waf"][wafTopic] = Status{0, AgentConf.Offset[0], 0, 0, -1, 1}
	}
	if (-1 != AgentConf.Offset[1]) && (vdsStartOffset < AgentConf.Offset[1]) && (AgentConf.Offset[1] < vdsEndOffset){
		Log("TRC", "vds offset is reset to: %d", AgentConf.Offset[1])
		status["vds"][vdsTopic] = Status{0, AgentConf.Offset[1], 0, 0, -1, 1}
	}
}

func InitStatusMap() {
	for _, v := range reqTypes {
		status[v] = make(map[string]Status)
	}
}

func GetStatusFromEtcd() error {
	Log("INF", "%s", "get status from etcd")

	bytes, ok := EtcdGet("apt/agent/status/" + Localhost)
	if !ok {
		Log("WRN", "%s", "can not get status from etcd")
		return errors.New("err")
	}

	err := json.Unmarshal(bytes, &statusFromEtcd)
	if err != nil {
		Log("WRN", "%s", "parse status from etcd err")
		return errors.New("err")
	}

	InitStatusMap()

	status["waf"] = statusFromEtcd.Status[0]
	status["vds"] = statusFromEtcd.Status[1]
	status["rule"] = statusFromEtcd.Status[2]

	if -1 != AgentConf.Offset[0] {
		Log("TRC", "%s %d", "waf offset is reset to", AgentConf.Offset[0])
		status["waf"][AgentConf.Topic[0]] = Status{0, AgentConf.Offset[0], 0, 0, -1, 1}
	}
	if -1 != AgentConf.Offset[1] {
		Log("TRC", "%s %d", "vds offset is reset to", AgentConf.Offset[1])
		status["vds"][AgentConf.Topic[1]] = Status{0, AgentConf.Offset[1], 0, 0, -1, 1}
	}

	receivedOfflineMsgOffset = statusFromEtcd.ReceivedOfflineMsgOffset
	Log("INF", "Received Offline Task Msg Offset is %d", receivedOfflineMsgOffset)

	return nil
}

func RightStatus() {
	err = GetStatusFromEtcd()
	if nil != err {
		InitStatus()
	}
}
