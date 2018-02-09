//init.go

package agent_pkg

import (
	"encoding/json"
	"errors"
	"strconv"
	"time"

	"github.com/widuu/goini"
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

	for {
		wafStartOffset, err = broker.OffsetEarliest(wafTopic, int32(Partition))
		if nil != err {
			time.Sleep(time.Duration(500) * time.Millisecond)
		} else {
			break
		}
	}
	for {
		wafEndOffset, err = broker.OffsetLatest(wafTopic, int32(Partition))
		if nil != err {
			time.Sleep(time.Duration(500) * time.Millisecond)
		} else {
			break
		}
	}
	for {
		vdsStartOffset, err = broker.OffsetEarliest(vdsTopic, int32(Partition))
		if nil != err {
			time.Sleep(time.Duration(500) * time.Millisecond)
		} else {
			break
		}
	}
	for {
		vdsEndOffset, err = broker.OffsetLatest(vdsTopic, int32(Partition))
		if nil != err {
			time.Sleep(time.Duration(500) * time.Millisecond)
		} else {
			break
		}
	}
}

func InitStatus() {
	Log.Info("%s", "init status")

	GetOfflineMsgOffset()

	status["waf"][wafTopic] = Status{0, wafStartOffset, 0, 0, -1, 1}
	status["vds"][vdsTopic] = Status{0, vdsStartOffset, 0, 0, -1, 1}

	ResetOffsetWafVds()
}

func GetResetOffset() (int64, int64, error, error) {
	conf := goini.SetConfig("conf.ini")
	w := conf.GetValue("onlineOffset", "waf")
	v := conf.GetValue("onlineOffset", "vds")
	waf, wafErr := strconv.Atoi(w)
	vds, vdsErr := strconv.Atoi(v)

	if nil != wafErr {
		Log.Error("ResetOffset Waf Value Err: %s", w)
	}
	if nil != vdsErr {
		Log.Error("ResetOffset Vds Value Err: %s", v)
	}

	return int64(waf), int64(vds), wafErr, vdsErr
}

func ResetOffset(t string, oft int64) {
	Log.Trace("%s offset is reset to %d", t, oft)
	if "waf" == t {
		status["waf"][wafTopic] = Status{0, oft, 0, 0, -1, 1}
	} else {
		status["vds"][vdsTopic] = Status{0, oft, 0, 0, -1, 1}
	}
}

func ResetOffsetWafVds() {
	waf, vds, wafErr, vdsErr := GetResetOffset()
	if nil == wafErr {
		if -1 != waf {
			if wafStartOffset <= waf && waf <= wafEndOffset {
				ResetOffset("waf", waf)
			} else {
				Log.Warn("waf offset %d out of <%d %d>", waf, wafStartOffset, wafEndOffset)
			}
		}
	} else {
		Log.Warn("waf reset offset in conf.ini err, %s", wafErr.Error())
	}

	if nil == vdsErr {
		if -1 != vds {
			if vdsStartOffset <= vds && vds <= vdsEndOffset {
				ResetOffset("vds", vds)
			} else {
				Log.Warn("vds offset %d out of <%d %d>", vds, vdsStartOffset, vdsEndOffset)
			}
		}
	} else {
		Log.Warn("vds reset offset in conf.ini err, %s", vdsErr.Error())
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

	if wafStartOffset > status["waf"][wafTopic].Engine || wafEndOffset < status["waf"][wafTopic].Engine {
		ResetOffset("waf", wafStartOffset)
	}
	if vdsStartOffset > status["vds"][vdsTopic].Engine || vdsEndOffset < status["vds"][vdsTopic].Engine {
		ResetOffset("vds", vdsStartOffset)
	}

	ResetOffsetWafVds()

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
