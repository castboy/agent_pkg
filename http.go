//http.go

package agent_pkg

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	//"regexp"
	"strconv"
)

type Base struct {
	Engine string
	Topic  string
}

type Start struct {
	Base
	Weight int
}

type NormalReq struct {
	Engine   string
	Count    int
	HandleCh chan *[][]byte
}

type RuleBindingReq struct {
	Base
	Count    int
	HandleCh chan *[][]byte
}

type PrefetchRes struct {
	Base
	DataPtr *[][]byte
}

type RdHdfsRes struct {
	Base
	PrefetchNum  int
	CacheDataPtr *[][]byte
	ErrNum       int
}

var NormalReqCh = make(chan NormalReq, 10000)
var RuleBindingReqCh = make(chan RuleBindingReq, 10000)

var PrefetchResCh = make(chan PrefetchRes)
var RdHdfsResCh = make(chan RdHdfsRes)

var StartOfflineCh = make(chan Start)
var StopOfflineCh = make(chan Base)
var ShutdownOfflineCh = make(chan Base)
var CompleteOfflineCh = make(chan Base)

var signals = []string{"start", "stop", "complete", "shutdown"}
var types = []string{"waf", "vds", "rule"}

func Handle(w http.ResponseWriter, r *http.Request) {
	HandleCh := make(chan *[][]byte)

	var engine, topic string
	var count int
	var err error

	r.ParseForm()

	if _, ok := r.Form["type"]; ok {
		engine = r.Form["type"][0]
	}
	if ok := paramsCheck(engine, types); !ok {
		io.WriteString(w, "params `type` err\n")
		return
	}

	if _, ok := r.Form["count"]; ok {
		count, err = strconv.Atoi(r.Form["count"][0])
		if nil != err {
			count = 100
			io.WriteString(w, "params `count` err, set `100` default\n")
		}
	}

	if _, ok := r.Form["topic"]; ok {
		topic = r.Form["topic"][0]
	}

	var isRuleBindingReq bool
	if "rule" == engine {
		if topicIsExist(topic) {
			isRuleBindingReq = true
		} else {
			return
		}
	}

	if isRuleBindingReq {
		RuleBindingReqCh <- RuleBindingReq{Base{engine, topic}, count, HandleCh}
		fmt.Println("Length of RuleBindingReqCh:", len(RuleBindingReqCh))
	} else {
		NormalReqCh <- NormalReq{engine, count, HandleCh}
		fmt.Println("Length of NormalReqCh:", len(NormalReqCh))
	}

	Data := <-HandleCh
	fmt.Println("Data := <-HandleCh")

	var data interface{}
	var dataSlice = make([]interface{}, 0)

	dataLen := len(*Data)
	for i := 0; i < dataLen; i++ {
		err := json.Unmarshal((*Data)[i], &data)
		if err != nil {
			fmt.Println("Unmarshal Error")
		}
		dataSlice = append(dataSlice, data)
	}

	if 0 != dataLen {
		data = dataSlice
	} else {
		data = nil
	}

	res := struct {
		Code int
		Data interface{}
		Num  int
	}{10000, data, dataLen}

	byte, _ := json.Marshal(res)

	io.WriteString(w, string(byte))
}

func Manage() {
	for {
		select {
		case req := <-NormalReqCh:
			DisposeNormalReq(req)

		case req := <-RuleBindingReqCh:
			DisposeRuleBindingReq(req)

		case res := <-PrefetchResCh:
			go RdHdfs(res)

		case res := <-RdHdfsResCh:
			WriteBufferAndUpdateBufferOffset(res)

		case start := <-StartOfflineCh:
			StartOffline(start)

		case stop := <-StopOfflineCh:
			StopOffline(stop)

		case shutdown := <-ShutdownOfflineCh:
			ShutdownOffline(shutdown)

		case complete := <-CompleteOfflineCh:
			CompleteOffline(complete)
		}
	}
}

func OfflineHandle(w http.ResponseWriter, r *http.Request) {
	r.ParseForm()

	var weight int
	var err error
	var signal, engine, topic string

	if val, ok := r.Form["signal"]; ok {
		signal = val[0]
	}
	if ok := paramsCheck(signal, signals); !ok {
		io.WriteString(w, "params `signal` err\n")
		return
	}

	if val, ok := r.Form["type"]; ok {
		engine = val[0]
	}
	if ok := paramsCheck(engine, types); !ok {
		io.WriteString(w, "params `type` err\n")
		return
	}

	if val, ok := r.Form["topic"]; ok {
		topic = val[0]
	}
	if "" == topic {
		io.WriteString(w, "params `topic` is needed\n")
		return
	}

	if val, ok := r.Form["weight"]; ok {
		weight, err = strconv.Atoi(val[0])
		if nil != err {
			weight = 1
			io.WriteString(w, "params `weight` err, set `1` default\n")
		}
	}

	start := Start{}
	other := Base{engine, topic}

	if "start" == signal && "rule" == engine {
		start = Start{Base{"rule", topic}, -1}
	}
	if "start" == signal && "rule" != engine {
		start = Start{Base{engine, topic}, weight}
	}

	switch signal {
	case "start":
		StartOfflineCh <- start
		if "rule" == engine {
			NewWafInstance("/home/NewWafInstance/src", "/home/NewWafInstance", start.Base.Topic, "10.88.1.103", 8091)
		}

	case "stop":
		StopOfflineCh <- other

	case "shutdown":
		ShutdownOfflineCh <- other
		if "rule" == engine {
			KillWafInstance("instance", other.Topic)
		}

	case "complete":
		CompleteOfflineCh <- other
		if "rule" == engine {
			KillWafInstance("/home/NewWafInstance", other.Topic)
		}
	}

	res := fmt.Sprintf("received offline msg below: signal-%s type-%s topic-%s weight-%d\n",
		signal, engine, topic, weight)

	io.WriteString(w, res)
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

func topicIsExist(topic string) bool {
	var ok bool
	for key, _ := range status["rule"] {
		if topic == key {
			ok = true
			break
		}
	}

	return ok
}

func ListenReq(url string) {
	http.HandleFunc("/", Handle)
	http.HandleFunc("/offline", OfflineHandle)
	http.ListenAndServe(url, nil)
}
