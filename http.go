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
	ErrNum       int64
}

var NormalReqCh = make(chan NormalReq, 10000)
var RuleBindingReqCh = make(chan RuleBindingReq, 10000)

var PrefetchResCh = make(chan PrefetchRes)
var RdHdfsResCh = make(chan RdHdfsRes)

var StartOfflineCh = make(chan Start)
var StopOfflineCh = make(chan Base)
var ShutdownOfflineCh = make(chan Base)
var CompleteOfflineCh = make(chan Base)

func Handle(w http.ResponseWriter, r *http.Request) {
	//match, _ := regexp.MatchString("/?type=(waf|vds)&count=([0-9]+$)", r.RequestURI)
	//if !match {
	//	io.WriteString(w, "Usage of: http://ip:port/?type=waf/vds&count=num")
	//	return
	//}

	HandleCh := make(chan *[][]byte)

	var engine, topic string
	var count int
	r.ParseForm()
	if _, ok := r.Form["type"]; ok {
		engine = r.Form["type"][0]
	}
	if _, ok := r.Form["count"]; ok {
		count, _ = strconv.Atoi(r.Form["count"][0])
	}
	if _, ok := r.Form["topic"]; ok {
		topic = r.Form["topic"][0]
	}

	var isNormalReq bool
	if "" == topic {
		isNormalReq = true
	}

	if isNormalReq {
		NormalReqCh <- NormalReq{engine, count, HandleCh}
		fmt.Println("Length of NormalReqCh:", len(NormalReqCh))
	} else {
		RuleBindingReqCh <- RuleBindingReq{Base{engine, topic}, count, HandleCh}
		fmt.Println("Length of RuleBindingReqCh:", len(RuleBindingReqCh))
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
			WriteCacheAndUpdateCacheCurrent(res)

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
	var signal, engine, topic, task string

	if val, ok := r.Form["signal"]; ok {
		signal = val[0]
	}
	if val, ok := r.Form["engine"]; ok {
		engine = val[0]
	}
	if val, ok := r.Form["topic"]; ok {
		topic = val[0]
	}
	if val, ok := r.Form["task"]; ok {
		task = val[0]
	}
	if val, ok := r.Form["weight"]; ok {
		weight, err = strconv.Atoi(val[0])
		if nil != err {
			weight = 1
		}
	}

	msg := Base{engine, topic}

	switch signal {
	case "start":
		if "ruleBinding" == task {
			msg := Start{Base{"rule", topic}, weight}
			StartOfflineCh <- msg
			NewWafInstance("/home/NewWafInstance/src", "/home/NewWafInstance", msg.Base.Topic, "10.88.1.103", 8091)
		} else {
			msg := Start{Base{engine, topic}, weight}
			StartOfflineCh <- msg
		}

	case "stop":
		StopOfflineCh <- msg

	case "shutdown":
		ShutdownOfflineCh <- msg
		if "ruleBinding" == task {
			KillWafInstance("instance", msg.Topic)
		}

	case "complete":
		CompleteOfflineCh <- msg
		if "ruleBinding" == task {
			KillWafInstance("/home/NewWafInstance", msg.Topic)
		}
	}

	fmt.Printf("signal:%s; engine:%s; topic:%s; weight:%d; task:%s\n",
		signal, engine, topic, weight, task)
}

func ListenReq(url string) {
	http.HandleFunc("/", Handle)
	http.HandleFunc("/offline", OfflineHandle)
	http.ListenAndServe(url, nil)
}
