//http.go

package agent_pkg

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	//"regexp"
	"strconv"
	"time"
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

var ReqCount int

func Handle(w http.ResponseWriter, r *http.Request) {
	ReqCount++
	fmt.Println("ReqCount:", ReqCount)

	HandleCh := make(chan *[][]byte)

	var engine, topic string
	var count int
	var err error

	r.ParseForm()

	if val, ok := r.Form["type"]; ok {
		engine = val[0]
	}
	if ok := paramsCheck(engine, types); !ok {
		io.WriteString(w, "params `type` err\n")
		return
	}

	if val, ok := r.Form["count"]; ok {
		count, err = strconv.Atoi(val[0])
		if nil != err {
			count = 100
			io.WriteString(w, "params `count` err, set `100` default\n")
		}
	}
	if 0 == count {
		fmt.Println("warning: http req count is zero")
	}

	if val, ok := r.Form["topic"]; ok {
		topic = val[0]
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
		time.Sleep(time.Duration(10) * time.Millisecond)
	}

	res := struct {
		Code int
		Data interface{}
		Num  int
	}{10000, data, dataLen}

	byte, _ := json.Marshal(res)

	io.WriteString(w, string(byte))
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
	http.ListenAndServe(url, nil)
}
