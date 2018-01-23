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

	"github.com/astaxie/beego/logs"
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
var ErrorOfflineCh = make(chan Base)
var ShutdownOfflineCh = make(chan Base)
var CompleteOfflineCh = make(chan Base)
var ReqCountCh = make(chan Base, 10000)

var beeLogs *logs.BeeLogger

func Handle(w http.ResponseWriter, r *http.Request) {
	HandleCh := make(chan *[][]byte)

	var (
		engine, topic string
		count         int
		err           error
	)

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
		Log.Warn("%s", "http req count is zero")
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
		ReqCountCh <- Base{engine, topic}
		RuleBindingReqCh <- RuleBindingReq{Base{engine, topic}, count, HandleCh}
		if len(RuleBindingReqCh) > 100 {
			Log.Info("Length of RuleBindingReqCh: %d", len(RuleBindingReqCh))
		}
	} else {
		ReqCountCh <- Base{Engine: engine}
		NormalReqCh <- NormalReq{engine, count, HandleCh}
		if len(NormalReqCh) > 100 {
			Log.Info("Length of NormalReqCh: %d", len(NormalReqCh))
		}
	}

	Data := <-HandleCh

	var data interface{}
	var dataSlice = make([]interface{}, 0)

	dataLen := len(*Data)
	for i := 0; i < dataLen; i++ {
		err := json.Unmarshal((*Data)[i], &data)
		if err != nil {
			Log.Error("%s", "json.Unmarshal err on data to response http req")
		}
		dataSlice = append(dataSlice, data)
	}

	if 0 != dataLen {
		data = dataSlice
	} else {
		data = nil
		time.Sleep(time.Duration(500) * time.Millisecond)
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

func Listen() {
	http.HandleFunc("/", Handle)
	http.ListenAndServe(":"+strconv.Itoa(AgentConf.EngineReqPort), nil)
}

func ReqCount() {
	initReqCountLog()

	count := make(map[string]int)
	ticker := time.NewTicker(time.Minute * time.Duration(5))
	for {
		select {
		case req := <-ReqCountCh:
			switch req.Engine {
			case "waf":
				count["waf"]++
			case "vds":
				count["vds"]++
			case "rule":
				count[req.Topic]++
			}
		case <-ticker.C:
			Log.Info("%s", "engine req count per 5 mins")
			ReqCountIntoFile(count)
			for k := range count {
				delete(count, k)
			}
		}
	}
}

func initReqCountLog() {
	beeLogs = logs.NewLogger(1000)
	beeLogs.SetLogger(logs.AdapterFile, `{"filename":"log/count/log","level":7}`)
}

func ReqCountIntoFile(count map[string]int) {
	cont := fmt.Sprintf("%s:   engine req count per 5 mins:     %v\n", time.Now().Format("2006-01-02 15:04:05"), count)
	beeLogs.Trace(cont)
}
