//http.go

package agent_pkg

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strconv"
)

type NormalReqMsg struct {
	Engine   string
	Count    int
	HandleCh chan *[][]byte
}

type RuleBindingReqMsg struct {
	Engine   string
	Count    int
	Topic    string
	HandleCh chan *[][]byte
}

type HttpRes struct {
	Code int
	Data interface{}
	Num  int
}

type StartOfflineMsg struct {
	Engine string
	Topic  string
	Weight int
}

type OtherOfflineMsg struct {
	Engine string
	Topic  string
}

type PrefetchResMsg struct {
	Engine  string
	Topic   string
	DataPtr *[][]byte
}

type RdHdfsResMsg struct {
	Engine       string
	Topic        string
	PrefetchNum  int
	CacheDataPtr *[][]byte
	ErrNum       int64
}

var NormalReqCh = make(chan NormalReqMsg, 10000)
var RuleBindingReqCh = make(chan RuleBindingReqMsg, 10000)

var PrefetchResCh = make(chan PrefetchResMsg)
var RdHdfsResCh = make(chan RdHdfsResMsg)

var StartOfflineCh = make(chan StartOfflineMsg)
var StopOfflineCh = make(chan OtherOfflineMsg)
var ShutdownOfflineCh = make(chan OtherOfflineMsg)
var CompleteOfflineCh = make(chan OtherOfflineMsg)

func Handle(w http.ResponseWriter, r *http.Request) {

	match, _ := regexp.MatchString("/?type=(waf|vds)&count=([0-9]+$)", r.RequestURI)
	if !match {
		io.WriteString(w, "Usage of: http://ip:port/?type=waf/vds&count=num")
		return
	}

	HandleCh := make(chan *[][]byte)

	r.ParseForm()

	Engine := r.Form["type"][0]
	Count, _ := strconv.Atoi(r.Form["count"][0])

	var isNormalReq bool
	paramsNum := len(r.Form)
	if 2 == paramsNum {
		isNormalReq = true
	}

	if isNormalReq {
		NormalReqCh <- NormalReqMsg{Engine, Count, HandleCh}
		fmt.Println("Length of ManageCh:", len(NormalReqCh))
	} else {
		Topic := r.Form["topic"][0]
		RuleBindingReqCh <- RuleBindingReqMsg{Engine, Count, Topic, HandleCh}
	}

	Data := <-HandleCh
	fmt.Println("http.go <-HandleCh")

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

	res := HttpRes{
		Code: 10000,
		Data: dataSlice,
		Num:  dataLen,
	}

	if dataLen == 0 {
		res = HttpRes{
			Code: 10000,
			Data: nil,
			Num:  dataLen,
		}
	}

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
	signal := r.Form["type"][0]
	engine := r.Form["engine"][0]
	topic := r.Form["topic"][0]
	task := r.Form["task"][0]

	msg := OtherOfflineMsg{
		Engine: engine,
		Topic:  topic,
	}

	var weight int
	switch signal {
	case "start":
		weight, _ = strconv.Atoi(r.Form["weight"][0])
		msg := StartOfflineMsg{
			Engine: engine,
			Topic:  topic,
			Weight: weight,
		}

		StartOfflineCh <- msg

	case "stop":
		StopOfflineCh <- msg

	case "shutdown":
		ShutdownOfflineCh <- msg

	case "complete":
		CompleteOfflineCh <- msg
	}

	fmt.Sprintf("signal:%s; engine:%s; topic:%s; weight:%s; task:%s",
		signal, engine, topic, weight, task)
}

func ListenReq(url string) {
	http.HandleFunc("/", Handle)
	http.HandleFunc("/offline", OfflineHandle)
	http.ListenAndServe(url, nil)
}
