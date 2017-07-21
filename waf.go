package agent_pkg

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
)

type BzWaf struct {
	Daemon    bool     `json:"daemon"`
	Cup       []string `json:"cup"`
	Processes int      `json:"worker_processes"`
	Threads   int      `json:"worker_threads"`
	Rule      string   `json:"rule"`
	Pid       string   `json:"pid"`
	Resource  Resource `json:"resource"`
	Logs      Logs     `json:"logs"`
}

type Resource struct {
	Method string `json:"method"`
	Url    string `json:"url"`
}

type Logs struct {
	Debug  bool   `json:"debug"`
	Method string `json:"method"`
	Url    string `json:"url"`
	Name   string `json:"data_field_name"`
}

func NewWafInstance(src, dst, topic, srvIp string, srvPort int) {
	CopyPkg(src, dst, topic)
	AppendRule(dst, topic)
	ReqRule(dst, topic, srvIp, srvPort)
	JsonFile(dst, topic)
	NewWaf(dst, topic)
}

func CopyPkg(src, dst, topic string) {
	dst = dst + "/" + topic
	err := os.MkdirAll(dst, 0777)
	if err != nil {
		fmt.Printf("%s", err.Error())
	} else {
		fmt.Print("Create Directory OK!")
	}

	dst = dst + "/" + topic + "/conf/rule"
	err = os.MkdirAll(dst, 0777)
	if err != nil {
		fmt.Printf("%s", err.Error())
	} else {
		fmt.Print("Create Directory OK!")
	}

	copyDir(src, dst)
}

func AppendRule(instance, topic string) {
	file := fmt.Sprintf("%s/%s/conf/modsecurity.conf", instance, topic)
	cont := fmt.Sprintf("Include rules/%s.conf", topic)
	AppendWr(file, cont)
}

func ReqRule(instance, topic, srvIp string, srvPort int) {
	url := fmt.Sprintf("http://%s:%d/offlineRule?task=%s", srvIp, srvPort, topic)
	res, err := http.Get(url)
	if nil != err {
		errLog := fmt.Sprintf("RuleReq Err: %s", err.Error())
		Log("Err", errLog)
	}

	defer res.Body.Close()

	body, err := ioutil.ReadAll(res.Body)

	var ruleRes struct {
		Task string
		Rule string
	}

	json.Unmarshal(body, &ruleRes)

	rule := ruleRes.Rule

	dir := fmt.Sprintf("%s/%s", instance, topic)
	file := fmt.Sprintf("%s.conf", topic)
	ok := WriteFile(dir, file, []byte(rule))
	if !ok {

	}
}

func JsonFile(instance, topic string) {
	file := fmt.Sprintf("%s/%s/conf/modsecurity.conf", instance, topic)
	pid := fmt.Sprintf("%s/%s/conf/bz_waf.pid", instance, topic)
	url := fmt.Sprintf("http://localhost:8081/?type=waf&task=%s&count=num", topic)

	bzWaf := BzWaf{
		Daemon:    false,
		Cup:       []string{},
		Processes: 1,
		Threads:   1,
		Rule:      file,
		Pid:       pid,
		Resource: Resource{
			Method: "GET",
			Url:    url,
		},
		Logs: Logs{
			Debug:  false,
			Method: "POST",
			Url:    "http://192.168.146.128/test.php",
			Name:   "",
		},
	}

	bytes, err := json.Marshal(bzWaf)
	if nil != err {
		fmt.Printf("json.Marshal Err: %s", err.Error())
	}

	dir := fmt.Sprintf("%s/%s/conf", instance, topic)
	ok := WriteFile(dir, "bz_waf.json", bytes)
	if !ok {

	}
}

func NewWaf(instance, topic string) {
	file := []string{fmt.Sprintf("%s/%s/conf/bz_waf.json", instance, topic)}
	ok := execCommand("/opt/bz_beta/bin/bz_waf -c ", file)
	if !ok {

	}
}

func KillWafInstance(instance, topic string) {
	KillWaf(instance, topic)
	RmConf(topic)
}

func KillWaf(instance, topic string) {
	sh := fmt.Sprintf("cat %s/%s/conf/bz_waf.pid | kill -9", instance, topic)
	ok := execCommand(sh, []string{})
	if !ok {

	}
}

func RmConf(topic string) {
	os.RemoveAll(topic)
}
