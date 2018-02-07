package agent_pkg

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io/ioutil"
	//	"log"
	"net/http"
	"os"
	"os/exec"
	"strconv"
	"time"

	"github.com/widuu/goini"
)

type BzWaf struct {
	Daemon    bool     `json:"daemon"`
	Cup       []string `json:"cup"`
	Processes int      `json:"worker_processes"`
	Threads   int      `json:"worker_threads"`
	Rule      string   `json:"rules"`
	Pid       string   `json:"pid"`
	Resource  Resource `json:"resource"`
	Alert     Alert    `json:"alert"`
	Logs      []Lg     `json:"logs"`
}

type Resource struct {
	Method string `json:"method"`
	Url    string `json:"url"`
}

type Alert struct {
	Debug  bool   `json:"debug"`
	Method string `json:"method"`
	Url    string `json:"url"`
	Name   string `json:"data_field_name"`
}

type Lg struct {
	Level  string `json:"level"`
	LgFile string `json:"log_file"`
}

func NewWafInstance(src, dst, topic, srvIp string, srvPort int) {
	Log.Info("new waf instance begin: %s", topic)

	if nil != CopyPkg(src, dst, topic) {
		return
	}

	if nil != AppendRule(dst, topic) {
		return
	}

	if nil != ReqRule(dst, topic, srvIp, srvPort) {
		return
	}

	if nil != JsonFile(dst, topic) {
		return
	}

	if nil != NewWaf(dst, topic) {
		return
	}

	Log.Info("%s", "new waf instance end: %s", topic)
}

func CopyPkg(src, dst, topic string) error {
	dst = dst + "/" + topic
	err := os.MkdirAll(dst, 0777)
	if err != nil {
		Log.Error("create dir %s failed when copypkg in newWafInstance", dst)
		return err
	}

	Log.Info("create dir %s ok when newWafInstance", dst)

	err = copyDir(src, dst)
	if err != nil {
		return err
	}

	dst = dst + "/conf/rules"
	err = os.MkdirAll(dst, 0777)
	if err != nil {
		Log.Error("create dir %s failed when copypkg in newWafInstance", dst)
		return err
	}

	return nil
}

func AppendRule(instance, topic string) error {
	file := fmt.Sprintf("%s/%s/conf/modsecurity.conf", instance, topic)
	cont := fmt.Sprintf("Include rules/%s.conf", topic)

	return AppendWr(file, cont)
}

func ReqRule(instance, topic, srvIp string, srvPort int) error {
	Log.Info("ReqRule start: %s", topic)
	url := fmt.Sprintf("http://%s:%d/byzoro.apt.com/off-line-dispatch/rule/request?topic=%s", srvIp, srvPort, topic)

	var res *http.Response
	var err error

	for i := 0; i < 40; i++ {
		res, err = http.Get(url)
		if nil != err {
			time.Sleep(time.Duration(5) * time.Second)
		} else {
			break
		}
	}

	if nil != err {
		Log.Error("req rule url failed, %s", url, err.Error())
		return err
	}

	defer res.Body.Close()

	body, err := ioutil.ReadAll(res.Body)
	if nil != err {
		return err
	}

	var ruleRes struct {
		Rule string
		Cont string
	}

	err = json.Unmarshal(body, &ruleRes)
	if nil != err {
		return err
	}

	ruleBytes, err := base64.StdEncoding.DecodeString(ruleRes.Cont)
	if nil != err {
		Log.Error("req rule res base64Decode err, %s", ruleRes.Cont)
		return err
	}

	dir := fmt.Sprintf("%s/%s/conf/rules", instance, topic)
	file := fmt.Sprintf("%s.conf", topic)

	err = WriteFile(dir, file, []byte(ruleBytes))
	if nil != err {
		return err
	}

	Log.Info("ReqRule end: %s", topic)
	return nil
}

func AlertUploadUrl() string {
	conf := goini.SetConfig("conf.ini")
	host := conf.GetValue("preproccess", "url")
	if nil != err {
		Log.Error("UNKNOW PREPROCESS HOST, %s", err.Error())
	}

	return fmt.Sprintf("http://%s:8092/alert/waf", host)
}

func JsonFile(instance, topic string) error {
	file := fmt.Sprintf("%s/%s/conf/modsecurity.conf", instance, topic)
	pid := fmt.Sprintf("%s/%s/conf/bz_waf.pid", instance, topic)
	url := fmt.Sprintf("http://localhost:%s/?type=rule&topic=%s&count=100",
		strconv.Itoa(AgentConf.EngineReqPort), topic)
	log := fmt.Sprintf("/home/waf_instance/log/%s", topic)

	bzWaf := BzWaf{
		Daemon:    true,
		Cup:       []string{},
		Processes: 1,
		Threads:   1,
		Rule:      file,
		Pid:       pid,
		Resource: Resource{
			Method: "GET",
			Url:    url,
		},
		Alert: Alert{
			Debug:  false,
			Method: "POST",
			Url:    AlertUploadUrl(),
			Name:   "",
		},
		Logs: []Lg{{Level: "error", LgFile: log}},
	}

	bytes, err := json.Marshal(bzWaf)
	if nil != err {
		Log.Error("bz_waf.json, json.Marshal err %v", bzWaf)
		return err
	}

	dir := fmt.Sprintf("%s/%s/conf", instance, topic)
	err = WriteFile(dir, "bz_waf.json", bytes)
	if nil != err {
		Log.Error("%s", "write file bz_waf.json failed")
		return err
	}

	return nil
}

func NewWaf(instance, topic string) error {
	Log.Info("NewWaf start: %s", topic)

	file := []string{"-c", fmt.Sprintf("%s/%s/conf/bz_waf.json", instance, topic)}
	bzWaf := fmt.Sprintf("%s/bz_waf/bin/bz_waf", os.Getenv("APT_HOME"))

	err := execCommand(bzWaf, file)
	if nil != err {
		Log.Error("exe newWafInstance failed, %s", topic)
		return err
	}

	Log.Info("NewWaf end: %s", topic)
	return nil
}

func KillWafInstance(instance, topic string) {
	Log.Info("Kill wafInstance, %s", topic)
	KillWaf(instance, topic)
	RmConf(instance, topic)
	Log.Info("Kill wafInstance ok, %s", topic)
}

func KillWaf(instance, topic string) {
	pidFile := fmt.Sprintf("%s/%s/conf/bz_waf.pid", instance, topic)
	cmd := exec.Command("cat", pidFile)
	out, err := cmd.CombinedOutput()
	if err != nil {
		Log.Error("pidFile %s not found", pidFile)
	}
	pid := string(out)

	cmd = exec.Command("kill", "-TERM", pid)
	out, err = cmd.CombinedOutput()
	if err != nil {
		Log.Error("can not kill newWafInstance, pid %s, err %s", pid, err.Error())
	}
}

func RmConf(instance, topic string) {
	path := fmt.Sprintf("%s/%s", instance, topic)
	os.RemoveAll(path)
}
