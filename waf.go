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
)

type BzWaf struct {
	Daemon    bool     `json:"daemon"`
	Cup       []string `json:"cup"`
	Processes int      `json:"worker_processes"`
	Threads   int      `json:"worker_threads"`
	Rule      string   `json:"rules"`
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
	Log.Info("%s", "new waf instance begin")
	CopyPkg(src, dst, topic)
	AppendRule(dst, topic)
	ReqRule(dst, topic, srvIp, srvPort)
	JsonFile(dst, topic)
	NewWaf(dst, topic)
	fmt.Println("after NewWaf()")
	Log.Info("%s", "new waf instance ok")
}

func CopyPkg(src, dst, topic string) {
	dst = dst + "/" + topic
	err := os.MkdirAll(dst, 0777)
	if err != nil {
		LogCrt("create dir %s failed when copypkg in newWafInstance", dst)
	}

	Log.Info("create dir %s ok when newWafInstance", dst)

	copyDir(src, dst)

	dst = dst + "/conf/rules"
	err = os.MkdirAll(dst, 0777)
	if err != nil {
		LogCrt("create dir %s failed when copypkg in newWafInstance", dst)
	}
}

func AppendRule(instance, topic string) {
	file := fmt.Sprintf("%s/%s/conf/modsecurity.conf", instance, topic)
	cont := fmt.Sprintf("Include rules/%s.conf", topic)
	AppendWr(file, cont)
}

func ReqRule(instance, topic, srvIp string, srvPort int) {
	url := fmt.Sprintf("http://%s:%d/byzoro.apt.com/off-line-dispatch/rule/request?topic=%s", srvIp, srvPort, topic)

	res, err := http.Get(url)
	if nil != err {
		LogCrt("req rule url failed, %s", url)
	}

	defer res.Body.Close()

	body, err := ioutil.ReadAll(res.Body)

	var ruleRes struct {
		Rule string
		Cont string
	}

	err = json.Unmarshal(body, &ruleRes)
	if nil != err {
		LogCrt("req rule res err, %s", string(body))
	}

	ruleBytes, err := base64.StdEncoding.DecodeString(ruleRes.Cont)
	if nil != err {
		LogCrt("req rule res base64Decode err, %s", ruleRes.Cont)
	}

	dir := fmt.Sprintf("%s/%s/conf/rules", instance, topic)
	file := fmt.Sprintf("%s.conf", topic)

	ok := WriteFile(dir, file, []byte(ruleBytes))
	if !ok {
		LogCrt("write rule file to %s/%s failed", dir, file)
	}
}

func JsonFile(instance, topic string) {
	file := fmt.Sprintf("%s/%s/conf/modsecurity.conf", instance, topic)
	pid := fmt.Sprintf("%s/%s/conf/bz_waf.pid", instance, topic)
	url := fmt.Sprintf("http://localhost:%s/?type=rule&topic=%s&count=100",
		strconv.Itoa(AgentConf.EngineReqPort), topic)

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
		Logs: Logs{
			Debug:  false,
			Method: "POST",
			Url:    "http://192.168.146.128/test.php",
			Name:   "",
		},
	}

	bytes, err := json.Marshal(bzWaf)
	if nil != err {
		LogCrt("bz_waf.json, json.Marshal err %v", bzWaf)
	}

	dir := fmt.Sprintf("%s/%s/conf", instance, topic)
	ok := WriteFile(dir, "bz_waf.json", bytes)
	if !ok {
		LogCrt("%s", "write file bz_waf.json failed")
	}
}

func NewWaf(instance, topic string) {
	file := []string{"-c", fmt.Sprintf("%s/%s/conf/bz_waf.json", instance, topic)}
	ok := execCommand("/opt/bz_waf/bin/bz_waf", file)
	if !ok {
		LogCrt("exe newWafInstance failed, %s", topic)
	}
}

func KillWafInstance(instance, topic string) {
	Log.Info("Kill wafInstance, %s", topic)
	KillWaf(instance, topic)
	RmConf(instance, topic)
	Log.Info("INF", "Kill wafInstance ok, %s", topic)
}

func KillWaf(instance, topic string) {
	pidFile := fmt.Sprintf("%s/%s/conf/bz_waf.pid", instance, topic)
	cmd := exec.Command("cat", pidFile)
	out, err := cmd.CombinedOutput()
	if err != nil {
		LogCrt("pidFile %s not found", pidFile)
	}
	pid := string(out)

	cmd = exec.Command("kill", "-9", pid)
	out, err = cmd.CombinedOutput()
	if err != nil {
		LogCrt("can not kill newWafInstance, pid %s", pid)
	}
}

func RmConf(instance, topic string) {
	path := fmt.Sprintf("%s/%s", instance, topic)
	os.RemoveAll(path)
}
