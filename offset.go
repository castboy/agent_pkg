package agent_pkg

import (
	"fmt"
	"os/exec"
	"strconv"
	"strings"
)

func ResetOffsetInConfFile() {
	conf := "conf.ini"

	line := fmt.Sprintf("sed -n '/offlineOffset/=' %s", conf)
	out, err := exec.Command("sh", "-c", line).Output()
	s := strings.Replace(string(out), "\n", "", -1)
	i, err := strconv.Atoi(s)

	waf := i + 1
	vds := waf + 1

	r := fmt.Sprintf("sed -i '%dc waf = -1' %s", waf, conf)
	out, err = exec.Command("sh", "-c", r).Output()
	if nil != err {
		fmt.Println(err)
	} else {
		fmt.Println(string(out))
	}

	r = fmt.Sprintf("sed -i '%dc vds = -1' %s", vds, conf)
	out, err = exec.Command("sh", "-c", r).Output()
	if nil != err {
		fmt.Println(err)
	} else {
		fmt.Println(string(out))
	}
}
