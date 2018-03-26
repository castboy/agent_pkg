package agent_pkg

import (
	"fmt"
	"runtime"
	"strconv"

	"github.com/widuu/goini"
)

func cpuNum() int {
	conf := goini.SetConfig("conf.ini")
	cpu, err := strconv.Atoi(conf.GetValue("other", "cpu"))
	if nil != err {
		fmt.Println(err.Error())
	}

	return cpu
}

func LimitCpuNum() {
	num := cpuNum()
	if -1 != num {
		runtime.GOMAXPROCS(num)
	}
}
