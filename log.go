package agent_pkg

import (
	"log"

	"github.com/astaxie/beego/logs"
)

var exitInfo = "Shut down due to critical fault."
var Log *logs.BeeLogger

func InitLog() {
	Log = logs.NewLogger(1000)
	Log.SetLogger(logs.AdapterFile, `{"filename":"log/log","level":7}`)
}

func LogCrt(format string, v ...interface{}) {
	Log.Critical(format, v)
	log.Fatal(exitInfo)
}
