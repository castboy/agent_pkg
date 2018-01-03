package agent_pkg

import (
	"fmt"
	"log"

	"github.com/astaxie/beego/logs"
)

var exitInfo = "Shut down due to critical fault."
var Log *logs.BeeLogger

func InitLog(logName string) {
	Log = logs.NewLogger(1000)
	Log.SetLogger(logs.AdapterFile, fmt.Sprintf(`{"filename":"log/%s","level":7}`, logName))
}

func LogCrt(format string, v ...interface{}) {
	Log.Critical(format, v)
	log.Fatal(exitInfo)
}
