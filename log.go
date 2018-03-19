package agent_pkg

import (
	"fmt"
	"log"

	"github.com/astaxie/beego/logs"
	"github.com/widuu/goini"
)

var exitInfo = "Shut down due to critical fault."
var Log *logs.BeeLogger

func InitLog(logName string) {
	defer func() {
		if err := recover(); nil != err {
			LogCrt("PANIC in InitLog(), %v", err)
		}
	}()

	Log = logs.NewLogger(1000)
	Log.SetLogger(logs.AdapterFile, fmt.Sprintf(`{"filename":"log/%s","level":7}`, logName))

	conf := goini.SetConfig("conf.ini")

	switch conf.GetValue("log", "level") {
	case "trace":
		Log.SetLevel(logs.LevelTrace)
	case "info":
		Log.SetLevel(logs.LevelInfo)
	case "warn":
		Log.SetLevel(logs.LevelWarn)
	case "error":
		Log.SetLevel(logs.LevelError)
	case "critical":
		Log.SetLevel(logs.LevelCritical)
	default:
		Log.SetLevel(logs.LevelInfo)
	}
}

func LogCrt(format string, v ...interface{}) {
	Log.Critical(format, v...)
	log.Fatal(exitInfo)
}
