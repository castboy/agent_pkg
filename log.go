package agent_pkg

import (
	"log"

	seelog "github.com/cihub/seelog"
)

func InitLog() {
	logger, err := seelog.LoggerFromConfigAsFile("seelog.xml")

	if err != nil {
		log.Fatal("Log Configuration File Does Not Exist")
	}
	seelog.ReplaceLogger(logger)
}

func Log(level string, format string, s ...interface{}) {
	defer seelog.Flush()

	switch level {
	case "TRC":
		seelog.Tracef(format, s)
	case "DBG":
		seelog.Debugf(format, s)
	case "INF":
		seelog.Infof(format, s)
	case "WRN":
		seelog.Warnf(format, s)
	case "ERR":
		seelog.Errorf(format, s)
	case "CRT":
		seelog.Criticalf(format, s)
	default:
		panic("wrong log type")
	}
}
