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

func Log(level string, log ...interface{}) {
	defer seelog.Flush()

	switch level {
	case "TRC":
		seelog.Tracef(s)
	case "DBG":
		seelog.Debugf(s)
	case "INF":
		seelog.Infof(s)
	case "WRN":
		seelog.Warnf(s)
	case "ERR":
		seelog.Errorf(s)
	case "CRT":
		seelog.Criticalf(s)
	}
}
