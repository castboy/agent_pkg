package agent_pkg

import (
	"time"
        "fmt"
)

func Manage() {
	ticker := time.NewTicker(time.Second * time.Duration(3))

	for {
		select {
		case req := <-NormalReqCh:
			Log.Trace("NormalReq: %v", req)
			DisposeNormalReq(req)

		case req := <-RuleBindingReqCh:
			Log.Trace("RuleBindingReq: %v", req)
			DisposeRuleBindingReq(req)

		case res := <-PrefetchResCh:
			go RdHdfs(res)

		case res := <-RdHdfsResCh:
			WriteBufferAndUpdateBufferOffset(res)

		case start := <-StartOfflineCh:
                        fmt.Println("recv StartOfflineCh")
			StartOffline(start)
			if "rule" == start.Base.Engine {
				go NewWafInstance(AgentConf.WafInstanceSrc, AgentConf.WafInstanceDst,
					start.Base.Topic, AgentConf.WebServerReqIp, AgentConf.WebServerReqPort)
			}

		case stop := <-StopOfflineCh:
			StopOffline(stop)

		case err := <-ErrorOfflineCh:
			ShutdownOffline(err)
			if "rule" == err.Engine {
				go KillWafInstance(AgentConf.WafInstanceDst, err.Topic)
			}

		case shutdown := <-ShutdownOfflineCh:
			ShutdownOffline(shutdown)
			if "rule" == shutdown.Engine {
				go KillWafInstance(AgentConf.WafInstanceDst, shutdown.Topic)
			}

		case complete := <-CompleteOfflineCh:
			CompleteOffline(complete)
			if "rule" == complete.Engine {
				go KillWafInstance(AgentConf.WafInstanceDst, complete.Topic)
			}

		case <-ticker.C:
			Record()
		}
	}
}
