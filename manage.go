package agent_pkg

import "fmt"

func Manage() {
	for {
		select {
		case req := <-NormalReqCh:
			DisposeNormalReq(req)

		case req := <-RuleBindingReqCh:
			fmt.Println("req := <-RuleBindingReqCh:", req)
			//			DisposeRuleBindingReq(req)

		case res := <-PrefetchResCh:
			go RdHdfs(res)

		case res := <-RdHdfsResCh:
			WriteBufferAndUpdateBufferOffset(res)

		case start := <-StartOfflineCh:
			StartOffline(start)
			if "rule" == start.Base.Engine {
				NewWafInstance(AgentConf.WafInstanceSrc, AgentConf.WafInstanceDst,
					start.Base.Topic, AgentConf.WebServerReqIp, AgentConf.WebServerReqPort)
			}
			fmt.Println("after start := <-StartOfflineCh:")

		case stop := <-StopOfflineCh:
			StopOffline(stop)

		case shutdown := <-ShutdownOfflineCh:
			ShutdownOffline(shutdown)
			if "rule" == shutdown.Engine {
				KillWafInstance(AgentConf.WafInstanceDst, shutdown.Topic)
			}

		case complete := <-CompleteOfflineCh:
			CompleteOffline(complete)
			if "rule" == complete.Engine {
				KillWafInstance(AgentConf.WafInstanceDst, complete.Topic)
			}

		default:

		}
	}
}
