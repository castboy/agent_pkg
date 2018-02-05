package agent_pkg

import (
	"time"
)

var NextOfflineMsg bool = true

func Manage() {
	defer func() {
		if err := recover(); nil != err {
			LogCrt("PANIC in Manage(), %v", err)
		}
	}()

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

		case msg := <-OfflineMsgCh:
			for {
				if NextOfflineMsg {
					break
				}
			}
			NextOfflineMsg = false

			ExeOfflineMsg(msg)

		case <-ticker.C:
			Record()
		}
	}
}
