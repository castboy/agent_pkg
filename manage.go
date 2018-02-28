package agent_pkg

import (
	"time"
)

var NextOfflineMsg bool = true

var manageHeartBeat = make(chan int)
var manageHeartBeatVerified bool

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
			DisposeNormalReq(req)

		case req := <-RuleBindingReqCh:
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

			ExeOfflineMsg(msg)

		case <-ticker.C:
			Record()

		case <-manageHeartBeat:
			manageHeartBeatVerified = true
		}
	}
}

func ManageHeartBeat() {
	for {
		manageHeartBeat <- 1
		time.Sleep(time.Duration(20) * time.Second)
		if manageHeartBeatVerified {
			Log.Info("%s", " manage run normal")
		} else {
			LogCrt("%s", "manage run abnormal")
		}

		manageHeartBeatVerified = false
	}
}
