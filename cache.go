//cache.go

package agent_pkg

import "fmt"

type BufferStatus struct {
	Current int
	End     int
}

type BufferAnalyse struct {
	ShouldRead      int
	SendPrefetchMsg bool
}

var buffersStatus = make(map[string]map[string]BufferStatus)

var buffers = make(map[string][][]byte)

func InitBuffersStatus() {
	buffersStatus["waf"] = make(map[string]BufferStatus)
	buffersStatus["vds"] = make(map[string]BufferStatus)
	buffersStatus["rule"] = make(map[string]BufferStatus)

	for engine, val := range status {
		for topic, _ := range val {
			buffersStatus[engine][topic] = BufferStatus{}
		}
	}
}

func InitBuffer() {
	for _, val := range status {
		for topic, _ := range val {
			buffers[topic] = make([][]byte, AgentConf.MaxCache)
		}
	}
}

func AnalyseBuffer(req NormalReq) map[string]BufferAnalyse {
	Res := make(map[string]BufferAnalyse)

	weightSum := 0
	for _, v := range status[req.Engine] {
		weightSum += v.Weight
	}

	Deserve := 0
	if 0 != weightSum {
		for topic, cacheInfo := range buffersStatus[req.Engine] {
			Remainder := cacheInfo.End - cacheInfo.Current
			Deserve = (req.Count / weightSum) * status[req.Engine][topic].Weight

			if Remainder > Deserve {
				Res[topic] = BufferAnalyse{Deserve, false}
			} else {
				Res[topic] = BufferAnalyse{Remainder, true}
			}
		}
	} else {
		for topic, _ := range buffersStatus[req.Engine] {
			Res[topic] = BufferAnalyse{0, false}
		}
	}

	return Res
}

func ReadBuffer(res map[string]BufferAnalyse, req NormalReq) {
	httpRes := make([][]byte, 0)

	for topic, v := range res {
		current := buffersStatus[req.Engine][topic].Current
		for i := 0; i < v.ShouldRead; i++ {
			httpRes = append(httpRes, buffers[topic][current+i])
		}
	}

	req.HandleCh <- &httpRes
}

func UpdateBufferStatus(res map[string]BufferAnalyse, req NormalReq) {
	fmt.Println("UpdataCacheStatus")
	for topic, v := range res {
		current := buffersStatus[req.Engine][topic].Current
		buffersStatus[req.Engine][topic] = BufferStatus{current + v.ShouldRead, buffersStatus[req.Engine][topic].End}
	}
}

func UpdateEngineOffset(res map[string]BufferAnalyse, req NormalReq) {
	for topic, v := range res {
		current := status[req.Engine][topic].Engine
		readCount := int64(v.ShouldRead)
		status[req.Engine][topic] = Status{status[req.Engine][topic].First, current + readCount,
			status[req.Engine][topic].Err, status[req.Engine][topic].Cache,
			status[req.Engine][topic].Last, status[req.Engine][topic].Weight}
	}
}

func SendPrefetchMsg(res map[string]BufferAnalyse, req NormalReq) {
	//fmt.Println("SendPrefetchMsg")
	//fmt.Println("MaxCache:", AgentConf.MaxCache)
	for topic, v := range res {
		if v.SendPrefetchMsg && PrefetchMsgSwitchMap[topic] {
			//fmt.Println("send prefetchMsg:", topic)
			PrefetchChMap[topic] <- PrefetchMsg{req.Engine, topic, AgentConf.MaxCache, false}

			//fmt.Println(PrefetchMsg{req.Engine, topic, AgentConf.MaxCache})
			PrefetchMsgSwitchMap[topic] = false
		}
	}
}

func WriteBuffer(res RdHdfsRes) {
	if 0 != res.PrefetchNum {
		topic := res.Base.Topic
		engine := res.Base.Engine
		count := len(*res.CacheDataPtr)
		data := *res.CacheDataPtr

		buffersStatus[engine][topic] = BufferStatus{0, count}
		buffers[topic] = data
	}
}

func UpdateBufferOffset(res RdHdfsRes) {
	topic := res.Base.Topic
	if 0 != res.PrefetchNum {
		count := int64(res.PrefetchNum)
		errNum := res.ErrNum

		status[res.Engine][topic] = Status{status[res.Engine][topic].First, status[res.Engine][topic].Engine,
			status[res.Engine][topic].Err + errNum, status[res.Engine][topic].Cache + count,
			status[res.Engine][topic].Last, status[res.Engine][topic].Weight}
	}

	PrefetchMsgSwitchMap[topic] = true
}

func DisposeNormalReq(req NormalReq) {
	res := AnalyseBuffer(req)
	//fmt.Println("analysisCacheRes", res)
	ReadBuffer(res, req)
	UpdateBufferStatus(res, req)
	UpdateEngineOffset(res, req)
	SendPrefetchMsg(res, req)
}

func WriteBufferAndUpdateBufferOffset(res RdHdfsRes) {
	WriteBuffer(res)
	UpdateBufferOffset(res)
}

func AnalysisRuleBindingBuffer(req RuleBindingReq) BufferAnalyse {
	engine := req.Base.Engine
	topic := req.Base.Topic
	deserve := req.Count

	var res BufferAnalyse

	remainder := buffersStatus[engine][topic].End - buffersStatus[engine][topic].Current
	if deserve > remainder {
		res = BufferAnalyse{remainder, true}
	} else {
		res = BufferAnalyse{deserve, false}
	}

	return res
}

func ReadRuleBindingBuffer(res BufferAnalyse, req RuleBindingReq) {
	engine := req.Base.Engine
	topic := req.Base.Topic
	readCount := res.ShouldRead

	httpRes := make([][]byte, 0)
	current := buffersStatus[engine][topic].Current

	for i := 0; i < readCount; i++ {
		httpRes = append(httpRes, buffers[topic][current+i])
	}

	req.HandleCh <- &httpRes
}

func UpdateRuleBindingBufferStatus(res BufferAnalyse, req RuleBindingReq) {
	engine := req.Base.Engine
	topic := req.Base.Topic
	readCount := res.ShouldRead

	current := buffersStatus[engine][topic].Current
	buffersStatus[engine][topic] = BufferStatus{current + readCount, buffersStatus[engine][topic].End}
}

func UpdateRuleBindingEngineOffset(res BufferAnalyse, req RuleBindingReq) {
	engine := req.Base.Engine
	topic := req.Base.Topic
	readCount := int64(res.ShouldRead)

	current := status[engine][topic].Engine
	status[engine][topic] = Status{status[engine][topic].First, current + readCount,
		status[engine][topic].Err, status[engine][topic].Cache,
		status[engine][topic].Last, status[engine][topic].Weight}
}

func SendRuleBindingPrefetchMsg(res BufferAnalyse, req RuleBindingReq) {
	engine := req.Base.Engine
	topic := req.Base.Topic
	sendPrefetchMsg := res.SendPrefetchMsg

	if sendPrefetchMsg && PrefetchMsgSwitchMap[topic] {
		PrefetchChMap[topic] <- PrefetchMsg{engine, topic, AgentConf.MaxCache, false}

		PrefetchMsgSwitchMap[topic] = false
	}
}

func DisposeRuleBindingReq(req RuleBindingReq) {
	res := AnalysisRuleBindingBuffer(req)
	ReadRuleBindingBuffer(res, req)
	UpdateRuleBindingBufferStatus(res, req)
	UpdateRuleBindingEngineOffset(res, req)
	SendRuleBindingPrefetchMsg(res, req)
}
