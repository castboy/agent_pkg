//cache.go

package agent_pkg

import "fmt"

type BufStatus struct {
	Current int
	End     int
}

type BufferAnalyse struct {
	EngineRead      int
	SendPrefetchMsg bool
}

var bufStatus = make(map[string]map[string]BufStatus)

var buffers = make(map[string][][]byte)

var reqTypes = []string{"waf", "vds", "rule"}

func InitBuffersStatus() {
	for _, v := range reqTypes {
		bufStatus[v] = make(map[string]BufStatus)
	}

	for engine, val := range status {
		for topic, _ := range val {
			bufStatus[engine][topic] = BufStatus{}
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
		for topic, cacheInfo := range bufStatus[req.Engine] {
			Remainder := cacheInfo.End - cacheInfo.Current
			Deserve = (req.Count / weightSum) * status[req.Engine][topic].Weight

			if Remainder > Deserve {
				Res[topic] = BufferAnalyse{Deserve, false}
			} else {
				Res[topic] = BufferAnalyse{Remainder, true}
			}
		}
	} else {
		for topic, _ := range bufStatus[req.Engine] {
			Res[topic] = BufferAnalyse{0, false}
		}
	}

	return Res
}

func ReadBuffer(res map[string]BufferAnalyse, req NormalReq) {
	httpRes := make([][]byte, 0, AgentConf.MaxCache)

	for topic, v := range res {
		current := bufStatus[req.Engine][topic].Current
		for i := 0; i < v.EngineRead; i++ {
			httpRes = append(httpRes, buffers[topic][current+i])
		}
	}

	req.HandleCh <- &httpRes
}

func UpdateBufferStatus(res map[string]BufferAnalyse, req NormalReq) {
	fmt.Println("UpdataCacheStatus")
	for topic, v := range res {
		s := bufStatus[req.Engine][topic]
		bufStatus[req.Engine][topic] = BufStatus{s.Current + v.EngineRead, s.End}
	}
}

func UpdateEngineOffset(res map[string]BufferAnalyse, req NormalReq) {
	for topic, v := range res {
		s := status[req.Engine][topic]
		readCount := int64(v.EngineRead)
		status[req.Engine][topic] = Status{s.First, s.Engine + readCount, s.Err, s.Cache, s.Last, s.Weight}
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

		bufStatus[engine][topic] = BufStatus{0, count}
		buffers[topic] = data
		fmt.Println("WriteBuffer+++++++++++++++++++++++++++++++")
		fmt.Println(buffers[topic])
	}
}

func UpdateBufferOffset(res RdHdfsRes) {
	topic := res.Base.Topic
	if 0 != res.PrefetchNum {
		count := int64(res.PrefetchNum)
		errNum := res.ErrNum
		s := status[res.Engine][topic]

		status[res.Engine][topic] = Status{s.First, s.Engine, s.Err + errNum, s.Cache + count, s.Last, s.Weight}
	}

	PrefetchMsgSwitchMap[topic] = true
}

func DisposeNormalReq(req NormalReq) {
	res := AnalyseBuffer(req)
	fmt.Println("DisposeNormalReq analysisCacheRes", res)
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

	remainder := bufStatus[engine][topic].End - bufStatus[engine][topic].Current
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
	readCount := res.EngineRead

	httpRes := make([][]byte, 0, AgentConf.MaxCache)
	current := bufStatus[engine][topic].Current

	for i := 0; i < readCount; i++ {
		httpRes = append(httpRes, buffers[topic][current+i])
	}

	req.HandleCh <- &httpRes
}

func UpdateRuleBindingBufferStatus(res BufferAnalyse, req RuleBindingReq) {
	engine := req.Base.Engine
	topic := req.Base.Topic
	readCount := res.EngineRead

	s := bufStatus[engine][topic]
	bufStatus[engine][topic] = BufStatus{s.Current + readCount, s.End}
}

func UpdateRuleBindingEngineOffset(res BufferAnalyse, req RuleBindingReq) {
	engine := req.Base.Engine
	topic := req.Base.Topic
	readCount := int64(res.EngineRead)

	s := status[engine][topic]
	status[engine][topic] = Status{s.First, s.Engine + readCount, s.Err, s.Cache, s.Last, s.Weight}
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

	fmt.Println("DisposeRuleBindingReq analysisRes:", res)

	ReadRuleBindingBuffer(res, req)
	UpdateRuleBindingBufferStatus(res, req)
	UpdateRuleBindingEngineOffset(res, req)
	SendRuleBindingPrefetchMsg(res, req)
}
