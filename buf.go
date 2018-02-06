//cache.go

package agent_pkg

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

	Log.Trace("AnalyseBuffer res: %v", Res)

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
	for topic, v := range res {
		s := bufStatus[req.Engine][topic]
		bufStatus[req.Engine][topic] = BufStatus{s.Current + v.EngineRead, s.End}
	}

	Log.Trace("UpdateBufferStatus: %v", bufStatus)
}

func UpdateEngineOffset(res map[string]BufferAnalyse, req NormalReq) {
	for topic, v := range res {
		s := status[req.Engine][topic]
		if s.Weight == 0 {
			s.Weight = 5
		}
		readCount := int64(v.EngineRead)
		status[req.Engine][topic] = Status{s.First, s.Engine + readCount, s.Err, s.Cache, s.Last, s.Weight}
	}

	Log.Trace("UpdateEngineOffset: %v", status)
}

func SendPrefetchMsg(res map[string]BufferAnalyse, req NormalReq) {
	for topic, v := range res {
		if v.SendPrefetchMsg && PrefetchMsgSwitchMap[topic] {
			PrefetchChMap[topic] <- PrefetchMsg{req.Engine, topic, AgentConf.MaxCache, false}

			PrefetchMsgSwitchMap[topic] = false

			Log.Trace("SendPrefetchMsg, PrefetchChMap[%s] <- %v", topic, PrefetchMsg{req.Engine, topic, AgentConf.MaxCache, false})
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

		Log.Trace("WriteBuffer: bufStatus[%s][%s] = %v", engine, topic, bufStatus[engine][topic])
	}
}

func UpdateBufferOffset(res RdHdfsRes) {
	topic := res.Base.Topic
	if 0 != res.PrefetchNum {
		count := int64(res.PrefetchNum)
		errNum := res.ErrNum
		s := status[res.Engine][topic]
		if s.Weight == 0 {
			s.Weight = 5
		}

		status[res.Engine][topic] = Status{s.First, s.Engine, s.Err + errNum, s.Cache + count, s.Last, s.Weight}

		Log.Trace("UpdateBufferOffset: status[%s][%s] = %v", res.Engine, topic, status[res.Engine][topic])
	}

	PrefetchMsgSwitchMap[topic] = true
}

func DisposeNormalReq(req NormalReq) {
	Log.Trace("NormalReq: %v, NormalReqCh length: %d", req, len(NormalReqCh))

	res := AnalyseBuffer(req)
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

	Log.Trace("UpdateRuleBindingBufferStatus: bufStatus[%s][%s] = %v", engine, topic, bufStatus[engine][topic])
}

func UpdateRuleBindingEngineOffset(res BufferAnalyse, req RuleBindingReq) {
	engine := req.Base.Engine
	topic := req.Base.Topic
	readCount := int64(res.EngineRead)

	s := status[engine][topic]
	if s.Weight == 0 {
		s.Weight = 5
	}
	status[engine][topic] = Status{s.First, s.Engine + readCount, s.Err, s.Cache, s.Last, s.Weight}

	Log.Trace("UpdateRuleBindingEngineOffset: status[%s][%s] = %v", engine, topic, status[engine][topic])
}

func SendRuleBindingPrefetchMsg(res BufferAnalyse, req RuleBindingReq) {
	engine := req.Base.Engine
	topic := req.Base.Topic
	sendPrefetchMsg := res.SendPrefetchMsg

	if sendPrefetchMsg && PrefetchMsgSwitchMap[topic] {
		PrefetchChMap[topic] <- PrefetchMsg{engine, topic, AgentConf.MaxCache, false}

		PrefetchMsgSwitchMap[topic] = false

		Log.Trace("SendRuleBindingPrefetchMsg, PrefetchChMap[%s] <- %v", topic, PrefetchMsg{engine, topic, AgentConf.MaxCache, false})
	}
}

func DisposeRuleBindingReq(req RuleBindingReq) {
	Log.Trace("RuleBindingReq: %v, RuleBindingReqCh length: %d", req, len(RuleBindingReqCh))

	if _, exist := status["rule"][req.Base.Topic]; exist {
		res := AnalysisRuleBindingBuffer(req)
		ReadRuleBindingBuffer(res, req)
		UpdateRuleBindingBufferStatus(res, req)
		UpdateRuleBindingEngineOffset(res, req)
		SendRuleBindingPrefetchMsg(res, req)
	} else {
		Log.Error("RuleBindingReq, status: %v, req: %v", status, req)
	}
}

func Buffer() {
	defer func() {
		if err := recover(); nil != err {
			LogCrt("PANIC in Buffer(), %v", err)
		}
	}()

	InitBuffersStatus()
	InitBuffer()
	InitPrefetchMsgSwitchMap()
}
