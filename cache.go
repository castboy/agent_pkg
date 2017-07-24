//cache.go

package agent_pkg

import "fmt"

type CacheInfo struct {
	Current int
	End     int
}

type CacheAnalysisRes struct {
	ReadCount       int
	SendPrefetchMsg bool
}

var CacheInfoMap = make(map[string]map[string]CacheInfo)

var CacheDataMap = make(map[string][][]byte)

func InitCacheInfoMap() {
	CacheInfoMap["waf"] = make(map[string]CacheInfo)
	CacheInfoMap["vds"] = make(map[string]CacheInfo)

	for topic, _ := range Waf {
		CacheInfoMap["waf"][topic] = CacheInfo{}
	}
	for topic, _ := range Vds {
		CacheInfoMap["vds"][topic] = CacheInfo{}
	}
}

func InitCacheDataMap() {
	for topic, _ := range Waf {
		CacheDataMap[topic] = make([][]byte, AgentConf.MaxCache)
	}
	for topic, _ := range Vds {
		CacheDataMap[topic] = make([][]byte, AgentConf.MaxCache)
	}
}

func AnalysisCache(req NormalReq) map[string]CacheAnalysisRes {
	Res := make(map[string]CacheAnalysisRes)

	weightSum := 0

	if req.Engine == "waf" {
		for _, v := range Waf {
			weightSum += v.Weight
		}
	} else {
		for _, v := range Vds {
			weightSum += v.Weight
		}
	}

	Deserve := 0
	if 0 != weightSum {
		for topic, cacheInfo := range CacheInfoMap[req.Engine] {
			Remainder := cacheInfo.End - cacheInfo.Current
			if req.Engine == "waf" {
				Deserve = (req.Count / weightSum) * Waf[topic].Weight
			} else {
				Deserve = (req.Count / weightSum) * Vds[topic].Weight
			}

			if Remainder > Deserve {
				Res[topic] = CacheAnalysisRes{Deserve, false}
			} else {
				Res[topic] = CacheAnalysisRes{Remainder, true}
			}
		}
	} else {
		for topic, _ := range CacheInfoMap[req.Engine] {
			Res[topic] = CacheAnalysisRes{0, false}
		}
	}

	return Res
}

func ReadCache(cacheAnalysisRes map[string]CacheAnalysisRes, req NormalReq) {
	httpRes := make([][]byte, 0)

	for topic, v := range cacheAnalysisRes {
		current := CacheInfoMap[req.Engine][topic].Current
		for i := 0; i < v.ReadCount; i++ {
			httpRes = append(httpRes, CacheDataMap[topic][current+i])
		}
	}

	req.HandleCh <- &httpRes
}

func UpdateCacheStatus(cacheAnalysisRes map[string]CacheAnalysisRes, req NormalReq) {
	fmt.Println("UpdataCacheStatus")
	for topic, v := range cacheAnalysisRes {
		current := CacheInfoMap[req.Engine][topic].Current
		CacheInfoMap[req.Engine][topic] = CacheInfo{current + v.ReadCount, CacheInfoMap[req.Engine][topic].End}
	}
}

func UpdateEngineCurrent(cacheAnalysisRes map[string]CacheAnalysisRes, req NormalReq) {
	//fmt.Println("UpdateEngineCurrent")
	if req.Engine == "waf" {
		for topic, v := range cacheAnalysisRes {
			current := Waf[topic].Engine
			readCount := int64(v.ReadCount)
			Waf[topic] = Status{Waf[topic].First, current + readCount, Waf[topic].Err, Waf[topic].Cache,
				Waf[topic].Last, Waf[topic].Weight}
		}
		//fmt.Println("UpdateEngineCurrent", Waf)
	} else {
		for topic, v := range cacheAnalysisRes {
			current := Vds[topic].Engine
			readCount := int64(v.ReadCount)
			Vds[topic] = Status{Vds[topic].First, current + readCount, Vds[topic].Err, Vds[topic].Cache,
				Vds[topic].Last, Vds[topic].Weight}
		}
		//fmt.Println("UpdateEngineCurrent", Vds)
	}
}

func SendPrefetchMsg(cacheAnalysisRes map[string]CacheAnalysisRes, req NormalReq) {
	//fmt.Println("SendPrefetchMsg")
	//fmt.Println("MaxCache:", AgentConf.MaxCache)
	for topic, v := range cacheAnalysisRes {
		if v.SendPrefetchMsg && PrefetchMsgSwitchMap[topic] {
			//fmt.Println("send prefetchMsg:", topic)
			PrefetchChMap[topic] <- PrefetchMsg{req.Engine, topic, AgentConf.MaxCache, false}

			//fmt.Println(PrefetchMsg{req.Engine, topic, AgentConf.MaxCache})
			PrefetchMsgSwitchMap[topic] = false
		}
	}
}

func WriteCache(res RdHdfsRes) {
	if 0 != res.PrefetchNum {
		topic := res.Base.Topic
		engine := res.Base.Engine
		count := len(*res.CacheDataPtr)
		data := *res.CacheDataPtr

		CacheInfoMap[engine][topic] = CacheInfo{0, count}
		CacheDataMap[topic] = data
	}
}

func UpdateCacheCurrent(res RdHdfsRes) {
	topic := res.Base.Topic
	if 0 != res.PrefetchNum {
		count := int64(res.PrefetchNum)
		errNum := res.ErrNum

		if res.Engine == "waf" {
			Waf[topic] = Status{Waf[topic].First, Waf[topic].Engine, Waf[topic].Err + errNum, Waf[topic].Cache + count,
				Waf[topic].Last, Waf[topic].Weight}
		} else {
			Vds[topic] = Status{Vds[topic].First, Vds[topic].Engine, Vds[topic].Err + errNum, Vds[topic].Cache + count,
				Vds[topic].Last, Vds[topic].Weight}
		}
	}

	PrefetchMsgSwitchMap[topic] = true
}

func DisposeNormalReq(req NormalReq) {
	res := AnalysisCache(req)
	//fmt.Println("analysisCacheRes", res)
	ReadCache(res, req)
	UpdateCacheStatus(res, req)
	UpdateEngineCurrent(res, req)
	SendPrefetchMsg(res, req)
}

func WriteCacheAndUpdateCacheCurrent(res RdHdfsRes) {
	WriteCache(res)
	UpdateCacheCurrent(res)
}

func AnalysisRuleBindingCache(req RuleBindingReq) CacheAnalysisRes {
	engine := req.Base.Engine
	topic := req.Base.Topic
	deserve := req.Count

	var res CacheAnalysisRes

	remainder := CacheInfoMap[engine][topic].End - CacheInfoMap[engine][topic].Current
	if deserve > remainder {
		res = CacheAnalysisRes{remainder, true}
	} else {
		res = CacheAnalysisRes{deserve, false}
	}

	return res
}

func ReadRuleBindingCache(res CacheAnalysisRes, req RuleBindingReq) {
	engine := req.Base.Engine
	topic := req.Base.Topic
	readCount := res.ReadCount

	httpRes := make([][]byte, 0)
	current := CacheInfoMap[engine][topic].Current

	for i := 0; i < readCount; i++ {
		httpRes = append(httpRes, CacheDataMap[topic][current+i])
	}

	req.HandleCh <- &httpRes
}

func UpdateRuleBindingCacheStatus(res CacheAnalysisRes, req RuleBindingReq) {
	engine := req.Base.Engine
	topic := req.Base.Topic
	readCount := res.ReadCount

	current := CacheInfoMap[engine][topic].Current
	CacheInfoMap[engine][topic] = CacheInfo{current + readCount, CacheInfoMap[engine][topic].End}
}

func UpdateRuleBindingEngineCurrent(res CacheAnalysisRes, req RuleBindingReq) {
	engine := req.Base.Engine
	topic := req.Base.Topic
	readCount := int64(res.ReadCount)

	if engine == "waf" {
		current := Waf[topic].Engine
		Waf[topic] = Status{Waf[topic].First, current + readCount, Waf[topic].Err, Waf[topic].Cache,
			Waf[topic].Last, Waf[topic].Weight}
	} else {
		current := Vds[topic].Engine
		Vds[topic] = Status{Vds[topic].First, current + readCount, Vds[topic].Err, Vds[topic].Cache,
			Vds[topic].Last, Vds[topic].Weight}
	}
}

func SendRuleBindingPrefetchMsg(res CacheAnalysisRes, req RuleBindingReq) {
	engine := req.Base.Engine
	topic := req.Base.Topic
	sendPrefetchMsg := res.SendPrefetchMsg

	if sendPrefetchMsg && PrefetchMsgSwitchMap[topic] {
		PrefetchChMap[topic] <- PrefetchMsg{engine, topic, AgentConf.MaxCache, false}

		PrefetchMsgSwitchMap[topic] = false
	}
}

func DisposeRuleBindingReq(req RuleBindingReq) {
	res := AnalysisRuleBindingCache(req)
	ReadRuleBindingCache(res, req)
	UpdateRuleBindingCacheStatus(res, req)
	UpdateRuleBindingEngineCurrent(res, req)
	SendRuleBindingPrefetchMsg(res, req)
}
