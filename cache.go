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

var WafCacheInfoMap = make(map[string]CacheInfo)
var VdsCacheInfoMap = make(map[string]CacheInfo)
var CacheInfoMapPtr *map[string]CacheInfo

var CacheDataMap = make(map[string][][]byte)

func InitCacheInfoMap() {
	for topic, _ := range Waf {
		WafCacheInfoMap[topic] = CacheInfo{0, 0}
	}
	for topic, _ := range Vds {
		VdsCacheInfoMap[topic] = CacheInfo{0, 0}
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

func AnalysisCache(normalReqMsg NormalReqMsg) map[string]CacheAnalysisRes {
	Res := make(map[string]CacheAnalysisRes)

	weightSum := 0
	if normalReqMsg.Engine == "waf" {
		for _, v := range Waf {
			weightSum += v.Weight
		}
		if 0 != weightSum {
			for topic, cacheInfo := range WafCacheInfoMap {
				Remainder := cacheInfo.End - cacheInfo.Current
				Deserve := (normalReqMsg.Count / weightSum) * Waf[topic].Weight
				if Remainder > Deserve {
					Res[topic] = CacheAnalysisRes{Deserve, false}
				} else {
					Res[topic] = CacheAnalysisRes{Remainder, true}
				}
			}
		} else {
			for topic, _ := range WafCacheInfoMap {
				Res[topic] = CacheAnalysisRes{0, false}
			}
		}
	} else {
		for _, v := range Vds {
			weightSum += v.Weight
		}
		if 0 != weightSum {
			for topic, cacheInfo := range VdsCacheInfoMap {
				Remainder := cacheInfo.End - cacheInfo.Current
				Deserve := (normalReqMsg.Count / weightSum) * Vds[topic].Weight
				if Remainder > Deserve {
					Res[topic] = CacheAnalysisRes{Deserve, false}
				} else {
					Res[topic] = CacheAnalysisRes{Remainder, true}
				}
			}
		} else {
			for topic, _ := range VdsCacheInfoMap {
				Res[topic] = CacheAnalysisRes{0, false}
			}
		}
	}

	return Res
}

func ReadCache(cacheAnalysisRes map[string]CacheAnalysisRes, normalReqMsg NormalReqMsg) {
	httpRes := make([][]byte, 0)

	if normalReqMsg.Engine == "waf" {
		for topic, v := range cacheAnalysisRes {
			current := WafCacheInfoMap[topic].Current
			for i := 0; i < v.ReadCount; i++ {
				httpRes = append(httpRes, CacheDataMap[topic][current+i])
			}
		}
	} else {
		for topic, v := range cacheAnalysisRes {
			current := VdsCacheInfoMap[topic].Current
			for i := 0; i < v.ReadCount; i++ {
				httpRes = append(httpRes, CacheDataMap[topic][current+i])
			}
		}
	}

	normalReqMsg.HandleCh <- &httpRes
}

func UpdateCacheStatus(cacheAnalysisRes map[string]CacheAnalysisRes, normalReqMsg NormalReqMsg) {
	fmt.Println("UpdataCacheStatus")
	if normalReqMsg.Engine == "waf" {
		for topic, v := range cacheAnalysisRes {
			current := WafCacheInfoMap[topic].Current
			WafCacheInfoMap[topic] = CacheInfo{current + v.ReadCount, WafCacheInfoMap[topic].End}
		}
		//fmt.Println("WafCacheInfoMap", WafCacheInfoMap)
	} else {
		for topic, v := range cacheAnalysisRes {
			current := VdsCacheInfoMap[topic].Current
			VdsCacheInfoMap[topic] = CacheInfo{current + v.ReadCount, VdsCacheInfoMap[topic].End}
		}
		//fmt.Println("VdsCacheInfoMap", VdsCacheInfoMap)
	}

}

func UpdateEngineCurrent(cacheAnalysisRes map[string]CacheAnalysisRes, normalReqMsg NormalReqMsg) {
	//fmt.Println("UpdateEngineCurrent")
	if normalReqMsg.Engine == "waf" {
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

func SendPrefetchMsg(cacheAnalysisRes map[string]CacheAnalysisRes, normalReqMsg NormalReqMsg) {
	//fmt.Println("SendPrefetchMsg")
	//fmt.Println("MaxCache:", AgentConf.MaxCache)
	for topic, v := range cacheAnalysisRes {
		if v.SendPrefetchMsg && PrefetchMsgSwitchMap[topic] {
			//fmt.Println("send prefetchMsg:", topic)
			PrefetchChMap[topic] <- PrefetchMsg{normalReqMsg.Engine, topic, AgentConf.MaxCache, false}

			//fmt.Println(PrefetchMsg{normalReqMsg.Engine, topic, AgentConf.MaxCache})
			PrefetchMsgSwitchMap[topic] = false
		}
	}
}

func WriteCache(rdHdfsResMsg RdHdfsResMsg) {
	if 0 != rdHdfsResMsg.PrefetchNum {
		topic := rdHdfsResMsg.Topic
		engine := rdHdfsResMsg.Engine
		count := len(*rdHdfsResMsg.CacheDataPtr)
		data := *rdHdfsResMsg.CacheDataPtr

		if engine == "waf" {
			WafCacheInfoMap[topic] = CacheInfo{0, count}
			CacheDataMap[topic] = data
		} else {
			VdsCacheInfoMap[topic] = CacheInfo{0, count}
			CacheDataMap[topic] = data
		}
	}
}

func UpdateCacheCurrent(rdHdfsResMsg RdHdfsResMsg) {
	topic := rdHdfsResMsg.Topic
	if 0 != rdHdfsResMsg.PrefetchNum {
		count := int64(rdHdfsResMsg.PrefetchNum)
		errNum := rdHdfsResMsg.ErrNum

		if rdHdfsResMsg.Engine == "waf" {
			Waf[topic] = Status{Waf[topic].First, Waf[topic].Engine, Waf[topic].Err + errNum, Waf[topic].Cache + count,
				Waf[topic].Last, Waf[topic].Weight}
		} else {
			Vds[topic] = Status{Vds[topic].First, Vds[topic].Engine, Vds[topic].Err + errNum, Vds[topic].Cache + count,
				Vds[topic].Last, Vds[topic].Weight}
		}
	}

	PrefetchMsgSwitchMap[topic] = true
}

func DisposeNormalReq(normalReqMsg NormalReqMsg) {
	res := AnalysisCache(normalReqMsg)
	//fmt.Println("analysisCacheRes", res)
	ReadCache(res, normalReqMsg)
	UpdateCacheStatus(res, normalReqMsg)
	UpdateEngineCurrent(res, normalReqMsg)
	SendPrefetchMsg(res, normalReqMsg)
}

func WriteCacheAndUpdateCacheCurrent(rdHdfsResMsg RdHdfsResMsg) {
	WriteCache(rdHdfsResMsg)
	UpdateCacheCurrent(rdHdfsResMsg)
}

func AnalysisRuleBindingCache(ruleBindingReqMsg RuleBindingReqMsg) CacheAnalysisRes {
	engine := ruleBindingReqMsg.Engine
	topic := ruleBindingReqMsg.Topic
	deserve := ruleBindingReqMsg.Count

	var remainder int
	var res CacheAnalysisRes

	if engine == "waf" {
		remainder = WafCacheInfoMap[topic].End - WafCacheInfoMap[topic].Current
	} else {
		remainder = VdsCacheInfoMap[topic].End - VdsCacheInfoMap[topic].Current
	}

	if deserve > remainder {
		res = CacheAnalysisRes{remainder, true}
	} else {
		res = CacheAnalysisRes{deserve, false}
	}

	return res
}

func ReadRuleBindingCache(analysisCacheRes CacheAnalysisRes, ruleBindingReqMsg RuleBindingReqMsg) {
	engine := ruleBindingReqMsg.Engine
	topic := ruleBindingReqMsg.Topic
	readCount := analysisCacheRes.ReadCount

	httpRes := make([][]byte, 0)
	var current int

	if engine == "waf" {
		current = WafCacheInfoMap[topic].Current
	} else {
		current = VdsCacheInfoMap[topic].Current
	}

	for i := 0; i < readCount; i++ {
		httpRes = append(httpRes, CacheDataMap[topic][current+i])
	}

	ruleBindingReqMsg.HandleCh <- &httpRes
}

func UpdateRuleBindingCacheStatus(analysisCacheRes CacheAnalysisRes, ruleBindingReqMsg RuleBindingReqMsg) {
	engine := ruleBindingReqMsg.Engine
	topic := ruleBindingReqMsg.Topic
	readCount := analysisCacheRes.ReadCount

	if engine == "waf" {
		current := WafCacheInfoMap[topic].Current
		WafCacheInfoMap[topic] = CacheInfo{current + readCount, WafCacheInfoMap[topic].End}
	} else {
		current := VdsCacheInfoMap[topic].Current
		VdsCacheInfoMap[topic] = CacheInfo{current + readCount, VdsCacheInfoMap[topic].End}
	}
}

func UpdateRuleBindingEngineCurrent(analysisCacheRes CacheAnalysisRes, ruleBindingReqMsg RuleBindingReqMsg) {
	engine := ruleBindingReqMsg.Engine
	topic := ruleBindingReqMsg.Topic
	readCount := int64(analysisCacheRes.ReadCount)

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

func SendRuleBindingPrefetchMsg(analysisCacheRes CacheAnalysisRes, ruleBindingReqMsg RuleBindingReqMsg) {
	engine := ruleBindingReqMsg.Engine
	topic := ruleBindingReqMsg.Topic
	sendPrefetchMsg := analysisCacheRes.SendPrefetchMsg

	if sendPrefetchMsg && PrefetchMsgSwitchMap[topic] {
		PrefetchChMap[topic] <- PrefetchMsg{engine, topic, AgentConf.MaxCache, false}

		PrefetchMsgSwitchMap[topic] = false
	}
}

func DisposeRuleBindingReq(ruleBindingReqMsg RuleBindingReqMsg) {
	res := AnalysisRuleBindingCache(ruleBindingReqMsg)
	ReadRuleBindingCache(res, ruleBindingReqMsg)
	UpdateRuleBindingCacheStatus(res, ruleBindingReqMsg)
	UpdateRuleBindingEngineCurrent(res, ruleBindingReqMsg)
	SendRuleBindingPrefetchMsg(res, ruleBindingReqMsg)
}
