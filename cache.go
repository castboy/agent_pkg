//cache.go

package agent_pkg

import "fmt"

type CacheInfo struct {
    Current int
    End int 
}

type CacheAnalysisRes struct {
    ReadCount int
    SendPrefetchMsg bool
}

var WafCacheInfoMap = make(map[string] CacheInfo)
var VdsCacheInfoMap = make(map[string] CacheInfo)
var CacheInfoMapPtr *map[string] CacheInfo

var CacheDataMap = make(map[string] [][]byte)

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

func AnalysisCache(manageMsg ManageMsg) map[string] CacheAnalysisRes{
    Res := make(map[string] CacheAnalysisRes)
    
    weightSum := 0
    if manageMsg.Engine == "waf" {
	    for _, v := range Waf {
		    weightSum += v.Weight
	    }
        for topic, cacheInfo := range WafCacheInfoMap {
            Remainder := cacheInfo.End - cacheInfo.Current
            //fmt.Println(topic, "Remainder:", Remainder)
            Deserve := (manageMsg.Count/ weightSum) * Waf[topic].Weight
            if Remainder > Deserve {
                Res[topic] = CacheAnalysisRes{Deserve , false}    
            } else {
                Res[topic] = CacheAnalysisRes{Remainder, true}    
            }
        }
    } else {
	    for _, v := range Vds {
		    weightSum += v.Weight
	    }
        for topic, cacheInfo := range VdsCacheInfoMap {
            Remainder := cacheInfo.End - cacheInfo.Current
            //fmt.Println(topic, "Remainder:", Remainder)
            Deserve := (manageMsg.Count/ weightSum) * Vds[topic].Weight
            if Remainder > Deserve {
                Res[topic] = CacheAnalysisRes{Deserve , false}    
            } else {
                Res[topic] = CacheAnalysisRes{Remainder, true}    
            }
        }
    }


    return Res
} 

func ReadCache(cacheAnalysisRes map[string] CacheAnalysisRes, manageMsg ManageMsg) {
    httpRes := make([][]byte, 0)

    if manageMsg.Engine == "waf" {
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
    
    manageMsg.HandleCh <- &httpRes
}

func UpdateCacheStatus(cacheAnalysisRes map[string] CacheAnalysisRes, manageMsg ManageMsg) {
    fmt.Println("UpdataCacheStatus")
    if manageMsg.Engine == "waf" {
        for topic, v := range cacheAnalysisRes {
            current := WafCacheInfoMap[topic].Current
            WafCacheInfoMap[topic] = CacheInfo{current+v.ReadCount, WafCacheInfoMap[topic].End}  
        } 
        //fmt.Println("WafCacheInfoMap", WafCacheInfoMap)
    } else {
        for topic, v := range cacheAnalysisRes {
            current := VdsCacheInfoMap[topic].Current
            VdsCacheInfoMap[topic] = CacheInfo{current+v.ReadCount, VdsCacheInfoMap[topic].End}  
        } 
        //fmt.Println("VdsCacheInfoMap", VdsCacheInfoMap)
    } 

}

func UpdateEngineCurrent(cacheAnalysisRes map[string] CacheAnalysisRes, manageMsg ManageMsg) {
    //fmt.Println("UpdateEngineCurrent")
    if manageMsg.Engine == "waf" {
        for topic, v := range cacheAnalysisRes {
            current := Waf[topic].Engine
            readCount := int64(v.ReadCount)
            Waf[topic] = Status{Waf[topic].First, current+readCount, Waf[topic].Cache, 
                                   Waf[topic].Last, Waf[topic].Weight} 
        }
        //fmt.Println("UpdateEngineCurrent", Waf)
    } else {
        for topic, v := range cacheAnalysisRes {
            current := Vds[topic].Engine
            readCount := int64(v.ReadCount)
            Vds[topic] = Status{Vds[topic].First, current+readCount, Vds[topic].Cache, 
                                   Vds[topic].Last, Vds[topic].Weight} 
        }
        //fmt.Println("UpdateEngineCurrent", Vds)
    }
}

func SendPrefetchMsg(cacheAnalysisRes map[string] CacheAnalysisRes, manageMsg ManageMsg) {
    //fmt.Println("SendPrefetchMsg")
    //fmt.Println("MaxCache:", AgentConf.MaxCache)
    for topic, v := range cacheAnalysisRes {
        if v.SendPrefetchMsg && PrefetchMsgSwitchMap[topic] {
            //fmt.Println("send prefetchMsg:", topic)
            PrefetchChMap[topic] <- PrefetchMsg{manageMsg.Engine, topic, AgentConf.MaxCache}   

            //fmt.Println(PrefetchMsg{manageMsg.Engine, topic, AgentConf.MaxCache})
            PrefetchMsgSwitchMap[topic] = false
        }
    } 
}

func DisposeReq(manageMsg ManageMsg) {
    res := AnalysisCache(manageMsg)
    //fmt.Println("analysisCacheRes", res)
    ReadCache(res, manageMsg)
    UpdateCacheStatus(res, manageMsg)
    UpdateEngineCurrent(res, manageMsg)
    SendPrefetchMsg(res, manageMsg)
}
