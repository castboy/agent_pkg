//cache.go

package pkg_wmg

import "fmt"

type CacheInfo struct {
    Current int
    End int 
}

var WafCacheInfoMap = make(map[string] CacheInfo)
var VdsCacheInfoMap = make(map[string] CacheInfo)
var CacheInfoMapPtr *map[string] CacheInfo

var CacheDataMap = make(map[string] [][]byte)

type CacheAnalysisRes struct {
    ReadCount int
    SendPrefetchMsg bool
}

func InitCacheInfoMap() {
    for topic, _ := range Waf {
        WafCacheInfoMap[topic] = CacheInfo{0, 0}    
    }
    for topic, _ := range Vds {
        VdsCacheInfoMap[topic] = CacheInfo{0, 0}    
    }
}

func AnalysisCache(EnginePtr *map[string]Partition, reqNum int) map[string] CacheAnalysisRes{
    Res := make(map[string] CacheAnalysisRes)
    
    weightSum := 0
    for _, v := range *EnginePtr {
        weightSum += v.Weight
    }

    for topic, cacheInfo := range *CacheInfoMapPtr {
        Remainder := cacheInfo.End - cacheInfo.Current
        Deserve := (reqNum / weightSum) * (*EnginePtr)[topic].Weight
        if Remainder > Deserve {
            Res[topic] = CacheAnalysisRes{Deserve , false}    
        } else {
            Res[topic] = CacheAnalysisRes{Remainder, true}    
        }
    }

    return Res
} 

func ReadCache(cacheAnalysisRes map[string] CacheAnalysisRes, handleIndex int) {
    httpRes := make([][]byte, 0)

    for topic, v := range cacheAnalysisRes {
        current := (*CacheInfoMapPtr)[topic].Current
        for i := 0; i < v.ReadCount; i++ {
            httpRes = append(httpRes, CacheDataMap[topic][current+i])
        }        
    } 
    
    HandleCh[handleIndex] <- &httpRes
}

func UpdateCacheStatus(cacheAnalysisRes map[string] CacheAnalysisRes) {
    for topic, v := range cacheAnalysisRes {
        current := (*CacheInfoMapPtr)[topic].Current
        (*CacheInfoMapPtr)[topic] = CacheInfo{current+v.ReadCount, (*CacheInfoMapPtr)[topic].End}  
    } 
}

func WriteCache(prefetchResMsg PrefetchResMsg) {
    (*CacheInfoMapPtr)[prefetchResMsg.Topic] = CacheInfo{0, prefetchResMsg.Count} 
    //fmt.Println(*CacheInfoMapPtr)
}

func UpdateEngineCurrent(EnginePtr *map[string]Partition, cacheAnalysisRes map[string] CacheAnalysisRes) {
    for topic, v := range cacheAnalysisRes {
        current := (*EnginePtr)[topic].Engine
        readCount := int64(v.ReadCount)
        (*EnginePtr)[topic] = Partition{(*EnginePtr)[topic].First, current+readCount, (*EnginePtr)[topic].Cache, 
                                        (*EnginePtr)[topic].Last, (*EnginePtr)[topic].Weight, (*EnginePtr)[topic].Stop} 
    }
    fmt.Println("UpdateEngineCurrent", *EnginePtr)
}

func UpdateCacheCurrent(prefetchResMsg PrefetchResMsg) {
    topic := prefetchResMsg.Topic
    count := int64(prefetchResMsg.Count)
    _, ok := Waf[topic]
    if ok {
        Waf[topic] = Partition{Waf[topic].First, Waf[topic].Engine, Waf[topic].Cache+count, Waf[topic].Last, Waf[topic].Weight, Waf[topic].Stop} 
        fmt.Println("UpdateCacheCurrent", Waf)
    } else {
        Vds[topic] = Partition{Vds[topic].First, Vds[topic].Engine, Vds[topic].Cache+count, Vds[topic].Last, Vds[topic].Weight, Vds[topic].Stop} 
        fmt.Println("UpdateCacheCurrent",Vds)
    }
}

func SendPrefetchMsg(cacheAnalysisRes map[string] CacheAnalysisRes) {
    for topic, v := range cacheAnalysisRes {
        if v.SendPrefetchMsg {
            fmt.Println("send prefetchMsg:", topic)
            PrefetchChMap[topic] <- PrefetchMsg{topic, 50}   
        }
    } 
}

func DisposeReq(manageMsg ManageMsg) {
    res := AnalysisCache(manageMsg.EnginePtr, manageMsg.Count)
    //fmt.Println("analysisCacheRes", res)
    ReadCache(res, manageMsg.HandleIndex)
    UpdateCacheStatus(res)
    SendPrefetchMsg(res)
    UpdateEngineCurrent(manageMsg.EnginePtr, res)
}

func DisposeRes(prefetchResMsg PrefetchResMsg) {
    WriteCache(prefetchResMsg)
    UpdateCacheCurrent(prefetchResMsg)
}
