//consume.go

package pkg_wmg

type CacheInfo struct {
    Current int
    End int 
}
var CacheInfoMap = make(map[string] CacheInfo)


func Consume(topic string) {
    byte := CacheMap[topic][CacheInfoMap[topic].Current]
    CacheInfoMap[topic].Current++

    return byte
}

func ReadCache() {
    
}

func CacheSignal(topic string) {
    for key, value := range TopicIndex {
        if topic == value {
            CacheNum[key] <- num     
        }    
    }
}

type AnalysisCacheRes struct {
    ReadCount int
    PreTakeMsg bool
}

func AnalysisCache(engine string, reqNum int) map[string] AnalysisCacheRes{
    Res := make(map[string] AnalysisCacheRes)

    return 
} 

func PreTakeMsg(analysisCacheRes map[string] AnalysisCacheRes) {
    for topic, v := range {
           
    } 
}

func ReadCache(engine string, count int) {
    Res := AnalysisCache(engine, count)
    PreTakeMsg(Res)
}

