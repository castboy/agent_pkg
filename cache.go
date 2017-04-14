//cache.go

package pkg_wmg


var CacheNum = make([]chan int, 100)
var TopicIndex = make([]string)
var CacheOffset = make(map[string] [int])

var CacheMap = make(map[string] [][]byte)

func ReadKafka(topic string, num string) {
    CacheData := make([][]byte)
    
    defer func() {
        if r := recover(); r != nil {
            log.Printf("consume err: %v", r)    
            CacheMap[topic] = CacheData
        }    
    }()

    for i := 0; i < num; i++ {
        msg, err := (*consumerPtr)[topic].Consume() 
        if err != nil {
            panic("no data in: " + topic)    
        }
        CacheData = append(CacheData, msg.Value)
    }

    CacheMap[topic] = CacheData
}

func SetCacheOffset(topic string, num int) {
    CacheOffset[topic] = num
}

func Cache(topic string, CacheNum chan int) {
    num := <-CacheNum 

    ReadKafka(topic, num)
    
    cache := CacheRes{topic, len(CacheData[topic])}
    CacheCh <- cache 

}

func InitCache() {
    for topic, _ := Waf {
        CacheNum[i] = make(chan int) 
        go Cache(topic, CacheNum[i])

        dataSlice := make([][]byte)
        CacheMap[topic] = dataSlice

        TopicIndex = append(TopicIndex, topic)
    }

    for topic, _ := Vds {
        CacheNum[i] = make(chan int) 
        go Cache(topic, CacheNum[i])
        TopicIndex = append(TopicIndex, topic)
    }
}
