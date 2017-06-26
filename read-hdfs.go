package agent_pkg

import (
)

type HdfsToLocalResTag struct {
    File string
    Success bool
}

func DisposeRdHdfs (ch chan HdfsToLocalRes, prefetchResMsg PrefetchResMsg) {
    data := *prefetchResMsg.DataPtr

    for key, val := range data {
        file, offset, size, xdrMark, prtn := xdrFields(val)
        
        hdfsToLocalReqParams := HdfsToLocalReqParams{
            File: file,
            Offset: offset,
            Size: size,
            XdrMark: xdrMark,
            Index: key, 
            HdfsToLocalResCh: ch,
        }
        
        HdfsToLocalReqChs[prtn] <- hdfsToLocalReqParams
    }
}

func xdrFields (bytes []byte) (string, int64, int, string, int) {
    
    return file, offset, size, xdrMark, prtn
}

func CollectHdfsToLocalRes (prefetchResMsg PrefetchResMsg, ch chan HdfsToLocalRes, tags []HdfsToLocalResTag) []HdfsToLocalResTag {
    dataNum := len(*prefetchResMsg.DataPtr)
    statNum := 0

    for {
        res := <- ch

        index := res.Index
        tags[index] = HdfsToLocalResTag{
            File: res.File,
            Success: res.Success,
        } 

        statNum++
        if statNum == dataNum {
            break
        }
    }

    return tags
}

func GetCache (tags []HdfsToLocalResTag, data [][]byte) [][]byte {
    var cache [][]byte

    for key, val := range tags {
        if val.Success {
            bytes := updateXdr(data, key, val.File)
            cache = append(cache, bytes)  
        }
    }

    return cache
}

func WriteCache(prefetchResMsg PrefetchResMsg, data [][]byte) {
    topic := prefetchResMsg.Topic
    engine := prefetchResMsg.Engine
    count := len(data)

    if engine == "waf" {
        WafCacheInfoMap[topic] = CacheInfo{0, count}
        CacheDataMap[topic] = data
    } else {
        VdsCacheInfoMap[prefetchResMsg.Topic] = CacheInfo{0, count}
        CacheDataMap[topic] = data
    }
}

func updateXdr (data [][]byte, index int, localFile string) []byte {
    bytes := data[index]

    appendStr := ", \"File\": " + localFile + "}"
    appendBytes := []byte(appendStr)
    
    bytes = bytes[ : len(bytes) - 1]
    for _, val := range appendBytes {
        bytes = append(bytes, val)
    }

    return bytes
}

func UpdateCacheCurrent(prefetchResMsg PrefetchResMsg) {
    topic := prefetchResMsg.Topic
    count := int64(len(*prefetchResMsg.DataPtr))

    if prefetchResMsg.Engine == "waf" {
        Waf[topic] = Status{Waf[topic].First, Waf[topic].Engine, Waf[topic].Cache+count,
                               Waf[topic].Last, Waf[topic].Weight}
    } else {
        Vds[topic] = Status{Vds[topic].First, Vds[topic].Engine, Vds[topic].Cache+count,
                               Vds[topic].Last, Vds[topic].Weight}
    }

    PrefetchMsgSwitchMap[topic] = true
}

func RdHdfs (prefetchResMsg PrefetchResMsg) {
    tags := make([]HdfsToLocalResTag, PRTNNUM)

    data := *prefetchResMsg.DataPtr

    hdfsToLocalResCh := make(chan HdfsToLocalRes)

    DisposeRdHdfs(hdfsToLocalResCh, prefetchResMsg)

    tags = CollectHdfsToLocalRes(prefetchResMsg, hdfsToLocalResCh, tags) 
    cache := GetCache(tags, data)

    WriteCache(prefetchResMsg, cache)
    UpdateCacheCurrent(prefetchResMsg)
}

