package agent_pkg

import (
	"strings"
	"strconv"
    "encoding/json"
)

type HdfsToLocalResTag struct {
    File []string
    Success bool
}

type XdrProperty struct {
    File string
    Offset []int64
    Size []int
    XdrMark []string
    Prtn int
}

func DisposeRdHdfs (ch chan HdfsToLocalRes, prefetchResMsg PrefetchResMsg) {
	engine := prefetchResMsg.Engine
	data := *prefetchResMsg.DataPtr
	
	for key, val := range data {
		var property XdrProperty
		
		if engine == "vds" {  
			property = xdrProperty("vds", val)
		} else {
			property = xdrProperty("waf", val)
		}

		hdfsToLocalReqParams := HdfsToLocalReqParams{
		    Engine: engine,
		    File: property.File,
		    Offset: property.Offset,
		    Size: property.Size,
		    XdrMark: property.XdrMark,
		    Index: key, 
		    HdfsToLocalResCh: ch,
		}

		if engine == "vds" {  
			FileHdfsToLocalReqChs[property.Prtn] <- hdfsToLocalReqParams
		} else {
			HttpHdfsToLocalReqChs[property.Prtn] <- hdfsToLocalReqParams
		}
	}
}

func xdrProperty (engine string, bytes []byte) XdrProperty {
	var (
		property XdrProperty
		jsonParse interface{}
	)
		
	json.Unmarshal(bytes, &jsonParse)
	
	m := jsonParse.(map[string]interface{})
	for _, v := range m {
		switch vv := v.(type) {
		case string:
		case float64:
		case int64:
		case int:
		case bool:
		case interface{}:
			n := vv.(map[string]interface{})
			for i, j := range n {
				switch jj := j.(type) {
				case string:
				case float64:
				case int64:
				case int:
				case bool:
				case interface{}:
					m := jj.(map[string]interface{})
					for z, l := range m {
						switch ll := l.(type) {
						case string:
							if engine == "vds" && i == "FileLocation" && z == "File" {
								property.File = ll
								
								index := strings.Index(ll, "/")
								id := ll[index - 1 : ]
                                i, err := strconv.Atoi(id)
                                if nil != err {
                                }
								property.Prtn = i 
							}
							if engine == "waf" && i == "RequestLocation" && z == "File" {
								property.File = ll
								
								index := strings.Index(ll, "/")
								id := ll[index - 1 : ]
                                i, err := strconv.Atoi(id)
                                if nil != err {
                                }
								property.Prtn = i 
							}
							if engine == "vds" && i == "FileLocation" && z == "Signature" {
								property.XdrMark[0] = ll
							}
							if engine == "waf" && i == "RequestLocation" && z == "Signature" {
								property.XdrMark[0] = ll
							}
							if engine == "waf" && i == "ResponseLocation" && z == "Signature" {
								property.XdrMark[1] = ll
							}									
						case float64:
						case int64:
							if engine == "vds" && i == "FileLocation" && z == "Offset" {
								property.Offset[0] = ll
							}
							if engine == "waf" && i == "RequestLocation" && z == "Offset" {
								property.Offset[0] = ll
							}
							if engine == "waf" && i == "ResponseLocation" && z == "Offset" {
								property.Offset[1] = ll
							}							
						case int:
							if engine == "vds" && i == "FileLocation" && z == "Size" {
								property.Size[0] = ll
							}
							if engine == "waf" && i == "RequestLocation" && z == "Size" {
								property.Size[0] = ll
							}	
							if engine == "waf" && i == "ResponseLocation" && z == "Size" {
								property.Size[1] = ll
							}							
						case bool:
						case interface{}:
						default:
						}
					}
				default:
				}
			}
		default:
		}
	}

    	return property
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

func GetCache (prefetchResMsg PrefetchResMsg, tags []HdfsToLocalResTag, data [][]byte) [][]byte {
    var cache [][]byte

    for key, val := range tags {
        if val.Success {
            bytes := updateXdr(prefetchResMsg, data, key, val.File)
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

func updateXdr (prefetchResMsg PrefetchResMsg, data [][]byte, index int, localFile []string) []byte {
    bytes := data[index]
    var appendBytes []byte
	
    if prefetchResMsg.Topic == "vds" {
	appendStr := ",\"File\":\"" + localFile[0] + "\"}"
        appendBytes = []byte(appendStr)
    } else {
	appendStr := ",\"File\":{\"request\":\"" + localFile[0] + "\"," + "\"response\":\"" + localFile[1] + "\"}}"
        appendBytes = []byte(appendStr)
    }

    
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
    data := *prefetchResMsg.DataPtr

    len := len(data)
    tags := make([]HdfsToLocalResTag, len)


    hdfsToLocalResCh := make(chan HdfsToLocalRes)

    DisposeRdHdfs(hdfsToLocalResCh, prefetchResMsg)

    tags = CollectHdfsToLocalRes(prefetchResMsg, hdfsToLocalResCh, tags) 
    cache := GetCache(prefetchResMsg, tags, data)

    WriteCache(prefetchResMsg, cache)
    UpdateCacheCurrent(prefetchResMsg)
}

