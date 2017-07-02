package agent_pkg

import (
	"strings"
    "fmt"
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

type XdrStr struct {
    Type int
    Data HttpFile
}

type HttpFile struct {
    Http struct {
        RequestLocatin LocationHdfs
        ResponseLocation LocationHdfs
    }
    App struct {
        FileLocation LocationHdfs
    }
}

type LocationHdfs struct {
    File      string `json:"File"`
    Offset    int64  `json:"Offset"`
    Size      int    `json:"Size"`
    Signature string `json:"Signature"`
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
	var property XdrProperty
	var xdrStr XdrStr 

	json.Unmarshal(bytes, &xdrStr)


    if engine == "waf" {
        property.File = xdrStr.Data.Http.ResponseLocation.File
        property.Offset = append(property.Offset, xdrStr.Data.Http.RequestLocatin.Offset)
        property.Offset = append(property.Offset, xdrStr.Data.Http.ResponseLocation.Offset)
        property.Size = append(property.Size, xdrStr.Data.Http.RequestLocatin.Size)
        property.Size = append(property.Size, xdrStr.Data.Http.ResponseLocation.Size)
        property.XdrMark = append(property.XdrMark, xdrStr.Data.Http.RequestLocatin.Signature)
        property.XdrMark = append(property.XdrMark, xdrStr.Data.Http.ResponseLocation.Signature)

        //fmt.Printf("signature:%x", []byte(property.XdrMark[0]))
        id := strings.LastIndex(property.File, "/")
        prtnStr := property.File[id + 1 : ]
        prtn, err := strconv.Atoi(prtnStr)


        if nil != err {
            fmt.Println("prtn-id err:", property.File)
        }

        property.Prtn = prtn
    } else {
        property.File = xdrStr.Data.App.FileLocation.File
        property.Offset = append(property.Offset, xdrStr.Data.App.FileLocation.Offset)
        property.Size = append(property.Size, xdrStr.Data.App.FileLocation.Size)
        property.XdrMark = append(property.XdrMark, xdrStr.Data.App.FileLocation.Signature)

        id := strings.LastIndex(property.File, "/")
        prtnStr := property.File[id + 1 : ]
        prtn, err := strconv.Atoi(prtnStr)
        if nil != err {
            fmt.Println("prtn-id err:", property.File)
        }

        property.Prtn = prtn
    }

    fmt.Println(property)
    return property
}

func CollectHdfsToLocalRes (prefetchResMsg PrefetchResMsg, ch chan HdfsToLocalRes, tags []HdfsToLocalResTag) []HdfsToLocalResTag {
    dataNum := len(*prefetchResMsg.DataPtr)
    statNum := 0

    for {
        res := <- ch
        //fmt.Println("ColletcHdfs:", res)
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
        VdsCacheInfoMap[topic] = CacheInfo{0, count}
        CacheDataMap[topic] = data
    }
}

func updateXdr (prefetchResMsg PrefetchResMsg, data [][]byte, index int, localFile []string) []byte {
    defer func()  {
        if r := recover(); r != nil {
            fmt.Printf("consume err: %v", r)    
        }    
    }()
    bytes := data[index]
    var appendBytes []byte
	
    if prefetchResMsg.Engine == "vds" {
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

    fmt.Println(string(data[0]))
    len := len(data)

    if 0 != len {
        tags := make([]HdfsToLocalResTag, len)

        hdfsToLocalResCh := make(chan HdfsToLocalRes, len)

        DisposeRdHdfs(hdfsToLocalResCh, prefetchResMsg)

        tags = CollectHdfsToLocalRes(prefetchResMsg, hdfsToLocalResCh, tags) 

        //fmt.Println("tags:", tags)
        cache := GetCache(prefetchResMsg, tags, data)

        WriteCache(prefetchResMsg, cache)
        UpdateCacheCurrent(prefetchResMsg)
    }
}


