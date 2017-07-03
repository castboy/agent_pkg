package agent_pkg

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
)

type HdfsToLocalResTag struct {
	Success bool
}

type XdrProperty struct {
	SrcFile string
	DstFile []string
	Offset  []int64
	Size    []int
	XdrMark []string
	Prtn    int
}

type HttpFile struct {
	Http struct {
		Request          string
		Response         string
		RequestLocatin   LocationHdfs
		ResponseLocation LocationHdfs
	}
	App struct {
		File         string
		FileLocation LocationHdfs
	}
}

type LocationHdfs struct {
	File      string `json:"File"`
	Offset    int64  `json:"Offset"`
	Size      int    `json:"Size"`
	Signature string `json:"Signature"`
}

func DisposeRdHdfs(ch chan HdfsToLocalRes, prefetchResMsg PrefetchResMsg) {
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
			Engine:           engine,
			SrcFile:          property.SrcFile,
			DstFile:          property.DstFile,
			Offset:           property.Offset,
			Size:             property.Size,
			XdrMark:          property.XdrMark,
			Index:            key,
			HdfsToLocalResCh: ch,
		}

		if engine == "vds" {
			FileHdfsToLocalReqChs[property.Prtn] <- hdfsToLocalReqParams
		} else {
			HttpHdfsToLocalReqChs[property.Prtn] <- hdfsToLocalReqParams
		}
	}
}

func xdrProperty(engine string, bytes []byte) XdrProperty {
	var property XdrProperty
	var httpFile HttpFile

	json.Unmarshal(bytes, &httpFile)

	if engine == "waf" {
		property.SrcFile = httpFile.Http.ResponseLocation.File
		property.DstFile = append(property.DstFile, httpFile.Http.Request)
		property.DstFile = append(property.DstFile, httpFile.Http.Response)
		property.Offset = append(property.Offset, httpFile.Http.RequestLocatin.Offset)
		property.Offset = append(property.Offset, httpFile.Http.ResponseLocation.Offset)
		property.Size = append(property.Size, httpFile.Http.RequestLocatin.Size)
		property.Size = append(property.Size, httpFile.Http.ResponseLocation.Size)
		property.XdrMark = append(property.XdrMark, httpFile.Http.RequestLocatin.Signature)
		property.XdrMark = append(property.XdrMark, httpFile.Http.ResponseLocation.Signature)

		//fmt.Printf("signature:%x", []byte(property.XdrMark[0]))
		id := strings.LastIndex(property.SrcFile, "/")
		prtnStr := property.SrcFile[id+1:]
		prtn, err := strconv.ParseFloat(prtnStr, 32)
		if nil != err {
			fmt.Println("prtn-id err:", property.SrcFile)
		}

		property.Prtn = int(prtn)
	} else {
		property.SrcFile = httpFile.App.FileLocation.File
		property.DstFile = append(property.DstFile, httpFile.App.File)
		property.Offset = append(property.Offset, httpFile.App.FileLocation.Offset)
		property.Size = append(property.Size, httpFile.App.FileLocation.Size)
		property.XdrMark = append(property.XdrMark, httpFile.App.FileLocation.Signature)

		id := strings.LastIndex(property.SrcFile, "/")
		prtnStr := property.SrcFile[id+1:]
		prtn, err := strconv.ParseFloat(prtnStr, 32)
		if nil != err {
			fmt.Println("prtn-id err:", property.SrcFile)
		}

		property.Prtn = int(prtn)
	}

	fmt.Println(property)
	return property
}

func CollectHdfsToLocalRes(prefetchResMsg PrefetchResMsg, ch chan HdfsToLocalRes, tags []HdfsToLocalResTag) []HdfsToLocalResTag {
	dataNum := len(*prefetchResMsg.DataPtr)
	statNum := 0

	for {
		res := <-ch
		//fmt.Println("ColletcHdfs:", res)
		index := res.Index
		tags[index] = HdfsToLocalResTag{
			Success: res.Success,
		}

		statNum++
		if statNum == dataNum {
			break
		}
	}

	return tags
}

func GetCache(prefetchResMsg PrefetchResMsg, tags []HdfsToLocalResTag, data [][]byte) [][]byte {
	var cache [][]byte

	for key, val := range tags {
		if val.Success {
			cache = append(cache, data[key])
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

func UpdateCacheCurrent(prefetchResMsg PrefetchResMsg) {
	topic := prefetchResMsg.Topic
	count := int64(len(*prefetchResMsg.DataPtr))

	if prefetchResMsg.Engine == "waf" {
		Waf[topic] = Status{Waf[topic].First, Waf[topic].Engine, Waf[topic].Cache + count,
			Waf[topic].Last, Waf[topic].Weight}
	} else {
		Vds[topic] = Status{Vds[topic].First, Vds[topic].Engine, Vds[topic].Cache + count,
			Vds[topic].Last, Vds[topic].Weight}
	}

	PrefetchMsgSwitchMap[topic] = true
}

func RdHdfs(prefetchResMsg PrefetchResMsg) {
	data := *prefetchResMsg.DataPtr

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
