package agent_pkg

import (
	"encoding/json"
	"fmt"
	"path"
	"strconv"
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
		RequestLocation  LocationHdfs
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
		property.Offset = append(property.Offset, httpFile.Http.RequestLocation.Offset)
		property.Offset = append(property.Offset, httpFile.Http.ResponseLocation.Offset)
		property.Size = append(property.Size, httpFile.Http.RequestLocation.Size)
		property.Size = append(property.Size, httpFile.Http.ResponseLocation.Size)
		property.XdrMark = append(property.XdrMark, httpFile.Http.RequestLocation.Signature)
		property.XdrMark = append(property.XdrMark, httpFile.Http.ResponseLocation.Signature)

		dir := path.Dir(property.SrcFile)
		prtn, err := strconv.Atoi(path.Base(dir))
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

		dir := path.Dir(property.SrcFile)
		prtn, err := strconv.Atoi(path.Base(dir))

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

func GetCacheAndErrDataNum(prefetchResMsg PrefetchResMsg, tags []HdfsToLocalResTag, data [][]byte) ([][]byte, int64) {
	var cache [][]byte
	var errNum int64 = 0

	for key, val := range tags {
		if val.Success {
			cache = append(cache, data[key])
		} else {
			errNum++

			//Log("Err", "Err xdr ++++++++++++++++++++++++++++++++++++++++++++++")
			//Log("Err", string(data[key]))
		}
	}

	return cache, errNum
}

func RdHdfs(prefetchResMsg PrefetchResMsg) {
	engine := prefetchResMsg.Engine
	data := *prefetchResMsg.DataPtr
	topic := prefetchResMsg.Topic

	len := len(data)

	if 0 != len {
		tags := make([]HdfsToLocalResTag, len)

		hdfsToLocalResCh := make(chan HdfsToLocalRes, len)

		DisposeRdHdfs(hdfsToLocalResCh, prefetchResMsg)

		tags = CollectHdfsToLocalRes(prefetchResMsg, hdfsToLocalResCh, tags)

		cache, errNum := GetCacheAndErrDataNum(prefetchResMsg, tags, data)

		res := RdHdfsResMsg{
			Engine:       engine,
			Topic:        topic,
			PrefetchNum:  len,
			CacheDataPtr: &cache,
			ErrNum:       errNum,
		}

		RdHdfsResCh <- res
	} else {
		PrefetchMsgSwitchMap[topic] = true
	}

}
