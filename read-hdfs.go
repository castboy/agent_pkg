package agent_pkg

import (
	"encoding/json"
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

var ErrXdrCh = make(chan string, 100)

func DisposeRdHdfs(ch chan HdfsToLocalRes, prefetchRes PrefetchRes) int {
	engine := prefetchRes.Base.Engine
	data := *prefetchRes.DataPtr

	var readHdfsNum int
	var err error

	for key, val := range data {
		var property XdrProperty

		if engine == "vds" {
			property, err = xdrProperty("vds", val)
			if nil != err || property.Prtn < 0 || property.Prtn >= FILEPRTNNUM {
				Log.Error("partition-id err, origin-xdr below: %s", string(val))
				break
			}
		} else {
			property, err = xdrProperty("waf", val)
			if nil != err || property.Prtn < 0 || property.Prtn >= HTTPPRTNNUM {
				Log.Error("partition-id err, origin-xdr below: %s", string(val))
				break
			}
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

		readHdfsNum++
	}

	return readHdfsNum
}

func xdrProperty(engine string, bytes []byte) (XdrProperty, error) {
	var property XdrProperty
	var httpFile HttpFile
	var prtn int
	var err error

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
		prtn, err = strconv.Atoi(path.Base(dir))

		property.Prtn = int(prtn)
	} else {
		property.SrcFile = httpFile.App.FileLocation.File
		property.DstFile = append(property.DstFile, httpFile.App.File)
		property.Offset = append(property.Offset, httpFile.App.FileLocation.Offset)
		property.Size = append(property.Size, httpFile.App.FileLocation.Size)
		property.XdrMark = append(property.XdrMark, httpFile.App.FileLocation.Signature)

		dir := path.Dir(property.SrcFile)
		prtn, err = strconv.Atoi(path.Base(dir))

		property.Prtn = int(prtn)
	}

	return property, err
}

func CollectHdfsToLocalRes(ch chan HdfsToLocalRes, tags []HdfsToLocalResTag, readHdfsNum int) []HdfsToLocalResTag {
	statNum := 0

	for {
		if statNum == readHdfsNum {
			break
		}

		res := <-ch

		index := res.Index
		tags[index] = HdfsToLocalResTag{
			Success: res.Success,
		}

		statNum++
	}

	return tags
}

func GetCacheAndRightDataNum(prefetchRes PrefetchRes, tags []HdfsToLocalResTag, data [][]byte) ([][]byte, int) {
	var cache = make([][]byte, 0, AgentConf.MaxCache)
	var rightNum int

	for key, val := range tags {
		if val.Success {
			cache = append(cache, data[key])
			rightNum++
		} else {
			ErrXdrCh <- string(data[key])
		}
	}

	return cache, rightNum
}

func RecordErrXdr() {
	for {
		msg := <-ErrXdrCh
		Log.Error("Err xdr: %s", msg)
	}
}

func RdHdfs(prefetchRes PrefetchRes) {
	engine := prefetchRes.Base.Engine
	data := *prefetchRes.DataPtr
	topic := prefetchRes.Base.Topic

	len := len(data)

	var res RdHdfsRes

	if 0 != len {
		tags := make([]HdfsToLocalResTag, len)

		hdfsToLocalResCh := make(chan HdfsToLocalRes, len)

		readHdfsNum := DisposeRdHdfs(hdfsToLocalResCh, prefetchRes)

		tags = CollectHdfsToLocalRes(hdfsToLocalResCh, tags, readHdfsNum)

		cache, rightNum := GetCacheAndRightDataNum(prefetchRes, tags, data)

		res = RdHdfsRes{Base{engine, topic}, len, &cache, len - rightNum}
	} else {
		res = RdHdfsRes{Base{engine, topic}, 0, nil, 0}
	}

	RdHdfsResCh <- res
}
