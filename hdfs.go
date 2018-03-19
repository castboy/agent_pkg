package agent_pkg

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/colinmarc/hdfs"
	"github.com/widuu/goini"
)

type HdfsToLocalReqParams struct {
	Engine  string
	SrcFile string
	DstFile []string
	Offset  []int64
	Size    []int
	XdrMark []string

	Index int

	HdfsToLocalResCh chan HdfsToLocalRes
}

type HdfsToLocalRes struct {
	Index int

	Success bool
}

const (
	CHECKFILE = true
)

var (
	FILEPRTNNUM int
	HTTPPRTNNUM int
)

var (
	reRdNum      int
	reRdInterval int
)

var (
	HttpHdfsToLocalReqChs []chan HdfsToLocalReqParams
	FileHdfsToLocalReqChs []chan HdfsToLocalReqParams
)

type HdfsFileHdl struct {
	Hdl     *hdfs.FileReader
	ReqTime int64
}

type HdfsFileHdls []map[string]HdfsFileHdl
type HdfsClients []*hdfs.Client

var FileHdfsClients = make([]*hdfs.Client, 0)
var HttpHdfsClients = make([]*hdfs.Client, 0)
var FileHdfsFileHdl = make([]map[string]HdfsFileHdl, 0)
var HttpHdfsFileHdl = make([]map[string]HdfsFileHdl, 0)

func HdfsClisOffline() {
	ticker := time.NewTicker(time.Second * time.Duration(300))
	for range ticker.C {
		for i := 0; i < HTTPPRTNNUM; i++ {
			HttpHdfsClients[i].Close()
		}
		for i := 0; i < FILEPRTNNUM; i++ {
			FileHdfsClients[i].Close()
		}
	}
}

func InitHdfsClis(namenode string) {
	for i := 0; i < HTTPPRTNNUM; i++ {
		HttpHdfsClients = append(HttpHdfsClients, InitHdfsCli(namenode))
	}
	for i := 0; i < FILEPRTNNUM; i++ {
		FileHdfsClients = append(FileHdfsClients, InitHdfsCli(namenode))
	}
}

func InitHdfsCli(namenode string) *hdfs.Client {
	client, err := hdfs.New(namenode + ":8020")
	if nil != err {
		LogCrt("Build Hdfs Client Err, %s", err.Error())
	}

	return client
}

func ReHdfsCli(cli HdfsClients, idx int) {
	client := InitHdfsCli(AgentConf.HdfsNameNode)
	cli[idx].Close()
	cli[idx] = client
	Log.Error("ReHdfsCli")
}

var ClearFileHdlChs, ClearHttpHdlChs []chan int

func SendClearFileHdlMsg(seconds int) {
	defer func() {
		if err := recover(); nil != err {
			LogCrt("PANIC in SendClearFileHdlMsg(), %v", err)
		}
	}()

	ticker := time.NewTicker(time.Second * time.Duration(seconds))
	for _ = range ticker.C {
		for _, ch := range ClearFileHdlChs {
			ch <- seconds
		}
		for _, ch := range ClearHttpHdlChs {
			ch <- seconds
		}
	}
}

func HttpHdfs(idx int) {
	defer func() {
		if err := recover(); nil != err {
			LogCrt("PANIC in HttpHdfs(), %v", err)
		}
	}()

	ClearHttpHdlChs[idx] = make(chan int)

	for {
		select {
		case msg := <-HttpHdfsToLocalReqChs[idx]:
			HttpHdfsToLocal(idx, msg)
		case msg := <-ClearHttpHdlChs[idx]:
			ClearHdlTiming(HttpHdfsFileHdl, idx, msg)
		}
	}
}

func FileHdfs(idx int) {
	defer func() {
		if err := recover(); nil != err {
			LogCrt("PANIC in FileHdfs(), %v", err)
		}
	}()

	ClearFileHdlChs[idx] = make(chan int)

	for {
		select {
		case msg := <-FileHdfsToLocalReqChs[idx]:
			FileHdfsToLocal(idx, msg)
		case msg := <-ClearFileHdlChs[idx]:
			ClearHdlTiming(FileHdfsFileHdl, idx, msg)
		}
	}
}

func ClearHdlTiming(fileHdl []map[string]HdfsFileHdl, idx int, seconds int) {
	timestamp := time.Now().Unix()

	for key, val := range fileHdl[idx] {
		if val.ReqTime+int64(seconds) < timestamp {
			fileHdl[idx][key].Hdl.Close()
			delete(fileHdl[idx], key)
		}
	}
}

func ClearHdlCertain(hdl HdfsFileHdls, idx int) {
	hdl[idx] = make(map[string]HdfsFileHdl)
}

func FileHdl(hdl HdfsFileHdls, cli HdfsClients, idx int, p HdfsToLocalReqParams) error {
	var f *hdfs.FileReader
	var err error

	hdfsFileHdl, exist := hdl[idx][p.SrcFile]

	if !exist {
		f, err = cli[idx].Open(p.SrcFile)
		if nil != err {
			if _, ok := err.(*os.PathError); ok {
				Log.Error("Open Hdfs File %s Path Err", p.SrcFile)
				return errors.New("path err")
			} else {
				ReHdfsCli(cli, idx)
				ClearHdlCertain(hdl, idx)
				f, err = cli[idx].Open(p.SrcFile)
				if nil != err {
					Log.Error("Open Hdfs File %s Err, after hdfs reconnect", p.SrcFile)
					return errors.New("path err")
				}
			}
		}
	} else {
		f = hdfsFileHdl.Hdl
	}
	timestamp := time.Now().Unix()
	hdl[idx][p.SrcFile] = HdfsFileHdl{f, timestamp}

	return nil
}

func HttpHdfsToLocal(idx int, p HdfsToLocalReqParams) {
	err := FileHdl(HttpHdfsFileHdl, HttpHdfsClients, idx, p)
	var res HdfsToLocalRes

	if nil != err {
		res = HdfsToLocalRes{
			Index:   p.Index,
			Success: false,
		}
	} else {
		reqBytes, reqRight := hdfsRdCheck(HttpHdfsFileHdl[idx][p.SrcFile].Hdl, p.SrcFile, p.Offset[0], p.Size[0], p.XdrMark[0])
		resBytes, resRight := hdfsRdCheck(HttpHdfsFileHdl[idx][p.SrcFile].Hdl, p.SrcFile, p.Offset[1], p.Size[1], p.XdrMark[1])

		wrOk := false
		if reqRight && resRight {
			wrReqOk := localWrite(p.DstFile[0], reqBytes)
			wrResOk := localWrite(p.DstFile[1], resBytes)
			wrOk = wrReqOk && wrResOk
		}
		res = HdfsToLocalRes{
			Index:   p.Index,
			Success: wrOk,
		}
	}

	p.HdfsToLocalResCh <- res

}

func FileHdfsToLocal(idx int, p HdfsToLocalReqParams) {
	err := FileHdl(FileHdfsFileHdl, FileHdfsClients, idx, p)
	var res HdfsToLocalRes

	if nil != err {
		res = HdfsToLocalRes{
			Index:   p.Index,
			Success: false,
		}
	} else {
		wrOk := false

		b, rdOk := hdfsRdCheck(FileHdfsFileHdl[idx][p.SrcFile].Hdl, p.SrcFile, p.Offset[0], p.Size[0], p.XdrMark[0])

		if rdOk {
			wrOk = localWrite(p.DstFile[0], b)
		}

		res = HdfsToLocalRes{
			Index:   p.Index,
			Success: wrOk,
		}
	}

	p.HdfsToLocalResCh <- res
}

func hdfsRdCheck(fHdl *hdfs.FileReader, file string, offset int64, size int, mark string) ([]byte, bool) {
	errNum := 0
	var bytes []byte

	for errNum < reRdNum {
		bytes, err := hdfsRd(fHdl, file, offset, size)
		if nil != err {
			errNum++
			time.Sleep(time.Duration(reRdInterval) * time.Millisecond)
			continue
		} else {
			ok := isRightFile(bytes, mark)
			if ok {
				return bytes, true
			} else {
				return bytes, false
			}
		}
	}

	LogXdrErr.Error(`{"file": "%s", "offset": %d, "size": %d, "signature": "%s", "fileSize": %d}`, file, offset, size, mark, fHdl.Stat().Size())

	return bytes, false
}

func hdfsRd(fHdl *hdfs.FileReader, file string, offset int64, size int) (bytes []byte, err error) {
	defer func() {
		if r := recover(); r != nil {
			Log.Error("HDFS-PANIC: %v", r)
			err = errors.New("HDFS-PANIC")
		}
	}()

	bytes = make([]byte, size)
	_, err = fHdl.ReadAt(bytes, offset)

	return bytes, err
}

func fileIsExist(file string) bool {
	var exist = true
	if _, err := os.Stat(file); os.IsNotExist(err) {
		exist = false
	}
	return exist
}

func isRightFile(hdfs []byte, xdrMark string) bool {
	right := true

	if CHECKFILE {
		if xdrMark != sha256Code(hdfs) {
			right = false
		}
	} else {
	}

	return right
}

func sha256Code(bytes []byte) string {
	h := sha256.New()
	h.Write(bytes)
	return fmt.Sprintf("%x", h.Sum(nil))
}

func localWrite(file string, bytes []byte) bool {
	success := true

	id := strings.LastIndex(file, "/")
	dir := file[:id]

	isExist, err := pathExists(dir)
	if !isExist {
		err = os.MkdirAll(dir, 0777)
		if err != nil {
			LogCrt("Create local dir %s failed", dir)
		}
	}

	err = ioutil.WriteFile(file, bytes, 0644)
	if nil != err {
		success = false
		Log.Error("Write local file %s failed, err: %s", file, err.Error())
	}

	return success
}

func HdfsToLocals() {
	for i := 0; i < HTTPPRTNNUM; i++ {
		HttpHdfsFileHdl = append(HttpHdfsFileHdl, make(map[string]HdfsFileHdl))
	}
	for i := 0; i < FILEPRTNNUM; i++ {
		FileHdfsFileHdl = append(FileHdfsFileHdl, make(map[string]HdfsFileHdl))
	}

	for i := 0; i < HTTPPRTNNUM; i++ {
		HttpHdfsToLocalReqChs[i] = make(chan HdfsToLocalReqParams, 100)
		go HttpHdfs(i)
	}
	for i := 0; i < FILEPRTNNUM; i++ {
		FileHdfsToLocalReqChs[i] = make(chan HdfsToLocalReqParams, 100)
		go FileHdfs(i)
	}
}

func InitHdfs() {
	conf := goini.SetConfig("conf.ini")
	n, err := strconv.Atoi(conf.GetValue("preproccess", "partition"))
	if nil != err {
		FILEPRTNNUM = 65
		HTTPPRTNNUM = 65
	}

	FILEPRTNNUM = n
	HTTPPRTNNUM = n

	HttpHdfsToLocalReqChs = make([]chan HdfsToLocalReqParams, HTTPPRTNNUM)
	FileHdfsToLocalReqChs = make([]chan HdfsToLocalReqParams, FILEPRTNNUM)

	ClearFileHdlChs = make([]chan int, FILEPRTNNUM)
	ClearHttpHdlChs = make([]chan int, HTTPPRTNNUM)

	reRdNum, err = strconv.Atoi(conf.GetValue("preproccess", "reRdNum"))
	if nil != err {
		reRdNum = 5
		Log.Info("reRdNum conf err, %d default.", reRdNum)
	}

	reRdInterval, err = strconv.Atoi(conf.GetValue("preproccess", "reRdInterval"))
	if nil != err {
		reRdInterval = 500
		Log.Info("reRdInterval conf err, %d default.", reRdInterval)
	}

	Log.Info("<HDFS> reRdNum: %d, reRdInterval: %d(ms)", reRdNum, reRdInterval)
}

func Hdfs() {
	InitHdfs()
	InitHdfsClis(AgentConf.HdfsNameNode)
	HdfsToLocals()
}
