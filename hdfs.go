package agent_pkg

import (
	"crypto/sha256"
	"fmt"
	"io/ioutil"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/colinmarc/hdfs"
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
	FILEPRTNNUM = 64
	HTTPPRTNNUM = 64
	CHECKFILE   = true
)

var (
	HttpHdfsToLocalReqChs [HTTPPRTNNUM]chan HdfsToLocalReqParams
	FileHdfsToLocalReqChs [FILEPRTNNUM]chan HdfsToLocalReqParams

	client *hdfs.Client
)

type HdfsFileHdl struct {
	Hdl     *hdfs.FileReader
	ReqTime int64
}

func InitHdfsCli(namenode string) {
	client, _ = hdfs.New(namenode)
}

var ClearFileHdlChs [FILEPRTNNUM]chan int
var ClearHttpHdlChs [HTTPPRTNNUM]chan int

func SendClearFileHdlMsg(hours int) {
	ticker := time.NewTicker(time.Hour * time.Duration(hours))
	for _ = range ticker.C {
		for _, ch := range ClearFileHdlChs {
			ch <- hours
		}
		for _, ch := range ClearHttpHdlChs {
			ch <- hours
		}
	}

}

func HttpHdfs(idx int) {
	fHdl := make(map[string]HdfsFileHdl)
	ClearHttpHdlChs[idx] = make(chan int)

	for {
		select {
		case msg := <-HttpHdfsToLocalReqChs[idx]:
			HttpHdfsToLocal(fHdl, msg)
		case msg := <-ClearHttpHdlChs[idx]:
			ClearHdl(fHdl, msg)
		}
	}
}

func FileHdfs(idx int) {
	fHdl := make(map[string]HdfsFileHdl)
	ClearFileHdlChs[idx] = make(chan int)

	for {
		select {
		case msg := <-FileHdfsToLocalReqChs[idx]:
			FileHdfsToLocal(fHdl, msg)
		case msg := <-ClearFileHdlChs[idx]:
			ClearHdl(fHdl, msg)
		}
	}
}

func ClearHdl(fileHdl map[string]HdfsFileHdl, hours int) {
	timestamp := time.Now().Unix()

	for key, val := range fileHdl {
		if val.ReqTime+int64(hours*60) < timestamp {
			delete(fileHdl, key)
		}
	}
}

func HttpHdfsToLocal(fileHdl map[string]HdfsFileHdl, p HdfsToLocalReqParams) {
	_, exist := fileHdl[p.SrcFile]
	if !exist {
		f, _ := client.Open(p.SrcFile)
		timestamp := time.Now().Unix()
		fileHdl[p.SrcFile] = HdfsFileHdl{f, timestamp}
	}

	reqBytes, _ := hdfsRd(fileHdl[p.SrcFile].Hdl, p.SrcFile, p.Offset[0], p.Size[0])
	reqRight := isRightFile(reqBytes, p.XdrMark[0])

	resBytes, _ := hdfsRd(fileHdl[p.SrcFile].Hdl, p.SrcFile, p.Offset[1], p.Size[1])
	resRight := isRightFile(resBytes, p.XdrMark[1])

	wrOk := false
	if reqRight && resRight {
		wrReqOk := localWrite(p.DstFile[0], reqBytes)
		wrResOk := localWrite(p.DstFile[1], resBytes)
		wrOk = wrReqOk && wrResOk
		fmt.Println("wrOk:", wrOk)
	}
	res := HdfsToLocalRes{
		Index:   p.Index,
		Success: wrOk,
	}

	p.HdfsToLocalResCh <- res

}

func FileHdfsToLocal(fileHdl map[string]HdfsFileHdl, p HdfsToLocalReqParams) {
	_, exist := fileHdl[p.SrcFile]
	if !exist {
		f, _ := client.Open(p.SrcFile)
		timestamp := time.Now().Unix()
		fileHdl[p.SrcFile] = HdfsFileHdl{f, timestamp}
	}

	wrOk := false

	bytes, _ := hdfsRd(fileHdl[p.SrcFile].Hdl, p.SrcFile, p.Offset[0], p.Size[0])
	ok := isRightFile(bytes, p.XdrMark[0])
	if ok {
		wrOk = localWrite(p.DstFile[0], bytes)
	}

	res := HdfsToLocalRes{
		Index:   p.Index,
		Success: wrOk,
	}

	p.HdfsToLocalResCh <- res
}

func hdfsRd(fHdl *hdfs.FileReader, file string, offset int64, size int) (bytes []byte, runTime int) {
	defer func() {
		if r := recover(); r != nil {
			fmt.Printf("hdfsRd err: %v\n", r)
			fmt.Println("file: ", file, "    offset: ", offset, "   size: ", size)
			errInfo := "file: " + file + " offset: " + strconv.FormatInt(offset, 10) +
				" size: " + strconv.Itoa(size)
			Log("Err", errInfo)
		}
	}()

	fmt.Println("file:", file)
	beginTime := time.Now().Nanosecond()

	bytes = make([]byte, size)
	int, err := fHdl.ReadAt(bytes, offset)

	if nil != err {
		fmt.Println(file, offset, size, int)
	}
	endTime := time.Now().Nanosecond()
	//fmt.Println("hdfs-rd:", bytes)

	return bytes, endTime - beginTime
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
		err := os.MkdirAll(dir, 0777)
		if err != nil {
			fmt.Printf("%s", err)
		} else {
			fmt.Print("Create Directory OK!")
		}
	}

	f, err := os.Create(file)
	if nil != err {
		fmt.Println(err.Error())
	}

	defer f.Close()

	err = ioutil.WriteFile(file, bytes, 0644)
	if nil != err {
		success = false
	}

	return success
}

func pathExists(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}

	return false, err
}

func HdfsToLocals() {
	for i := 0; i < HTTPPRTNNUM; i++ {
		HttpHdfsToLocalReqChs[i] = make(chan HdfsToLocalReqParams, 100)
		go HttpHdfs(i)
	}
	for i := 0; i < FILEPRTNNUM; i++ {
		FileHdfsToLocalReqChs[i] = make(chan HdfsToLocalReqParams, 100)
		go FileHdfs(i)
	}
}
