package agent_pkg

import (
	"crypto/sha256"
	"fmt"
	"io/ioutil"
	"log"
	"os"
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
	var err error
	client, err = hdfs.New(namenode + ":8020")
	if nil != err {
		errLog := fmt.Sprintf("Init Hdfs Client Err: %s", err.Error())
		Log("Err", errLog)
		log.Fatalf(errLog)
	}
}

var ClearFileHdlChs, ClearHttpHdlChs [FILEPRTNNUM]chan int

func SendClearFileHdlMsg(seconds int) {
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
	fHdl := make(map[string]HdfsFileHdl)
	ClearHttpHdlChs[idx] = make(chan int)

	for {
		select {
		case msg := <-HttpHdfsToLocalReqChs[idx]:
			HttpHdfsToLocal(fHdl, msg)
		case msg := <-ClearHttpHdlChs[idx]:
			ClearHdl(fHdl, msg)
		default:
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
		default:
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
		f, err := client.Open(p.SrcFile)
		if nil != err {
			errLog := fmt.Sprintf("Hdfs Open Err: %s", err.Error())
			Log("Err", errLog)
		} else {
			timestamp := time.Now().Unix()
			fileHdl[p.SrcFile] = HdfsFileHdl{f, timestamp}
		}
	} else {
		timestamp := time.Now().Unix()
		fileHdl[p.SrcFile] = HdfsFileHdl{fileHdl[p.SrcFile].Hdl, timestamp}
	}

	reqBytes := hdfsRd(fileHdl[p.SrcFile].Hdl, p.SrcFile, p.Offset[0], p.Size[0])
	reqRight := isRightFile(reqBytes, p.XdrMark[0])

	resBytes := hdfsRd(fileHdl[p.SrcFile].Hdl, p.SrcFile, p.Offset[1], p.Size[1])
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
		f, err := client.Open(p.SrcFile)
		if nil != err {
			errLog := fmt.Sprintf("Hdfs Open Err: %s", err.Error())
			Log("Err", errLog)
		} else {
			timestamp := time.Now().Unix()
			fileHdl[p.SrcFile] = HdfsFileHdl{f, timestamp}
		}
	} else {
		timestamp := time.Now().Unix()
		fileHdl[p.SrcFile] = HdfsFileHdl{fileHdl[p.SrcFile].Hdl, timestamp}
	}

	wrOk := false

	bytes := hdfsRd(fileHdl[p.SrcFile].Hdl, p.SrcFile, p.Offset[0], p.Size[0])
	ok := isRightFile(bytes, p.XdrMark[0])

	//	if !ok {
	//		errNum := 1
	//		for {
	//			bytes = hdfsRd(fileHdl[p.SrcFile].Hdl, p.SrcFile, p.Offset[0], p.Size[0])
	//			ok := isRightFile(bytes, p.XdrMark[0])
	//			if ok {
	//				break
	//			} else {
	//				errNum++
	//			}
	//		}
	//		errInfo := "file: " + p.SrcFile + " offset: " + strconv.FormatInt(p.Offset[0], 10) +
	//			" size: " + strconv.Itoa(p.Size[0]) + " errNum: " + strconv.Itoa(errNum)
	//		Log("Err", errInfo)
	//	}

	if ok {
		wrOk = localWrite(p.DstFile[0], bytes)
	}

	res := HdfsToLocalRes{
		Index:   p.Index,
		Success: wrOk,
	}

	p.HdfsToLocalResCh <- res
}

func hdfsRd(fHdl *hdfs.FileReader, file string, offset int64, size int) (bytes []byte) {
	bytes = make([]byte, size)
	_, err := fHdl.ReadAt(bytes, offset)

	if nil != err {
		errLog := fmt.Sprintf("Read Hdfs Err: file %s is %d bytes, but you want %d bytes from offset %d, %s",
			file, fHdl.Stat().Size(), size, offset, err.Error())
		Log("Err", errLog)
	}

	return bytes
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
			fmt.Printf("%s", err.Error())
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
