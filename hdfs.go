package agent_pkg

import (
	"crypto/sha256"
	"fmt"
	"io/ioutil"
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

var httpHdl = make(map[string]*hdfs.FileReader)
var fileHdl = make(map[string]*hdfs.FileReader)

func InitHdfsCli(namenode string) {
	client, _ = hdfs.New(namenode)
}

func HttpHdfsToLocal(idx int) {
	for {
		p := <-HttpHdfsToLocalReqChs[idx]

		fHdl, exist := httpHdl[p.SrcFile]
		if !exist {
			f, _ := client.Open(p.SrcFile)
			httpHdl[p.SrcFile] = f
		}

		reqBytes, _ := hdfsRd(fHdl, p.SrcFile, p.Offset[0], p.Size[0])
		reqRight := isRightFile(reqBytes, p.XdrMark[0])

		resBytes, _ := hdfsRd(fHdl, p.SrcFile, p.Offset[1], p.Size[1])
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
			Success: true,
		}

		p.HdfsToLocalResCh <- res
	}
}

func FileHdfsToLocal(idx int) {
	for {
		p := <-FileHdfsToLocalReqChs[idx]

		fHdl, exist := httpHdl[p.SrcFile]
		if !exist {
			f, _ := client.Open(p.SrcFile)
			httpHdl[p.SrcFile] = f
		}

		wrOk := false

		bytes, _ := hdfsRd(fHdl, p.SrcFile, p.Offset[0], p.Size[0])
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
}

func hdfsRd(fHdl *hdfs.FileReader, file string, offset int64, size int) (bytes []byte, runTime int) {
	defer func() {
		if r := recover(); r != nil {
			fmt.Printf("consume err: %v", r)
		}
	}()

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
		go HttpHdfsToLocal(i)
	}
	for i := 0; i < FILEPRTNNUM; i++ {
		FileHdfsToLocalReqChs[i] = make(chan HdfsToLocalReqParams, 100)
		go FileHdfsToLocal(i)
	}
}
