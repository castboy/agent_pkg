package agent_pkg

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"strings"

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
	FILEPRTNNUM  = 64
	HTTPPRTNNUM  = 64
	CHECKFILE    = true
	READTOLERANT = 3
)

var (
	HttpHdfsToLocalReqChs [HTTPPRTNNUM]chan HdfsToLocalReqParams
	FileHdfsToLocalReqChs [FILEPRTNNUM]chan HdfsToLocalReqParams

	client *hdfs.Client

	clientBuildTime int64
)

var ReHdfsCliChs = make(chan int, 300)

type HdfsFileHdl struct {
	Hdl     *hdfs.FileReader
	ReqTime int64
}

func InitHdfsCli(namenode string) {
	var err error
	client, err = hdfs.New(namenode + ":8020")
	if nil != err {
		LogCrt("Init Hdfs Client Err, %s", err.Error())
	}
}

func HttpHdfs(idx int) {
	for {
		msg := <-HttpHdfsToLocalReqChs[idx]
		HttpHdfsToLocal(msg)
	}
}

func FileHdfs(idx int) {
	for {
		msg := <-FileHdfsToLocalReqChs[idx]
		FileHdfsToLocal(msg)
	}
}

func HttpHdfsToLocal(p HdfsToLocalReqParams) {
	var res HdfsToLocalRes

	if nil != err {
		res = HdfsToLocalRes{
			Index:   p.Index,
			Success: false,
		}
	} else {
		reqBytes, reqRight := hdfsRdCheck(p.SrcFile, p.Offset[0], p.Size[0], p.XdrMark[0])
		resBytes, resRight := hdfsRdCheck(p.SrcFile, p.Offset[1], p.Size[1], p.XdrMark[1])

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

func FileHdfsToLocal(p HdfsToLocalReqParams) {
	var res HdfsToLocalRes

	wrOk := false

	b, rdOk := hdfsRdCheck(p.SrcFile, p.Offset[0], p.Size[0], p.XdrMark[0])

	if rdOk {
		wrOk = localWrite(p.DstFile[0], b)
	}

	res = HdfsToLocalRes{
		Index:   p.Index,
		Success: wrOk,
	}

	p.HdfsToLocalResCh <- res
}

func hdfsRdCheck(file string, offset int64, size int, mark string) ([]byte, bool) {
	errNum := 0
	var b []byte

	for errNum < READTOLERANT {
		bytes, err := hdfsRd(file, offset, size)
		if nil != err {

		} else {
			ok := isRightFile(bytes, mark)
			if ok {
				return bytes, true
			}
		}
		errNum++
	}

	return b, false
}

func hdfsRd(file string, offset int64, size int) ([]byte, error) {
	var bytes []byte

	fHdl, err := client.Open(file)

	if err != nil {
		if _, ok := err.(*os.PathError); ok {
			Log.Error("First Open Hdfs File %s Path Err", file)
			return bytes, errors.New("PathError")
		} else {
			os.Exit(1)
		}
	}

	defer fHdl.Close()

	bytes = make([]byte, size)
	_, err = fHdl.ReadAt(bytes, offset)

	if nil != err {
		Log.Error("Read Hdfs, file = %s, offset = %d, size = %d fileSize = %d", file, offset, size, fHdl.Stat().Size())
	}

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
		Log.Error("Write local file %s failed", file)
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

func Hdfs() {
	InitHdfsCli(AgentConf.HdfsNameNode)
	HdfsToLocals()
}
