package agent_pkg

import (
    "github.com/colinmarc/hdfs"
    "fmt"
    "io/ioutil"
    "crypto/sha256" 
    "os"
    "strconv"
    "time"
)

type HttpHdfsToLocalReqParams struct {
    Engine string
    File []string
    Offset []int64
    Size []int
    XdrMark []string
    
    Index int

    HdfsToLocalResCh chan HdfsToLocalRes 
}

type FileHdfsToLocalReqParams struct {
    Engine string
    File string
    Offset int64
    Size int
    XdrMark string
    
    Index int

    HdfsToLocalResCh chan HdfsToLocalRes 
}

type HdfsToLocalRes struct {
    File []string

    Index int

    Success bool    
}

const (
    FILEPRTNNUM = 64
    HTTPPRTNNUM = 64
)

var (
    HttpHdfsToLocalReqChs [HttpPRTNNUM]chan HttpHdfsToLocalReqParams
    FileHdfsToLocalReqChs [FilePRTNNUM]chan FileHdfsToLocalReqParams
    
    client *hdfs.Client
)

func InitHdfsCli (namenode string) {
    client, _ = hdfs.New(namenode)
}

func HttpHdfsToLocal (idex int) {
    p := <-FileHdfsToLocalReqChs[idx]

    file := make([]string, 2)
    file[0] = p.File[0] + "_" + strconv.FormatInt(p.Offset[0], 10) + "_" + 
              strconv.Itoa(p.Size[0]) + "_" + strconv.Itoa(runTime) 
    file[1] = p.File[1] + "_" + strconv.FormatInt(p.Offset[1], 10) + "_" + 
              strconv.Itoa(p.Size[1]) + "_" + strconv.Itoa(runTime) 

    bytes, runTime := hdfsRd(p.File[0])
    ok1 := isRightFile(bytes, p.File[0])

    bytes, runTime := hdfsRd(p.File[1])
    ok2 := isRightFile(bytes, p.File[1])

    wrOk := false
    if ok1 && ok2 {
        wrOK1 := localWrite(file[0], bytes)
        wrOK2 := localWrite(file[1], bytes)
        wrOk = true
    }
    res := HdfsToLocalRes{
        File: file,
        Index: p.Index,
        Success: wrOk,
    }

    p.HdfsToLocalResCh <- res
}

func FileHdfsToLocal (idx int) {
    p := <-FileHdfsToLocalReqChs[idx]
   
    wrOK := false
    file := make([]string, 1)

    bytes, runTime := hdfsRd(p.File[0])
    ok := isRightFile(bytes, p.File[0])
    if ok {
        file[0] = p.File[0] + "_" + strconv.FormatInt(p.Offset[0], 10) + "_" + 
                strconv.Itoa(p.Size[0]) + "_" + strconv.Itoa(runTime) 
        wrOK = localWrite(file[0], bytes)
    }

    res := HdfsToLocalRes{
        File: file,
        Index: p.Index,
        Success: wrOk,
    }

    p.HdfsToLocalResCh <- res
}

func hdfsRd (file string, size int, offset int64) (bytes []byte, runTime int) {
    beginTime := time.Now().Nanosecond()

    if fileIsExist(file) {
        file, _ := client.Open(file)
        bytes := make([]byte, size)
        file.ReadAt(bytes, offset)
    }

    endTime := time.Now().Nanosecond()
    
    return bytes, endTime - beginTime 
}

func fileIsExist(file string) (bool) {
    var exist = true;
    if _, err := os.Stat(file); os.IsNotExist(err) {
        exist = false;
    }
    return exist;
}

func isRightFile (hdfs []byte, xdrMark string) bool {
    right := true
 
    if xdrMark != sha256Code(hdfs) {
        right = false
    }
 
    return right     
}

func sha256Code (bytes []byte) string {
    h := sha256.New()
    h.Write(bytes)
    return fmt.Sprintf("%x", h.Sum(nil))
}

func localWrite (file string, bytes []byte) bool {
    success := true

    err := ioutil.WriteFile(file, bytes, 0666)
    if nil != err {
        success = false
    }

    return success
}

func HdfsToLocals () {
    for i := 0; i < HTTPPRTNNUM; i++ {
        go HttpHdfsToLocal(i)
    }
    for i := 0; i < FILEPRTNNUM; i++ {
        go FileHdfsToLocal(i)
    }
}
