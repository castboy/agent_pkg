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

type HdfsToLocalReqParams struct {
    File string
    Offset int64
    Size int

    XdrMark string
    
    Index int

    HdfsToLocalResCh chan HdfsToLocalRes 
}

type HdfsToLocalRes struct {
    File string

    Index int

    Success bool    
}

const (
    PRTNNUM = 128
)

var (
    HdfsToLocalReqChs [PRTNNUM]chan HdfsToLocalReqParams
    
    client *hdfs.Client
)

var HdfsToLocalResCh = make(chan HdfsToLocalRes, 1000)

func InitHdfsCli (namenode string) {
    client, _ = hdfs.New(namenode)
}

func HdfsToLocal (idx int) {
    p := <-HdfsToLocalReqChs[idx]
    
    wrSuccess := false
    file := ""

    bytes, runTime := hdfsRd(p)
    ok := isRightFile(bytes, p)
    if ok {
        file = p.File + "_" + strconv.FormatInt(p.Offset, 10) + "_" + 
                strconv.Itoa(p.Size) + "_" + strconv.Itoa(runTime) 
        wrSuccess = localWrite(file, bytes)
    }
    
    res := HdfsToLocalRes{
        File: file,
        Index: p.Index,
        Success: wrSuccess,
    }
    
    p.HdfsToLocalResCh <- res
}

func hdfsRd (ch HdfsToLocalReqParams) (bytes []byte, runTime int) {
    beginTime := time.Now().Nanosecond()

    if fileIsExist(ch.File) {
        file, _ := client.Open(ch.File)
        bytes := make([]byte, ch.Size)
        file.ReadAt(bytes, ch.Offset)
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

func isRightFile (hdfs []byte, ch HdfsToLocalReqParams) bool {
    right := true
 
    if ch.XdrMark != sha256Code(hdfs) {
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
    for i := 0; i < PRTNNUM; i++ {
        go HdfsToLocal(i)
    }
}
