package agent_pkg

import (
    "github.com/colinmarc/hdfs"
    "fmt"
    "io/ioutil"
    "crypto/sha256" 
)

type ToUpdateDataInfo struct {
    Topic string
    DataIndex int
} 

type HdfsToLocalReqParams struct {
    File string
    Offset int64
    Size int

    XdrMark string
    
    ToUpdateData ToUpdateDataInfo 
}

type HdfsToLocalRes struct {
    Topic string
    
    XdrBytesPtr *[]byte
}

const (
    PRTNNUM = 128
)

var (
    HdfsToLocalReqChs [PRTNNUM]chan HdfsToLocalReqParams
    HdfsToLocalResCh chan HdfsToLocalRes
    
    client *hdfs.Client
)

func InitHdfsCli (namenode string) {
    client, _ = hdfs.New(namenode)
}

func HdfsToLocal (idx int) {
    p := <-HdfsToLocalReqChs[idx]
    
    var xdrBytesPtr = new([]byte) 

    bytes, runTime := hdfsRd(p)
    ok := isRightFile(bytes, p.XdrMark)
    if ok {
        file := p.File + "_" + strconv.Itoa(p.Offset) + "_" + 
                strconv.Itoa(p.Size) + "_" + strconv.Itoa(runTime) 
        localWrite(file, bytes)
        
        *xdrBytesPtr = updateXdr(p.ToUpdateData, file)
    }
    
    res := {
        Topic: p.ToUpdateData.Topic,
        XdrBytesPtr: xdrBytesPtr,
    }
    
    HdfsToLocalResCh <- res
}

func hdfsRd (ch chan HdfsToLocalReqParams) (bytes []byte, runTime int) {
    beginTime := time.Now().Nanosecond()

    if fileIsExist(ch.File) {
        file, _ := client.Open(ch.File)
        bytes := make([]byte, ch.Size)
        file.ReadAt(bytes, ch.Offset)
    }

    endTime := time.Now().Nanosecond()
    
    return buf, endTime - beginTime 
}

func fileIsExist(file string) (bool) {
    var exist = true;
    if _, err := os.Stat(file); os.IsNotExist(err) {
        exist = false;
    }
    return exist;
}

func isRightFile (hdfs []byte, xdrMark string) {
    right := true
 
    if xdrMark != getSha256Code(hdfs) {
        right = false
    }
 
    return right     
}

func sha256Code (bytes []byte) string {
    h := sha256.New()
    h.Write(bytes)
    return fmt.Sprintf("%x", h.Sum(nil))
}

func localWrite (file string, bytes []byte) {
    err := ioutil.WriteFile(file, bytes, 0666)
    if nil != err {
        fmt.Println("LocalWrite Err")
    }
}

func updateXdr (info ToUpdateDataInfo, localFile string) []byte {
    bytes := CacheDataMap[info.Topic][info.DataIndex]
    
    appendStr = ", \"File\": " + localFile + "}" 
    appendBytes = []byte(appendStr)

    bytes = append(bytes[ : len(bytes) - 1], appendBytes)
    
    return bytes
}

