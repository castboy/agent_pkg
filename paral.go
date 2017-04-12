package pkg_wmg

import (
    "fmt"
    "net/http"
)

var Ch chan int
var WafChIndex int

func Parallize(ChSlice chan [][]byte) {
    ChSlice = append(ChSlice, WafChIndex)    

    Ch <- WafChIndex
     
    Data := <-ChSlice[WafChIndex]

    WafChIndex++
}

func ParaBack(w http.ResponseWriter, req *http.Request) {
        
}

func Listen() {
     http.HandleFunc("/waf", Parallize)  
     err := http.ListenAndServe("192.168.1.27:8081", nil)  
}
