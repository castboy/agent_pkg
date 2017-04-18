//listen.go

package pkg_wmg

import (
    "net/http"
)

func ListenHttp () {
    mux := http.NewServeMux()
    mux.HandleFunc("/waf", ReadWaf)
    mux.HandleFunc("/vds", ReadVds)
    mux.HandleFunc("/start", StartOffline)
    mux.HandleFunc("/end", GetLastOffset)
    http.ListenAndServe(":9090", mux)
}
