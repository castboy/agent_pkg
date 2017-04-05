//listen.go

package pkg_wmg

import (
    "net/http"
)

func ListenHttp () {
    mux := http.NewServeMux()
    mux.HandleFunc("/", ReadData)
    mux.HandleFunc("/start", StartOffline)
    mux.HandleFunc("/end", GetLastOffset)
    http.ListenAndServe(":9090", mux)
}
