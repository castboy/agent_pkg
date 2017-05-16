//ip.go

package agent_pkg

import (
    "os"
    "net"
    "fmt"
)

var Localhost string
var Partition int32

func GetLocalhost() {
    addrs, err := net.InterfaceAddrs()
    if err != nil {
        os.Stderr.WriteString("Oops:" + err.Error())
        os.Exit(1)
    }

    n := 0
    for _, a := range addrs {
        if ipnet, ok := a.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
            if ipnet.IP.To4() != nil {
                if 0 == n {
                    //os.Stdout.WriteString(ipnet.IP.String() + "\n")
                    Localhost =  ipnet.IP.String()
                }
            }
        }
        n++
    }
    Localhost = "192.168.1.103"
    fmt.Println("Localhost   : ", Localhost)
}

func GetPartition() {
    value, ok := AgentConf.Partition[Localhost] 
    if ok {
        Partition = value    
        fmt.Println("Partition   : ", Partition)
    }
}


