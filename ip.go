//ip.go

package agent_pkg

import (
	"net"
        "fmt"
	"os"
	"regexp"
)

var Localhost string
var Partition int32

func GetLocalhost() {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		Log.Warn("%s", "The address list of the system's network interface failed")
	} else {
		ips := []string{}
		for _, a := range addrs {
			if ipnet, ok := a.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
				if ipnet.IP.To4() != nil {
					ips = append(ips, ipnet.IP.String())
				}
			}
		}
		for _, ip := range ips {
			match, _ := regexp.MatchString("^192.*", ip)
			if match {
				Localhost = ip
			}
		}
	}

	env := os.Getenv("THIS_HOST")
	if "" != env {
		Localhost = env
		Log.Trace("%s", `Localhost set by os-Env "THIS_HOST"`)
	} else {
		Log.Trace("%s", `Localhost set by intranet`)
	}
}

func GetPartition() {
	value, ok := AgentConf.Partition[Localhost]
	if ok {
		Partition = value
                fmt.Println("Partition", Partition)
		Log.Trace("Consume Partition: %d", Partition)
	} else {
		LogCrt("%s", "Consume Partition Unknow")
	}
}
