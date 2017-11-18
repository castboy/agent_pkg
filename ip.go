//ip.go

package agent_pkg

import (
	"fmt"
	"log"
	"net"
	"os"
	"regexp"
)

var Localhost string
var Partition int32

func GetLocalhost() {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		Log("WRN", "%s", "The address list of the system's network interface failed")
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
		Log("TRC", "%s", `Localhost set by os-Env "THIS_HOST"`)
	}

	Log("TRC", "%s", `Localhost set by intranet`)
	fmt.Println("Localhost   : ", Localhost)
}

func GetPartition() {
	value, ok := AgentConf.Partition[Localhost]
	if ok {
		Partition = value
		Log("TRC", "%s: %s", `Partition`, Partition)
		fmt.Println("Partition   : ", Partition)
	} else {
		Log("CRT", "%s", "Can not get partiton")
		log.Fatal(exit)
	}
}
