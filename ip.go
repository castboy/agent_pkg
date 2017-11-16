//ip.go

package agent_pkg

import (
	"fmt"
	"net"
	"os"
	"regexp"
)

var Localhost string
var Partition int32

func GetLocalhost() {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		os.Stderr.WriteString("Oops:" + err.Error())
		os.Exit(1)
	}

	var ips []string
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

	env := os.Getenv("THIS_HOST")
	if "" != env {
		Localhost = env
	}

	fmt.Println("Localhost   : ", Localhost)

}

func GetPartition() {
	value, ok := AgentConf.Partition[Localhost]
	if ok {
		Partition = value
		fmt.Println("Partition   : ", Partition)
	}
}
