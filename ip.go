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
var KafkaHost string

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

	KafkaHost = AgentConf.KafkaHost
	fmt.Println("KafkaHost   : ", AgentConf.KafkaHost)

}

func GetPartition() {
	Partition = AgentConf.KafkaPartition
}
