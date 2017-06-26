package agent_pkg

import (
)

type HdfsToLocalResTag struct {
    File string
    Success bool
}

func DisposeRdHdfs (ch chan HdfsToLocalRes, prefetchResMsg PrefetchResMsg) {
    data := *prefetchResMsg.DataPtr

    for key, val := range data {
        file, offset, size, xdrMark, prtn := xdrFields(val)
        
        hdfsToLocalReqParams := HdfsToLocalReqParams{
            File: file,
            Offset: offset,
            Size: size,
            XdrMark: xdrMark,
            Index: key, 
            HdfsToLocalResCh: ch,
        }
        
        HdfsToLocalReqChs[prtn] <- hdfsToLocalReqParams
    }
}

func xdrFields (bytes []byte) (string, int64, int, string, int) {
    str := `{"App":{"FileLocation":{"File":"wmg","Offset":100,"Size":100,"Signature":"123456"},"ClassId":0,"Proto":0,"ProtoInfo":0,"Status":0},"Class":103,"Conn":{"Dport":19465,"Proto":6,"Sport":36807},"ConnEx":{"Dir":false,"Over":false},"ConnSt":{"FlowDown":223,"FlowUp":340,"IpFragDown":0,"IpFragUp":0,"PktDown":1,"PktUp":2},"ConnTime":{"End":1496735792797969,"Start":1496735792797969},"Dns":{"AuthCnttCount":"0","Domain":"","ExtraRecordCount":"0","IpCount":"0","Ipv4":"","Ipv6":"","PktValid":"false","ReqCount":"0","RspCode":"0","RspDelay":"0","RspRecordCount":"0"},"Ftp":{"FileCount":0,"FileSize":0,"RspTm":0,"State":0,"TransMode":0,"TransTm":0,"TransType":0,"UserCount":0},"Http":{"HttpRequest":{"File":"wmg","Offset":100,"Size":100,"Signature":"123456"},"Browser":0,"ContentLen":49,"FirstResponseTime":1496735792797969,"LastContentTime":1496735792797969,"Method":6,"Portal":0,"RequestTime":1496735792797969,"ServFlag":0,"ServTime":0,"StateCode":200,"Version":3},"Id":0,"Ipv4":false,"Mail":{"AcsType":"0","DomainInfo":"","Hdr":"","Len":"0","MsgType":"0","RecvAccount":"","RecverInfo":"","RspState":"0","UserName":""},"Offline_Tag":"offline","Proxy":{"Type":0},"QQ":{"Number":""},"Rtsp":{"AudeoStreamCount":0,"ClientBeginPort":0,"ClientEndPort":0,"ResDelay":0,"ServerBeginPort":0,"ServerEndPort":0,"VideoStreamCount":0},"ServSt":{"FlowDown":0,"FlowUp":0,"IpFragDown":0,"IpFragUp":0,"PktDown":0,"PktUp":0,"TcpDisorderDown":0,"TcpDisorderUp":0,"TcpRetranDown":0,"TcpRetranUp":0},"Sip":{"CallDir":0,"CallType":0,"HangupReason":0,"SignalType":0,"StreamCount":0},"Task_Id":"20","Tcp":{"AckCount":0,"AckDelay":0,"Close":0,"CloseReason":0,"DisorderDown":0,"DisorderUp":0,"FirstRequestDelay":908262217,"FirstResponseDely":0,"Mss":0,"Open":0,"ReportFlag":1,"RetranDown":0,"RetranUp":0,"SynAckCount":0,"SynAckDelay":0,"SynCount":0,"Window":0},"Time":1496735792797969,"Type":1,"Vendor":"","Vpn":{"Type":0}}`

	var jsonParse interface{}
	json.Unmarshal([]byte(str), &jsonParse)
	m := jsonParse.(map[string]interface{})

	for _, v := range m {
		switch vv := v.(type) {
		case string:
		case float64:
		case int:
		case bool:
		case interface{}:
			n := vv.(map[string]interface{})
			for i, j := range n {
				switch jj := j.(type) {
				case string:
					fmt.Println(i, "is inner string", jj)
				case int:
				case bool:
				case float64:
				case interface{}:
					m := jj.(map[string]interface{})
					for z, l := range m {
						switch ll := l.(type) {
						case string:
							fmt.Println(z, "is last-inner string", ll)
						case int:
							fmt.Println(z, "is last-inner int", ll)
						case bool:
						case float64:
						case interface{}:
						default:
						}
					}
				default:
				}
			}
		default:
		}
	}
    return file, offset, size, xdrMark, prtn
}

func CollectHdfsToLocalRes (prefetchResMsg PrefetchResMsg, ch chan HdfsToLocalRes, tags []HdfsToLocalResTag) []HdfsToLocalResTag {
    dataNum := len(*prefetchResMsg.DataPtr)
    statNum := 0

    for {
        res := <- ch

        index := res.Index
        tags[index] = HdfsToLocalResTag{
            File: res.File,
            Success: res.Success,
        } 

        statNum++
        if statNum == dataNum {
            break
        }
    }

    return tags
}

func GetCache (tags []HdfsToLocalResTag, data [][]byte) [][]byte {
    var cache [][]byte

    for key, val := range tags {
        if val.Success {
            bytes := updateXdr(data, key, val.File)
            cache = append(cache, bytes)  
        }
    }

    return cache
}

func WriteCache(prefetchResMsg PrefetchResMsg, data [][]byte) {
    topic := prefetchResMsg.Topic
    engine := prefetchResMsg.Engine
    count := len(data)

    if engine == "waf" {
        WafCacheInfoMap[topic] = CacheInfo{0, count}
        CacheDataMap[topic] = data
    } else {
        VdsCacheInfoMap[prefetchResMsg.Topic] = CacheInfo{0, count}
        CacheDataMap[topic] = data
    }
}

func updateXdr (data [][]byte, index int, localFile string) []byte {
    bytes := data[index]

    appendStr := ", \"File\": " + localFile + "}"
    appendBytes := []byte(appendStr)
    
    bytes = bytes[ : len(bytes) - 1]
    for _, val := range appendBytes {
        bytes = append(bytes, val)
    }

    return bytes
}

func UpdateCacheCurrent(prefetchResMsg PrefetchResMsg) {
    topic := prefetchResMsg.Topic
    count := int64(len(*prefetchResMsg.DataPtr))

    if prefetchResMsg.Engine == "waf" {
        Waf[topic] = Status{Waf[topic].First, Waf[topic].Engine, Waf[topic].Cache+count,
                               Waf[topic].Last, Waf[topic].Weight}
    } else {
        Vds[topic] = Status{Vds[topic].First, Vds[topic].Engine, Vds[topic].Cache+count,
                               Vds[topic].Last, Vds[topic].Weight}
    }

    PrefetchMsgSwitchMap[topic] = true
}

func RdHdfs (prefetchResMsg PrefetchResMsg) {
    tags := make([]HdfsToLocalResTag, PRTNNUM)

    data := *prefetchResMsg.DataPtr

    hdfsToLocalResCh := make(chan HdfsToLocalRes)

    DisposeRdHdfs(hdfsToLocalResCh, prefetchResMsg)

    tags = CollectHdfsToLocalRes(prefetchResMsg, hdfsToLocalResCh, tags) 
    cache := GetCache(tags, data)

    WriteCache(prefetchResMsg, cache)
    UpdateCacheCurrent(prefetchResMsg)
}

