//consumer.go

package agent_pkg

import (
	"log"
    "fmt"
	"github.com/optiopay/kafka"
    "os"
)

var broker kafka.Client
var consumer kafka.Consumer
var wafConsumers map[string]kafka.Consumer
var vdsConsumers map[string]kafka.Consumer

func InitConsumer (topic string, partition int32, start int64) kafka.Consumer {
	conf := kafka.NewConsumerConf(topic, partition)
	conf.StartOffset = start
    conf.RetryLimit = 1
	consumer, _ = broker.Consumer(conf)
	
    return consumer
}

func Offset (topic string, partition int32) (int64, int64) {
    start, err := broker.OffsetEarliest(topic, partition)
    if err != nil {
        log.Fatalf("cannot get start %s", err)
        errLog := "cannot get start of" + topic + "-partition" + string(partition)
        Log("Err", errLog)
    }
    end, err := broker.OffsetLatest(topic, partition)
    if err != nil {
        log.Fatalf("cannot get end %s", err)
        errLog := "cannot get end of" + topic + "-partition" + string(partition)
        Log("Err", errLog)
    }

    return start, end
}

func InitBroker (localhost string) {
    var kafkaAddrs []string = []string{localhost+":9092", localhost+":9093"}
	conf := kafka.NewBrokerConf("wmg-test-client")
	conf.AllowTopicCreation = false

    var err error
	broker, err = kafka.Dial(kafkaAddrs, conf)
	if err != nil {
		log.Fatalf("cannot connect to kafka cluster: %s", err)
        errLog := "cannot connect to kafka cluster"
        Log("Err", errLog)
	}

	defer broker.Close()
}

func InitConsumers (partition int32) {
    wafConsumers = make(map[string] kafka.Consumer)
    vdsConsumers = make(map[string] kafka.Consumer)

    for k, v := range Waf {
        wafConsumers[k] = InitConsumer(k, partition, v.Engine)
    }

    for k, v := range Vds {
        vdsConsumers[k] = InitConsumer(k, partition, v.Engine)
    }

}

func UpdateOffset () {
    for k, v := range Waf {
        startOffset, endOffset := Offset(k, Partition)
        if startOffset > v.Engine {
            Waf[k] = Status{startOffset, startOffset, startOffset, endOffset, v.Weight}   
        } else {
            Waf[k] = Status{startOffset, v.Engine, v.Engine, endOffset, v.Weight}   
        }
        if v.Engine > endOffset {
            fmt.Println("Waf", Waf)
            fmt.Println("conf err: xdrHttp msg-offset requested out of kafka msg-offset")
            errLog := "conf err: xdrHttp msg-offset requested out of kafka msg-offset"
            Log("Err", errLog)
            os.Exit(0)
        }
        
    } 

    for k, v := range Vds {
        startOffset, endOffset := Offset(k, Partition)
        if startOffset > v.Engine {
            Vds[k] = Status{startOffset, startOffset, startOffset, endOffset, v.Weight}   
        } else {
            Vds[k] = Status{startOffset, v.Engine, v.Engine, endOffset, v.Weight}   
        }
        if v.Engine > endOffset {
            fmt.Println("Vds", Vds)
            fmt.Println("conf err: xdrFile msg-offset requested out of kafka msg-offset")
            errLog := "conf err: xdrFile msg-offset requested out of kafka msg-offset"
            Log("Err", errLog)
            os.Exit(0)
        }
    } 
    //fmt.Println("UpdateOffset: ", Waf, Vds)
    PrintUpdateOffset()
}

func PrintUpdateOffset() {
    fmt.Println("\n\nUpdateOffset:")

    fmt.Println("Waf")
    for key, val := range Waf {
        fmt.Println(key, "     ", val)
    }

    fmt.Println("\nVds")
    for key, val := range Vds {
        fmt.Println(key, "     ", val)
    }
}
