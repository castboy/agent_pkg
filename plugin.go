//plugin.go

package pkg_wmg

import (
	"log"
    "fmt"
	"github.com/optiopay/kafka"
)

var localhostPartition int32 = MyConf.Partition

var broker kafka.Client
var consumer kafka.Consumer
var wafConsumers map[string]kafka.Consumer
var vdsConsumers map[string]kafka.Consumer
var consumerPtr *map[string]kafka.Consumer 

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
    }
    end, err := broker.OffsetLatest(topic, partition)
    if err != nil {
        log.Fatalf("cannot get end %s", err)
    }

    return start, end
}

func InitBroker () {
    var kafkaAddrs []string = []string{MyConf.HostPort1, MyConf.HostPort2}
	conf := kafka.NewBrokerConf("wmg-test-client")
	conf.AllowTopicCreation = false

    var err error
	broker, err = kafka.Dial(kafkaAddrs, conf)
	if err != nil {
		log.Fatalf("cannot connect to kafka cluster: %s", err)
	}

	defer broker.Close()
}

func InitConsumers () {
    wafConsumers = make(map[string] kafka.Consumer)
    vdsConsumers = make(map[string] kafka.Consumer)

    for k, v := range Waf {
        wafConsumers[k] = InitConsumer(k, localhostPartition, v.Engine)
    }

    for k, v := range Vds {
        vdsConsumers[k] = InitConsumer(k, localhostPartition, v.Engine)
    }

}

func UpdateOffset () {
    for k, v := range Waf {
        startOffset, endOffset := Offset(k, localhostPartition)
        if startOffset > v.Engine {
            Waf[k] = Partition{startOffset, startOffset, startOffset, endOffset, v.Weight, false}   
        } else {
            Waf[k] = Partition{startOffset, v.Engine, v.Engine, endOffset, v.Weight, false}   
        }
        
    } 

    for k, v := range Vds {
        startOffset, endOffset := Offset(k, localhostPartition)
        if startOffset > v.Engine {
            Vds[k] = Partition{startOffset, startOffset, startOffset, endOffset, v.Weight, false}   
        } else {
            Vds[k] = Partition{startOffset, v.Engine, v.Engine, endOffset, v.Weight, false}   
        }
    } 
    fmt.Println(Waf, Vds)
}
