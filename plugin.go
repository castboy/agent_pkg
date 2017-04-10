//plugin.go

package pkg_wmg

import (
	"log"
    "fmt"
	"github.com/optiopay/kafka"
)

const (
	localhostPartition = 0
)

var kafkaAddrs = []string{"10.80.6.9:9092", "10.80.6.9:9093"}
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

func Offset  (topic string, partition int32) (int64, int64) {
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
        wafConsumers[k] = InitConsumer(k, localhostPartition, v.Current)
        fmt.Println(v.First)
    }

    for k, v := range Vds {
        vdsConsumers[k] = InitConsumer(k, localhostPartition, v.Current)
    }

}
func UpdateOffset () {
    for k, v := range Waf {
        startOffset, endOffset := Offset(k, localhostPartition)
        Waf[k] = Partition{startOffset, v.Current, endOffset, v.Weight, false}   
    } 

    for k, v := range Vds {
        startOffset, endOffset := Offset(k, localhostPartition)
        Vds[k] = Partition{startOffset, v.Current, endOffset, v.Weight, false}   
    } 
}
