//consumer.go

package agent_pkg

import (
	"fmt"
	"log"

	"github.com/optiopay/kafka"
)

var broker kafka.Client
var Consumers = make(map[string]map[string]kafka.Consumer)

func InitConsumer(topic string, partition int32, start int64) (kafka.Consumer, error) {
	conf := kafka.NewConsumerConf(topic, partition)
	conf.StartOffset = start
	conf.RetryLimit = 1
	consumer, err := broker.Consumer(conf)

	if err != nil {
		errLog := fmt.Sprintf("cannot initConsumer of %s %d partition", topic, partition)
		Log("Err", errLog)
	}

	return consumer, err
}

func Offset(topic string, partition int32) (int64, int64, error, error) {
	start, startErr := broker.OffsetEarliest(topic, partition)
	if startErr != nil {
		errLog := fmt.Sprintf("cannot get start of %s %d partition", topic, partition)
		Log("Err", errLog)
	}
	end, endErr := broker.OffsetLatest(topic, partition)
	if endErr != nil {
		errLog := fmt.Sprintf("cannot get end of %s %d partition", topic, partition)
		Log("Err", errLog)
	}

	return start, end, startErr, endErr
}

func InitBroker(localhost string) {
	var kafkaAddrs []string = []string{localhost + ":9092", localhost + ":9093"}
	conf := kafka.NewBrokerConf("agent")
	conf.AllowTopicCreation = false

	var err error
	broker, err = kafka.Dial(kafkaAddrs, conf)
	if err != nil {
		Log("Err", "cannot connect to kafka cluster")
		log.Fatalf("cannot connect to kafka cluster: %s", err)
	}

	defer broker.Close()
}

func InitConsumers(partition int32) {
	Consumers["waf"] = make(map[string]kafka.Consumer)
	Consumers["vds"] = make(map[string]kafka.Consumer)

	for engine, val := range status {
		for topic, v := range val {
			consumer, err := InitConsumer(topic, partition, v.Engine)
			if nil != err {
				delete(val, topic)
			} else {
				Consumers[engine][topic] = consumer
			}
		}
	}
}

func UpdateOffset() {
	for engine, val := range status {
		for topic, v := range val {
			startOffset, endOffset, startErr, endErr := Offset(topic, Partition)
			if nil == startErr && nil == endErr {
				if startOffset > v.Engine {
					status[engine][topic] = Status{startOffset, startOffset, 0, startOffset, endOffset, v.Weight}
				} else {
					status[engine][topic] = Status{startOffset, v.Engine, 0, v.Engine, endOffset, v.Weight}
				}
				if v.Engine > endOffset {
					errLog := "conf err: xdrHttp msg-offset requested out of kafka msg-offset"
					Log("Err", errLog)
					log.Fatal(errLog)
				}
			}
		}
	}
	//fmt.Println("UpdateOffset: ", Waf, Vds)
	PrintUpdateOffset()
}

func PrintUpdateOffset() {
	fmt.Println("\n\nUpdateOffset:")

	fmt.Println("Waf")
	for key, val := range status["waf"] {
		fmt.Println(key, "     ", val)
	}

	fmt.Println("\nVds")
	for key, val := range status["vds"] {
		fmt.Println(key, "     ", val)
	}
}
