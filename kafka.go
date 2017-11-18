//consumer.go

package agent_pkg

import (
	"fmt"
	"log"

	"github.com/optiopay/kafka"
)

var broker kafka.Client
var consumers = make(map[string]map[string]kafka.Consumer)

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

func kafkaAddrs() []string {
	var addrs []string
	addrs = append(addrs, Localhost+":9092")
	for k, _ := range AgentConf.Partition {
		addrs = append(addrs, k+":9092")
	}

	return addrs
}

func InitBroker() {
	var kafkaAddrs []string = kafkaAddrs()
	conf := kafka.NewBrokerConf("agent")
	conf.AllowTopicCreation = false

	broker, err = kafka.Dial(kafkaAddrs, conf)
	if err != nil {
		Log("CRT", "%s, broker lists %s", "can not connect to kafka cluster", kafkaAddrs)
		log.Fatal(exit)
	}

	Log("INF", "%s", "init kafka broker ok")
}

func InitConsumers(partition int32) {
	for _, v := range reqTypes {
		consumers[v] = make(map[string]kafka.Consumer)
	}

	for engine, val := range status {
		for topic, v := range val {
			consumer, err := InitConsumer(topic, partition, v.Engine)
			if nil != err {
				delete(val, topic)
				Log("ERR", "%s: topic: %s,   partition: %d,  engine: %s", "init consumer err", topic, partition, v.Engine)
			} else {
				consumers[engine][topic] = consumer
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
					status[engine][topic] = Status{startOffset, endOffset, 0, endOffset, endOffset, v.Weight}
					wrn := fmt.Sprintf("%s %d partition offset is set to last-offset as what you set is out of kafka current offset", topic, Partition)
					Log("WRN", "%s", wrn)
					fmt.Println(wrn)
				}

				Log("INF", "%s %d partition offset after UpdateOffset", topic, Partition, status[engine][topic])
			} else {
				Log("ERR", "can not get start or end offset of %s %d partition", topic, Partition)
			}
		}
	}
}

func Kafka() {
	InitBroker()
	InitConsumers(Partition)
	UpdateOffset()
}
