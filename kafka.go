//consumer.go

package agent_pkg

import (
	"github.com/optiopay/kafka"
)

var broker kafka.Client
var consumers = make(map[string]map[string]kafka.Consumer)

func InitConsumer(topic string, partition int32, start int64) (kafka.Consumer, error) {
	conf := kafka.NewConsumerConf(topic, partition)
	conf.StartOffset = start
	conf.RetryLimit = 1
	consumer, err := broker.Consumer(conf)

	return consumer, err
}

func Offset(topic string, partition int32) (int64, int64, error, error) {
	start, startErr := broker.OffsetEarliest(topic, partition)
	end, endErr := broker.OffsetLatest(topic, partition)

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
	defer func() {
		if err := recover(); nil != err {
			LogCrt("PANIC in InitBroker(), %v", err)
		}
	}()

	var kafkaAddrs []string = kafkaAddrs()
	conf := kafka.NewBrokerConf("agent")
	conf.AllowTopicCreation = false

	broker, err = kafka.Dial(kafkaAddrs, conf)
	if err != nil {
		LogCrt("can not connect to kafka cluster, broker lists %s", kafkaAddrs)
	}

	Log.Info("%s", "init kafka broker ok")
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
				Log.Error("init consumer, topic = %s, partition = %d, engine = %s", topic, partition, v.Engine)
			} else {
				consumers[engine][topic] = consumer
			}
		}
	}
}

func UpdateOffset() {
	for engine, val := range status {
		for topic, v := range val {
			if v.Weight == 0 {
				v.Weight = 5
				Log.Error("%s: s.Weight==0", "UpdateOffset")
			}
			startOffset, endOffset, startErr, endErr := Offset(topic, Partition)
			if "xdrHttp" == topic || "xdrFile" == topic {
				if nil == startErr && nil == endErr {
					if startOffset > v.Engine {
						status[engine][topic] = Status{startOffset, startOffset, 0, startOffset, endOffset, v.Weight}
					} else {
						status[engine][topic] = Status{startOffset, v.Engine, 0, v.Engine, endOffset, v.Weight}
					}
					if v.Engine > endOffset {
						status[engine][topic] = Status{startOffset, endOffset, 0, endOffset, endOffset, v.Weight}
						Log.Warn("%s %d partition offset is set to last-offset as what you set is out of kafka current offset", topic, Partition)
					}
				} else {
					Log.Error("Unknow firstOffset or lastOffset, topic = %s, partition = %d", topic, Partition)
				}
			}
		}
	}
}

func Kafka() {
	defer func() {
		if err := recover(); nil != err {
			LogCrt("PANIC in Kafka(), %v", err)
		}
	}()

	InitConsumers(Partition)
	UpdateOffset()
}
