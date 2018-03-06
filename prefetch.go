//prefetch.go

package agent_pkg

import (
	"sync"
)

type PrefetchMsg struct {
	Engine string
	Topic  string
	Count  int

	Shutdown bool
}

var PrefetchMsgSwitchMap = make(map[string]bool)
var PrefetchChMap = make(map[string]chan PrefetchMsg)

var MutexConsumer sync.Mutex

func InitPrefetchMsgSwitchMap() {
	for _, val := range status {
		for topic, _ := range val {
			PrefetchMsgSwitchMap[topic] = true
		}
	}
}

func ReadKafka(prefetchMsg PrefetchMsg, data *[][]byte) {
	defer func() {
		recover()
	}()

	MutexConsumer.Lock()

	for i := 0; i < prefetchMsg.Count; i++ {
		msg, err := consumers[prefetchMsg.Engine][prefetchMsg.Topic].Consume()
		if err != nil {
			Log.Info("no data in: %s", prefetchMsg.Topic)
		}
		*data = append(*data, msg.Value)
	}

	MutexConsumer.Unlock()
}

func Prefetch(prefetchCh chan PrefetchMsg) {
	defer func() {
		if err := recover(); nil != err {
			LogCrt("PANIC in Prefetch(), %v", err)
		}
	}()

	for {
		prefetchMsg := <-prefetchCh

		if prefetchMsg.Shutdown {
			break
		}

		var Data [][]byte
		ReadKafka(prefetchMsg, &Data)
		dataPtr := &Data

		res := PrefetchRes{Base{prefetchMsg.Engine, prefetchMsg.Topic}, dataPtr}

		PrefetchResCh <- res

	}
}

func InitPrefetch() {
	for _, val := range status {
		for topic, _ := range val {
			PrefetchChMap[topic] = make(chan PrefetchMsg, 100)
			go Prefetch(PrefetchChMap[topic])
		}
	}
}
