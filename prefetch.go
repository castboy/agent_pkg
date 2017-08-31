//prefetch.go

package agent_pkg

import (
	"fmt"
)

type PrefetchMsg struct {
	Engine string
	Topic  string
	Count  int

	Shutdown bool
}

var PrefetchMsgSwitchMap = make(map[string]bool)
var PrefetchChMap = make(map[string]chan PrefetchMsg)

func InitPrefetchMsgSwitchMap() {
	for _, val := range status {
		for topic, _ := range val {
			PrefetchMsgSwitchMap[topic] = true
		}
	}
}

func ReadKafka(prefetchMsg PrefetchMsg, data *[][]byte) {
	defer func() {
		if r := recover(); r != nil {
			fmt.Printf("consume err: %v", r)
		}
	}()

	for i := 0; i < prefetchMsg.Count; i++ {
		msg, err := consumers[prefetchMsg.Engine][prefetchMsg.Topic].Consume()
		if err != nil {
			fmt.Println("no data in: " + prefetchMsg.Topic)
		}
		*data = append(*data, msg.Value)
	}
}

func Prefetch(prefetchCh chan PrefetchMsg) {
	for {
		prefetchMsg := <-prefetchCh
		fmt.Println("received PrefetchMsg:", prefetchMsg)

		if prefetchMsg.Shutdown {
			break
		}

		var Data [][]byte
		ReadKafka(prefetchMsg, &Data)
		dataPtr := &Data

		res := PrefetchRes{Base{prefetchMsg.Engine, prefetchMsg.Topic}, dataPtr}

		fmt.Println("Prefetch res:", res)

		PrefetchResCh <- res

	}

	fmt.Println("break out prefetch routine")
}

func InitPrefetch() {
	for _, val := range status {
		for topic, _ := range val {
			PrefetchChMap[topic] = make(chan PrefetchMsg, 100)
			go Prefetch(PrefetchChMap[topic])
		}
	}
}
