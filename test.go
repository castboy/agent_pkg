package agent_pkg

var TestCh = make(chan int, 10000)

func ChTest() {
	for {
		TestCh <- 1
	}
}
