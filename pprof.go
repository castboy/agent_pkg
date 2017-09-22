package agent_pkg

import (
	"flag"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	//"preprocess/modules/mlog"
	"runtime/pprof"
)

var cpuprofile = flag.String("cpuprofile", "", "write cpu profile `file`")
var memprofile = flag.String("memprofile", "", "write memory profile to `file`")

func StartPProf() {

	//go tool pprof preprocess cpu.prof
	flag.Parse()

	go func() {
		log.Println(http.ListenAndServe(":6060", nil))
	}()

	if *cpuprofile != "" {
		log.Print("start cpuprofile")
		f, err := os.Create(*cpuprofile)
		if err != nil {
			log.Fatal("could not create CPU profile: ", err)
		}
		if err := pprof.StartCPUProfile(f); err != nil {
			log.Fatal("could not start CPU profile: ", err)
		}
	}
}

func Stoppprof() {
	if *cpuprofile != "" {
		pprof.StopCPUProfile()
	}
	if *memprofile != "" {
		log.Print("start memprofile")
		f, err := os.Create(*memprofile)
		if err != nil {
			log.Fatal("could not create memory profile: ", err)
		}
		//runtime.GC() // get up-to-date statistics
		if err := pprof.WriteHeapProfile(f); err != nil {
			log.Fatal("could not write memory profile: ", err)
		}
		f.Close()
	}
}
