//distri.go

package pkg_wmg 

import (
    "fmt"
)

var successConsumeNum int = 0

func Distri (pType string, num int) {
    if "waf" == pType {
        times:= num / wafWeightTotal
        for topic, v := range Waf {
            for j := 0; j < v.Weight * times; j++ {
                Consume(pType, topic)    
            }    
        }    
        
        compensate := num - successConsumeNum 
        for n := 0; n < compensate; n++ {
            Consume(pType, "waf")    
        }
        
        fmt.Println(Waf) 
        fmt.Println(Vds) 
            
    } else {

    }

}

