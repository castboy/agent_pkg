//distri.go

package pkg_wmg 

import (
    "fmt"
)

var successConsumeNum int = 0

func Distri (pType string, num int) {
    if "waf" == pType {
        times:= num / wafWeightTotal
        for topic, v := range WafBak {
            for j := 0; j < v.Weight * times; j++ {
                if WafBak[topic].StopConsume {
                    delete(WafBak, topic)
                    break    
                } else {
                    Consume(pType, topic)    
                }
            }    
        }    
       
        fmt.Println("last WafBak is:") 
        fmt.Println(WafBak) 

        for {
            if len(WafBak) != 0 {
                for topic, v := range WafBak {
                    for j := 0; j < v.Weight; j++ {
                        if WafBak[topic].StopConsume {
                            delete(WafBak, topic)
                            break    
                        } else {
                            Consume(pType, topic)    
                        }
                        if successConsumeNum != num {
                            Consume(pType, topic)    
                        } else {
                            fmt.Println("successConsumeNum == num")
                            break
                            break
                            break    
                        }
                        fmt.Println("3 layer")
                    }    
                    fmt.Println("2 layer")
                }    
                fmt.Println("1 layer")
                
            }
        } 
        fmt.Println(Waf) 
        fmt.Println(Vds) 
            
    } else {

    }

}

