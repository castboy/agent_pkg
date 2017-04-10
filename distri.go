//distri.go

package pkg_wmg 

import (
    "fmt"
//    "encoding/json"
)

var successConsumeNum int = 0
var panicNum int = 0
//var data interface{}
//type Res struct {
//    Code int
//    Data interface{}    
//    Num int
//}

func Distri (pType string, num int) {
    wafLen := len(WafBak)
//    dataSlice := make([]interface{}, 0) 
    if "waf" == pType {
        for {
            for topic, v := range WafBak {
                for j := 0; j < v.Weight; j++ {
                    if WafBak[topic].StopConsume {
                        delete(WafBak, topic)
                        break    
                    } else {
                        //byte, err := Consume(pType, topic)
                        //if err != nil {
                        //    err = json.Unmarshal(byte, &data)    
                        //    if err != nil {
                        //        
                        //    }
                        //    dataSlice = append(dataSlice, data)
                        //}
                        Consume(pType, topic)
                    }
                    fmt.Println(WafBak)
                    if (successConsumeNum == num || panicNum == wafLen) {
                        break
                    }
                }    
                if (successConsumeNum == num || panicNum == wafLen) {
                    break
                }
            }    
            if (successConsumeNum == num || panicNum == wafLen) {
                break
            }
        }
       
        //res := Res {
        //    Code: 10000,
        //    Data: dataSlice,
        //    Num: successConsumeNum,
        //}
        //byte, _ := json.Marshal(res)
        //fmt.Println(string(byte))

        fmt.Println(Waf) 
        fmt.Println(Vds) 
        fmt.Println(successConsumeNum)
            
        for topic, v := range Waf {
            WafBak[topic] = Action{v.Weight, false}   
        }
        fmt.Println(WafBak)
        panicNum = 0
        successConsumeNum = 0

    } else {

    }

}

