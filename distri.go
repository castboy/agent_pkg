//distri.go

package pkg_wmg 

import (
    "fmt"
    "encoding/json"
)

var successConsumeNum int = 0
var panicNum int = 0
var data interface{}
type Res struct {
    Code int
    Data interface{}    
    Num int
}

func Distri (num int) string {
    Len := len(*PtrBak)
    dataSlice := make([]interface{}, 0) 
    for {
        for topic, v := range *PtrBak {
            for j := 0; j < v.Weight; j++ {
                if (*PtrBak)[topic].StopConsume {
                    delete(*PtrBak, topic)
                    break    
                } else {
                    byte, err := Consume(topic)
                    if err != nil {
                    } else {
                        err = json.Unmarshal(byte, &data)    
                        if err != nil {
                            fmt.Println("Unmarshal Error")    
                        }
                        dataSlice = append(dataSlice, data)
                    }
                }
                if (successConsumeNum == num || panicNum == Len) {
                    break
                }
            }    
            if (successConsumeNum == num || panicNum == Len) {
                break
            }
        }    
        if (successConsumeNum == num || panicNum == Len) {
            break
        }
    }
   
    res := Res {
        Code: 10000,
        Data: dataSlice,
        Num: successConsumeNum,
    }

    if 0 == successConsumeNum {
        res = Res {
            Code: 10000,
            Data: nil,
            Num: successConsumeNum,
        }
    }

    byte, _ := json.Marshal(res)
    jsonStr := string(byte)

    fmt.Println(*Ptr) 
        
    for topic, v := range *Ptr {
        (*PtrBak)[topic] = Action{v.Weight, false}   
    }
    panicNum = 0
    successConsumeNum = 0

    return jsonStr
}

