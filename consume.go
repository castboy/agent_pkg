//consume.go

package pkg_wmg

import (
    //"net/http"
    "log"
    //"io"
    //"fmt"
    //"errors"
)
//func Consume (pType string, topic string)  {
func Consume (pType string, topic string) ([]byte, error) {
    defer func() {
        if r := recover(); r != nil {
            log.Printf("consume err: %v", r)    
        }    
    }()

    //if "waf" == pType {
        msg, err := wafConsumers[topic].Consume()
        if err != nil {
            WafBak[topic] = Action{WafBak[topic].Weight, true}
            panicNum++
            panic("no data in: " + topic)
        } else {
            Waf[topic] = Partition{Waf[topic].First, msg.Offset+1, Waf[topic].Last, Waf[topic].Weight, Waf[topic].Stop}   
            successConsumeNum++
            //fmt.Println(Waf[topic])
        }
        //log.Printf("message %d: %s", msg.Offset, msg.Value)
        return msg.Value, err
    //}
            
}



