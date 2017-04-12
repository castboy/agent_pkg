//consume.go

package pkg_wmg

import (
    //"net/http"
    "log"
    //"io"
    //"fmt"
    //"errors"
)
func Consume (topic string) ([]byte, error) {
    defer func() {
        if r := recover(); r != nil {
            log.Printf("consume err: %v", r)    
        }    
    }()

    msg, err := (*consumerPtr)[topic].Consume()
    if err != nil {
        (*PtrBak)[topic] = Action{(*PtrBak)[topic].Weight, true}
        panicNum++
        panic("no data in: " + topic)
    } else {
        (*Ptr)[topic] = Partition{(*Ptr)[topic].First, msg.Offset+1, (*Ptr)[topic].Last, (*Ptr)[topic].Weight, (*Ptr)[topic].Stop}   
        successConsumeNum++
    }
    //log.Printf("message %d: %s", msg.Offset, msg.Value)
    return msg.Value, err
            
}



