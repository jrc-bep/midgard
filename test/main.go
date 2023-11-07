package main

import (
	"context"
	"flag"
	"log"
	"time"

	"github.com/adjackura/midgard"
	"google.golang.org/protobuf/types/known/anypb"

	acpb "github.com/adjackura/midgard/cloud.google.com/go/agentcommunication/apiv1/agentcommunicationpb"
)

var (
	endpoint = flag.String("endpoint", "", "endpoint override, don't set if not needed")
	channel  = flag.String("channel", "my-channel", "channel")
)

func main() {
	flag.Parse()

	ctx := context.Background()

	conn, err := midgard.CreateConnection(ctx, *channel, false, *endpoint)
	if err != nil {
		log.Fatal(err)
	}

	go func() {
		for {
			msg, err := conn.Receive()
			if err != nil {
				log.Println(err)
				return
			}
			log.Printf("Got message: %+v", msg)
		}
	}()

	if err := conn.SendMessage(&acpb.MessageBody{Body: &anypb.Any{Value: []byte("hello world")}}); err != nil {
		log.Fatal(err)
	}
	time.Sleep(5 * time.Second)
	if err := conn.SendMessage(&acpb.MessageBody{Body: &anypb.Any{Value: []byte("hello world")}}); err != nil {
		log.Fatal(err)
	}
	time.Sleep(60 * time.Second)

	conn.Close(nil)

	time.Sleep(1 * time.Second)
}
