package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"time"

	cm "cloud.google.com/go/compute/metadata"
	agentcommunication "github.com/adjackura/midgard/internal/cloud.google.com/go/agentcommunication/apiv1"
	"github.com/google/uuid"
	"google.golang.org/api/option"
	"google.golang.org/genproto/googleapis/rpc/code"
	"google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/anypb"

	agentcommunicationpb "github.com/adjackura/midgard/internal/google.golang.org/genproto/googleapis/cloud/agentcommunication/v1"
)

var path = flag.String("path", "", "")
var chunkKBs = flag.Int("chunk_kbs", 256, "")
var latency = flag.Bool("latency", false, "")

func copy(stream agentcommunicationpb.AgentCommunication_StreamAgentMessagesClient) error {
	f, err := os.Open(*path)
	if err != nil {
		return err
	}
	start := time.Now()
	var size int

	chunk := make([]byte, 1024*(*chunkKBs))
	for i := 0; ; i++ {
		n, err := f.Read(chunk)
		if err != nil && err != io.EOF {
			return err
		}
		size += n
		msgID := uuid.NewString()
		req := &agentcommunicationpb.StreamAgentMessagesRequest{
			MessageId: msgID,
			Type: &agentcommunicationpb.StreamAgentMessagesRequest_MessageBody{
				MessageBody: &agentcommunicationpb.MessageBody{
					Labels: map[string]string{"copy": fmt.Sprintf("%d", i)},
					Body:   &anypb.Any{Value: chunk[:n]},
				},
			},
		}
	send:
		for {
			log.Printf("Send Message with %d bytes\n", n)
			if err := stream.Send(req); err != nil {
				return err
			}

			// Wait for the response.
			for {
				resp, err := stream.Recv()
				if err != nil {
					log.Fatalln("Recv error:", err)
				}
				if resp.GetMessageResponse() != nil && resp.GetMessageId() == msgID {
					if resp.GetMessageResponse().GetStatus().GetCode() == int32(code.Code_OK) {
						break send
					}
					log.Println(resp.GetMessageResponse().GetStatus())
					if resp.GetMessageResponse().GetStatus().GetCode() == int32(code.Code_RESOURCE_EXHAUSTED) {
						//time.Sleep(500 * time.Millisecond)
						continue send
					}
					return fmt.Errorf("Unexpected status: %v", resp.GetMessageResponse().GetStatus())
				}
			}
		}

		if n != len(chunk) {
			req := &agentcommunicationpb.StreamAgentMessagesRequest{
				MessageId: uuid.NewString(),
				Type: &agentcommunicationpb.StreamAgentMessagesRequest_MessageBody{
					MessageBody: &agentcommunicationpb.MessageBody{
						Labels: map[string]string{"copy": fmt.Sprintf("%d", i+1)},
						Body:   &anypb.Any{Value: []byte("end")},
					},
				},
			}
			//time.Sleep(1 * time.Second)
			log.Println("Send Message 'end'")
			if err := stream.Send(req); err != nil {
				return err
			}

			tt := time.Since(start)
			log.Printf("Sent %dKiB in %s, %d KiB/s", (size / 1024), tt, (size/1024)/int(tt.Seconds()))
			return nil
		}
	}
}

func main() {
	flag.Parse()
	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds | log.Lshortfile)

	ctx := context.Background()
	opts := []option.ClientOption{
		option.WithoutAuthentication(), // Do not use oauth.
		option.WithGRPCDialOption(grpc.WithTransportCredentials(credentials.NewTLS(nil))), // Because we disabled Auth we need to specifically enable TLS.
		option.WithEndpoint("us-central1-agentcommunication.googleapis.com:443"),
	}

	token, err := cm.Get("instance/service-accounts/default/identity?audience=agentcommunication.googleapis.com&format=full")
	if err != nil {
		log.Fatalf("error getting token from metadata: %v", err)
	}
	zone, _ := cm.Zone()
	projectNum, _ := cm.NumericProjectID()
	instanceID, _ := cm.InstanceID()

	resourceID := fmt.Sprintf("projects/%s/zones/%s/instances/%s", projectNum, zone, instanceID)
	channelID := "my-channel"
	ctx = metadata.NewOutgoingContext(ctx, metadata.New(map[string]string{
		"authentication":                  "Bearer " + token,
		"agent-communication-resource-id": resourceID,
		"agent-communication-channel-id":  channelID,
	}))

	log.Println("resourceID:", resourceID)
	log.Println("channelID:", channelID)

	c, err := agentcommunication.NewClient(ctx, opts...)
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()

	stream, err := c.StreamAgentMessages(ctx)
	if err != nil {
		log.Fatalln("StreamAgentMessages error:", err)
	}
	regReq := &agentcommunicationpb.StreamAgentMessagesRequest{
		MessageId: "register",
		Type: &agentcommunicationpb.StreamAgentMessagesRequest_RegisterConnection{
			RegisterConnection: &agentcommunicationpb.RegisterConnection{ResourceId: resourceID, ChannelId: channelID}}}
	if err := stream.Send(regReq); err != nil {
		log.Fatal(err)
	}
	for {
		resp, err := stream.Recv()
		if err != nil {
			log.Fatalln("Recv error:", err)
		}
		log.Printf("Got Message: %+v", resp)
		if resp.GetMessageResponse() != nil {
			continue
		}

		if *latency {
			sent := string(resp.GetMessageBody().Body.Value)
			st, err := time.Parse(time.RFC3339Nano, sent)
			if err != nil {
				log.Fatal(err)
			}
			fmt.Println(time.Since(st))
		}

		req := &agentcommunicationpb.StreamAgentMessagesRequest{
			MessageId: resp.MessageId,
			Type: &agentcommunicationpb.StreamAgentMessagesRequest_MessageResponse{
				MessageResponse: &agentcommunicationpb.MessageResponse{Status: &status.Status{Code: 0}}}}
		log.Printf("Send Message: %+v", req)
		if err := stream.Send(req); err != nil {
			log.Fatal(err)
		}

		if *latency {
			req := &agentcommunicationpb.StreamAgentMessagesRequest{
				MessageId: uuid.NewString(),
				Type: &agentcommunicationpb.StreamAgentMessagesRequest_MessageBody{
					MessageBody: &agentcommunicationpb.MessageBody{
						Labels: resp.GetMessageBody().GetLabels(),
						Body:   &anypb.Any{Value: []byte(time.Now().Format(time.RFC3339Nano))},
					},
				},
			}
			if err := stream.Send(req); err != nil {
				log.Fatal(err)
			}

			continue
		}

		if *path != "" {
			if err := copy(stream); err != nil {
				log.Fatal(err)
			}
		}
	}
}
