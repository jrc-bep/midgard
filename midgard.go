package midgard

import (
	"context"
	"fmt"
	"io"
	"log"
	"sync"
	"time"

	cm "cloud.google.com/go/compute/metadata"
	agentcommunication "github.com/adjackura/midgard/cloud.google.com/go/agentcommunication/apiv1"
	"github.com/google/uuid"
	"google.golang.org/api/option"
	"google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"

	acpb "github.com/adjackura/midgard/cloud.google.com/go/agentcommunication/apiv1/agentcommunicationpb"
)

// Stream is an agent message stream.
type Connection struct {
	context    context.Context
	client     *agentcommunication.Client
	stream     acpb.AgentCommunication_StreamAgentMessagesClient
	closed     chan struct{}
	closeErr   error
	resourceID string
	channelID  string

	messages     chan *acpb.MessageBody
	responseSubs map[string]chan struct{}
	responseMx   sync.Mutex
}

// CloseSend on the underlying stream.
func (c *Connection) Close(err error) error {
	select {
	case <-c.closed:
		return nil
	default:
		close(c.closed)
		c.closeErr = err
	}
	if err := c.stream.CloseSend(); err != nil {
		return err
	}
	return c.client.Close()
}

func (c *Connection) waitForResponse(key string, channel chan struct{}) error {
	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()
	select {
	case <-channel:
	case <-timer.C:
		return fmt.Errorf("timed out waiting for response, MessageID: %q", key)
	case <-c.closed:
		return fmt.Errorf("connection closed with err: %v", c.closeErr)
	}
	c.responseMx.Lock()
	delete(c.responseSubs, key)
	c.responseMx.Unlock()
	return nil
}

func (c *Connection) sendWithResp(req *acpb.StreamAgentMessagesRequest) error {
	channel := make(chan struct{})

	c.responseMx.Lock()
	c.responseSubs[req.GetMessageId()] = channel
	c.responseMx.Unlock()

	select {
	case <-c.closed:
		return fmt.Errorf("connection closed with err: %v", c.closeErr)
	default:
	}

	log.Printf("Sending message %+v", req)
	if err := c.stream.Send(req); err != nil {
		c.Close(err)
		return fmt.Errorf("error sending message: %v", err)
	}

	return c.waitForResponse(req.GetMessageId(), channel)
}

// SendMessage sends a message to the client.
func (c *Connection) SendMessage(msg *acpb.MessageBody) error {
	req := &acpb.StreamAgentMessagesRequest{
		MessageId: uuid.New().String(),
		Type:      &acpb.StreamAgentMessagesRequest_MessageBody{MessageBody: msg},
	}

	return c.sendWithResp(req)
}

// Receive messages.
func (c *Connection) Receive() (*acpb.MessageBody, error) {
	select {
	case msg := <-c.messages:
		return msg, nil
	case <-c.closed:
		return nil, fmt.Errorf("connection closed with err: %v", c.closeErr)
	}
}

// recv keeps receiving and acknowledging new messages.
func (c *Connection) recv() {
	for {
		resp, err := c.stream.Recv()
		if err != nil {
			if err != io.EOF {
				c.Close(err)
			}
			select {
			case <-c.closed:
				return
			default:
			}
			if err := c.createStream(); err != nil {
				c.Close(err)
				return
			}
		}
		switch resp.GetType().(type) {
		case *acpb.StreamAgentMessagesResponse_MessageBody:
			c.messages <- resp.GetMessageBody()
			if err := c.acknowledgeMessage(resp.GetMessageId()); err != nil {
				c.Close(err)
				return
			}
		case *acpb.StreamAgentMessagesResponse_MessageResponse:
			c.responseMx.Lock()
			for _, sub := range c.responseSubs {
				select {
				case sub <- struct{}{}:
				default:
				}
			}
			c.responseMx.Unlock()
		}
	}
}

func (c *Connection) acknowledgeMessage(messageID string) error {
	ackReq := &acpb.StreamAgentMessagesRequest{
		MessageId: messageID,
		Type:      &acpb.StreamAgentMessagesRequest_MessageResponse{MessageResponse: &acpb.MessageResponse{Status: &status.Status{}}},
	}
	select {
	case <-c.closed:
		return fmt.Errorf("connection closed with err: %v", c.closeErr)
	default:
		return c.stream.Send(ackReq)
	}
}

func (c *Connection) createStream() error {
	token, err := cm.Get("instance/service-accounts/default/identity?audience=agentcommunication.googleapis.com&format=full")
	if err != nil {
		return fmt.Errorf("error getting instance token: %v", err)
	}

	ctx := metadata.NewOutgoingContext(c.context, metadata.New(map[string]string{
		"authentication":                  "Bearer " + token,
		"agent-communication-resource-id": c.resourceID,
		"agent-communication-channel-id":  c.channelID,
	}))

	log.Printf("Using ResourceID %q", c.resourceID)
	log.Printf("Using ChannelID %q", c.channelID)

	c.stream, err = c.client.StreamAgentMessages(ctx)
	if err != nil {
		return fmt.Errorf("error sending register: %v", err)
	}

	go c.recv()

	req := &acpb.StreamAgentMessagesRequest{
		MessageId: uuid.New().String(),
		Type: &acpb.StreamAgentMessagesRequest_RegisterConnection{
			RegisterConnection: &acpb.RegisterConnection{ResourceId: c.resourceID, ChannelId: c.channelID}}}

	return c.sendWithResp(req)
}

func CreateConnection(ctx context.Context, channelID string, regional bool, endpointOverride string) (*Connection, error) {
	zone, err := cm.Zone()
	if err != nil {
		return nil, err
	}
	projectNum, err := cm.NumericProjectID()
	if err != nil {
		return nil, err
	}
	instanceID, err := cm.InstanceID()
	if err != nil {
		return nil, err
	}
	resourceID := fmt.Sprintf("projects/%s/zones/%s/instances/%s", projectNum, zone, instanceID)

	location := zone
	if regional {
		location = location[:len(location)-2]
	}

	endpoint := fmt.Sprintf("%s-agentcommunication.googleapis.com:443", location)
	if endpointOverride != "" {
		endpoint = endpointOverride
	}

	log.Printf("Using endpoint %q", endpoint)

	opts := []option.ClientOption{
		option.WithoutAuthentication(), // Do not use oauth.
		option.WithGRPCDialOption(grpc.WithTransportCredentials(credentials.NewTLS(nil))), // Because we disabled Auth we need to specifically enable TLS.
		option.WithEndpoint(endpoint),
	}

	c, err := agentcommunication.NewClient(ctx, opts...)
	if err != nil {
		log.Fatal(err)
	}

	conn := &Connection{
		context:      ctx,
		client:       c,
		channelID:    channelID,
		resourceID:   resourceID,
		closed:       make(chan struct{}),
		messages:     make(chan *acpb.MessageBody, 5),
		responseSubs: make(map[string]chan struct{}),
	}

	if err := conn.createStream(); err != nil {
		return nil, err
	}

	log.Printf("Streaming connection established.")

	return conn, nil
}
