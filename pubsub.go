package lib

import (
	"context"
	"fmt"

	"cloud.google.com/go/pubsub"
	log "github.com/sirupsen/logrus"
)

type PubSubClient struct {
	ServerClient *pubsub.Client
	Topic        *pubsub.Topic
	ProjectID    string
	TopicID      string
}

func GetPubSubClient(projectID string, topicOut string) *PubSubClient {
	client := PubSubClient{}

	client.ProjectID = projectID
	client.TopicID = topicOut

	client.connect()

	return &client
}

func (cli *PubSubClient) connect() {
	log.Info("Connecting to pub sub...", cli.ProjectID, cli.TopicID)

	ctx := context.Background()
	var err error
	cli.ServerClient, err = pubsub.NewClient(ctx, cli.ProjectID)
	if err != nil {
		log.Error("pubsub.NewClient: ", err)
		panic(err)
	}

	cli.Topic = cli.ServerClient.Topic(cli.TopicID)

	log.Info("Pub sub ok.")

}

func (cli *PubSubClient) Publish(msgType string, msg string) error {
	attributes := map[string]string{}
	return cli.PublishWithAttribs(msgType, msg, attributes)
}

func (cli *PubSubClient) PublishWithAttribs(msgType string, msg string, attributes map[string]string) error {

	ctx := context.Background()
	attributes["type"] = msgType

	result := cli.Topic.Publish(ctx, &pubsub.Message{
		Data:       []byte(msg),
		Attributes: attributes,
	})

	// Block until the result is returned and a server-generated
	// ID is returned for the published message.
	id, err := result.Get(ctx)
	if err != nil {
		return fmt.Errorf("get: %v", err)
	}
	log.Debug("Published a message; msg ID:", id)
	return nil
}

func (cli *PubSubClient) Close() {
	cli.ServerClient.Close()
}
