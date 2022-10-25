package lib

import (
	"context"
	"fmt"

	"cloud.google.com/go/pubsub"
	log "github.com/sirupsen/logrus"
)

type PubSubClient struct {
	ServerClient     *pubsub.Client
	ProjectID        string
	DefaultTopicName string
	Topics           map[string]*pubsub.Topic
}

func GetPubSubClient(projectID string, topicOut string) *PubSubClient {
	client := PubSubClient{}

	client.Topics = map[string]*pubsub.Topic{}
	client.ProjectID = projectID
	client.DefaultTopicName = topicOut

	client.connect()

	return &client
}

func (cli *PubSubClient) connect() {
	log.Info("Connecting to pub sub...", cli.ProjectID, cli.DefaultTopicName)

	ctx := context.Background()
	var err error
	cli.ServerClient, err = pubsub.NewClient(ctx, cli.ProjectID)
	if err != nil {
		log.Error("pubsub.NewClient: ", err)
		panic(err)
	}

	cli.Topics[cli.DefaultTopicName] = cli.ServerClient.Topic(cli.DefaultTopicName)

	log.Info("Pub sub ok.")

}

func (cli *PubSubClient) AddTopic(topicName string) error {
	topic := cli.ServerClient.Topic(topicName)
	topicExists, err := topic.Exists(context.Background())
	if err != nil {
		return fmt.Errorf("failed to check if topic exists: %v", err)
	}
	if !topicExists {
		topic, err = cli.ServerClient.CreateTopic(context.Background(), topicName)
		if err != nil {
			return fmt.Errorf("failed to create topic: %v", err)
		}
	}
	cli.Topics[topicName] = topic
	return nil
}

func (cli *PubSubClient) RemoveTopic(topicName string) {
	cli.Topics[topicName].Stop()
	delete(cli.Topics, topicName)
}

func (cli *PubSubClient) Subscribe(topicName string, subscriberName string, sync bool) (*pubsub.Subscription, error) {
	ctx := context.Background()
	if _, found := cli.Topics[topicName]; !found {
		err := cli.AddTopic(topicName)
		if err != nil {
			return nil, fmt.Errorf("failed to add topic: %v", err)
		}
	}

	topic := cli.Topics[topicName]

	sub := cli.ServerClient.Subscription(subscriberName)
	subExists, err := sub.Exists(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to check if sub exists: %v", err)
	}
	if !subExists {
		sub, err = cli.ServerClient.CreateSubscription(ctx, subscriberName,
			pubsub.SubscriptionConfig{Topic: topic})
	}

	sub.ReceiveSettings.Synchronous = sync // sincrono pra evitar concorrencia
	sub.ReceiveSettings.MaxOutstandingMessages = 1

	return sub, nil

}

func (cli *PubSubClient) Publish(msgType string, msg string) error {
	attributes := map[string]string{}
	return cli.PublishWithAttribs(msgType, msg, attributes)
}

func (cli *PubSubClient) PublishWithAttribs(msgType string, msg string, attributes map[string]string) error {
	attributes["type"] = msgType
	return cli.PublishInTopicWithAttribs(cli.DefaultTopicName, msg, attributes)
}

func (cli *PubSubClient) PublishInTopicWithAttribs(topic string, msg string, attributes map[string]string) error {
	ctx := context.Background()
	if _, found := cli.Topics[topic]; !found {
		cli.AddTopic(topic)
	}
	result := cli.Topics[topic].Publish(ctx, &pubsub.Message{
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
