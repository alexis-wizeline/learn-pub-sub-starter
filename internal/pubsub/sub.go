package pubsub

import (
	"encoding/json"
	"fmt"

	ampq "github.com/rabbitmq/amqp091-go"
)

type SimpleQueueType string

const (
	DurableQueue   SimpleQueueType = "durable"
	TransientQueue SimpleQueueType = "transient"
)

func DeclareAndBind(
	conn *ampq.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType,
) (*ampq.Channel, ampq.Queue, error) {
	ch, err := conn.Channel()
	if err != nil {
		return nil, ampq.Queue{}, fmt.Errorf("unable to create channel from connection: %s", err)
	}

	durable, autoDelete, exlusive := true, false, false
	if queueType == TransientQueue {
		durable, autoDelete, exlusive = false, true, true
	}
	queue, err := ch.QueueDeclare(queueName, durable, autoDelete, exlusive, false, nil)
	if err != nil {
		return nil, ampq.Queue{}, fmt.Errorf("Unable to decalre queue: %s", err)
	}

	err = ch.QueueBind(queueName, key, exchange, false, nil)
	if err != nil {
		return nil, ampq.Queue{}, fmt.Errorf("Unable to bind queue: %s", err)
	}

	return ch, queue, nil
}

func SubscribeJSON[T any](
	conn *ampq.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType,
	handler func(T),
) error {
	ch, queue, err := DeclareAndBind(
		conn,
		exchange,
		queueName,
		key,
		queueType)
	if err != nil {
		return err
	}

	delivery, err := ch.Consume(queue.Name,
		"",
		false,
		false,
		false,
		false,
		nil)

	go func() {
		for message := range delivery {
			var messageBody T
			err := json.Unmarshal(message.Body, &messageBody)
			if err != nil {
				fmt.Printf("an errro happen while parsing the message body queue: %s, exchange: %s \n", queueName, exchange)
				continue
			}

			handler(messageBody)
			message.Ack(true)
		}
	}()

	return nil
}
