package pubsub

import (
	"encoding/json"
	"fmt"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	ampq "github.com/rabbitmq/amqp091-go"
)

type SimpleQueueType string
type AckType int64

const (
	Ack AckType = iota
	NackReque
	NackDiscard
)

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
	queue, err := ch.QueueDeclare(queueName, durable, autoDelete, exlusive, false,
		ampq.Table{
			"x-dead-letter-exchange": routing.ExchnagePerilDLX,
		})
	if err != nil {
		return nil, ampq.Queue{}, fmt.Errorf("Unable to bind queue: %s", err)
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
	handler func(T) AckType,
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
				fmt.Printf("an error happen while parsing the message body queue: %s, exchange: %s \n", queueName, exchange)
				continue
			}

			switch handler(messageBody) {
			case Ack:
				// fmt.Println("message acknowledged")
				message.Ack(true)
			case NackReque:
				// fmt.Println("message acknowledgement failed requeue")
				message.Nack(false, true)
			case NackDiscard:
				// fmt.Println("message acknowledgement failed dead letter queue")
				message.Nack(false, false)
			default:
				fmt.Println("Unkown acktype")
			}
		}
	}()

	return nil
}
