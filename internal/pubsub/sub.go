package pubsub

import (
	"bytes"
	"encoding/gob"
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
	return subscriber[T](
		conn,
		exchange,
		queueName,
		key,
		queueType,
		handler,
		jsonUnmarsahller[T])
}

func SubscribeGob[T any](
	conn *ampq.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType,
	handler func(T) AckType,
) error {
	return subscriber[T](
		conn,
		exchange,
		queueName,
		key,
		queueType,
		handler,
		gobUnmarshaller[T])
}

func subscriber[T any](
	conn *ampq.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType,
	handler func(T) AckType,
	unmarshaller func([]byte) (T, error),
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
	if err = ch.Qos(10, 0, false); err != nil {
		return fmt.Errorf("unable to set QoS on channel: %s", err)
	}

	delivery, err := ch.Consume(queue.Name,
		"",
		false,
		false,
		false,
		false,
		nil)
	if err != nil {
		return fmt.Errorf("unable to start consuming from queue: %s", err)
	}

	go func() {
		for letter := range delivery {
			message, err := unmarshaller(letter.Body)
			if err != nil {
				fmt.Printf("an error happen while parsing the message body queue: %s, exchange: %s \n", queueName, exchange)
				continue
			}

			switch handler(message) {
			case Ack:
				// fmt.Println("message acknowledged")
				letter.Ack(true)
			case NackReque:
				// fmt.Println("message acknowledgement failed requeue")
				letter.Nack(false, true)
			case NackDiscard:
				// fmt.Println("message acknowledgement failed dead letter queue")
				letter.Nack(false, false)
			default:
				fmt.Println("Unkown acktype")
			}
		}
	}()

	return nil
}

func jsonUnmarsahller[T any](b []byte) (T, error) {
	var data T
	err := json.Unmarshal(b, &data)
	if err != nil {
		return data, err
	}

	return data, nil
}

func gobUnmarshaller[T any](b []byte) (T, error) {
	buffer := bytes.NewBuffer(b)
	decoder := gob.NewDecoder(buffer)
	var data T
	err := decoder.Decode(&data)
	if err != nil {
		return data, err
	}
	return data, nil
}
