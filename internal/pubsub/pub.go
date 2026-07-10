package pubsub

import (
	"bytes"
	"context"
	"encoding/gob"
	"encoding/json"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	ampq "github.com/rabbitmq/amqp091-go"
)

func PublishJSON[T any](ch *ampq.Channel, exchange, key string, val T) error {
	buf, err := json.Marshal(val)
	if err != nil {
		return err
	}

	err = ch.PublishWithContext(context.Background(), exchange, key, false, false, ampq.Publishing{
		ContentType: "application/json",
		Body:        buf,
	})
	if err != nil {
		return err
	}

	return nil
}

func PublishGOB[T any](ch *ampq.Channel, exchange, key string, val T) error {
	var buff bytes.Buffer
	enc := gob.NewEncoder(&buff)
	err := enc.Encode(val)
	if err != nil {
		return err
	}

	return ch.PublishWithContext(
		context.Background(),
		exchange, key,
		false, false,
		ampq.Publishing{
			ContentType: "application/gob",
			Body:        buff.Bytes(),
		},
	)
}

func PublishGameLog(ch *ampq.Channel, log routing.GameLog) error {
	return PublishGOB(
		ch,
		routing.ExchangePerilTopic,
		routing.GenerateKey(routing.GameLogSlug, log.Username),
		log)
}
