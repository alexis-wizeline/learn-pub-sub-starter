package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	connectionStrig := "amqp://guest:guest@localhost:5672/"
	rabbit, err := amqp.Dial(connectionStrig)
	if err != nil {
		fmt.Printf("connection to rabbitMQ fail: %v", err)
		os.Exit(1)
	}

	ch, err := rabbit.Channel()
	if err != nil {
		fmt.Println("unable to init a new channel, ", err.Error())
		os.Exit(1)
	}
	defer ch.Close()
	defer rabbit.Close()

	err = subscribeLogQueue(rabbit)
	if err != nil {
		fmt.Println("unable to subscribe the logs queue, ", err)
		os.Exit(1)
	}

	fmt.Println("Starting Peril server...")
	fmt.Println("Connected to Rabbit succesfully", rabbit.RemoteAddr())
	gamelogic.PrintServerHelp()
	for {
		input := gamelogic.GetInput()
		if len(input) == 0 {
			<-ctx.Done()
			fmt.Println("\nShutting down server...")
			return
		}
		var err error
		switch input[0] {
		case "pause":
			fmt.Println("pausing the game......")
			err = publishGameState(ch, true)
		case "resume":
			fmt.Println("resuming the game....")
			err = publishGameState(ch, false)
		case "quit":
			fmt.Println("You are shutting down the game!")
			cancel()
			return
		case "help":
			gamelogic.PrintServerHelp()
		default:
			fmt.Println("Unknow command, I probably don't talk that language")
		}

		if err != nil {
			fmt.Printf("an erro ocurr for the command: %s, err: %s", input[0], err)
		}
		err = nil
	}

}

func publishGameState(ch *amqp.Channel, inPuase bool) error {
	return pubsub.PublishJSON(ch, routing.ExchangePerilDirect, routing.PauseKey, routing.PlayingState{
		IsPaused: inPuase,
	})
}

func subscribeLogQueue(conn *amqp.Connection) error {
	return pubsub.SubscribeGob(conn,
		routing.ExchangePerilTopic,
		routing.GameLogSlug,
		routing.GenerateKey(routing.GameLogSlug),
		pubsub.DurableQueue,
		logsHandler,
	)
}

func logsHandler(gl routing.GameLog) pubsub.AckType {
	defer fmt.Print("> ")
	err := gamelogic.WriteLog(gl)
	if err != nil {
		return pubsub.NackReque
	}
	return pubsub.Ack
}
