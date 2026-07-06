package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
)

func main() {
	_, cancel := signal.NotifyContext(context.Background(), os.Interrupt, os.Kill)
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

	logsCh, logsQueu, err := pubsub.DeclareAndBind(
		rabbit,
		routing.ExchangePerilTopic,
		routing.GameLogSlug,
		fmt.Sprintf("%s.*", routing.GameLogSlug),
		pubsub.DurableQueue)
	if err != nil {
		fmt.Println("unable to bind queue for ", routing.ExchangePerilTopic, err)
		os.Exit(1)
	}

	_ = logsQueu
	defer logsCh.Close()
	defer rabbit.Close()

	fmt.Println("Starting Peril server...")
	fmt.Println("Connected to Rabbit succesfully", rabbit.RemoteAddr())
	gamelogic.PrintServerHelp()
	for {
		input := gamelogic.GetInput()
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
