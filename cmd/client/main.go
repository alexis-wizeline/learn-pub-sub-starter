package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"

	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	queuNameTemplate = "%s.%s"
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

	user, err := gamelogic.ClientWelcome()
	if err != nil {
		fmt.Println("Unable to create user: ", err)
		os.Exit(1)
	}

	ch, queue, err := pubsub.DeclareAndBind(
		rabbit,
		routing.ExchangePerilDirect,
		fmt.Sprintf(queuNameTemplate, routing.PauseKey, user),
		routing.PauseKey,
		pubsub.TransientQueue)
	if err != nil {
		fmt.Println("Unable to bind queue for peril_direct ", err)
		os.Exit(1)
	}

	_ = queue
	defer ch.Close()
	defer rabbit.Close()
	state := gamelogic.NewGameState(user)
	for {
		input := gamelogic.GetInput()
		var err error
		switch input[0] {
		case "spawn":
			err = state.CommandSpawn(input)
		case "move":
			_, err = state.CommandMove(input)
			if err == nil {
				fmt.Println("Move has been succesfully complete")
			}
		case "status":
			state.CommandStatus()
		case "spam":
			fmt.Println("Spamming not allowed yet!")
		case "help":
			gamelogic.PrintClientHelp()
		case "quit":
			gamelogic.PrintQuit()
			cancel()
			return
		default:
			err = errors.New("I', not able to understand you, yu are probably yaping")
		}

		if err != nil {
			fmt.Println("An erro ocurr:", err)
		}
		err = nil
	}

}
