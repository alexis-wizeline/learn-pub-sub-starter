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
	ch, err := rabbit.Channel()
	if err != nil {
		fmt.Println("Unable to create a channel from connection", err)
		os.Exit(1)
	}
	defer ch.Close()

	user, err := gamelogic.ClientWelcome()
	if err != nil {
		fmt.Println("Unable to create user: ", err)
		os.Exit(1)
	}
	state := gamelogic.NewGameState(user)

	err = pubsub.SubscribeJSON(
		rabbit,
		routing.ExchangePerilDirect,
		fmt.Sprintf(queuNameTemplate, routing.PauseKey, user),
		routing.PauseKey,
		pubsub.TransientQueue,
		handlerPause(state),
	)
	if err != nil {
		fmt.Println("Unable to subscribe to queue for pause", err)
		os.Exit(1)
	}

	err = pubsub.SubscribeJSON(
		rabbit,
		routing.ExchangePerilTopic,
		fmt.Sprintf(queuNameTemplate, routing.ArmyMovesPrefix, user),
		fmt.Sprintf("%s.*", routing.ArmyMovesPrefix),
		pubsub.TransientQueue,
		handlerMove(state),
	)
	if err != nil {
		fmt.Println("Unable to subscribe to queue for moves", err)
		os.Exit(1)
	}

	defer rabbit.Close()
	for {
		input := gamelogic.GetInput()
		if len(input) == 0 {
			continue
		}
		var err error
		switch input[0] {
		case "spawn":
			err = state.CommandSpawn(input)
		case "move":
			mv, err := state.CommandMove(input)
			if err != nil {
				fmt.Println("Move has failed: ", err)
				continue
			}

			err = pubsub.PublishJSON(
				ch,
				routing.ExchangePerilTopic,
				fmt.Sprintf(queuNameTemplate, routing.ArmyMovesPrefix, user),
				mv,
			)
			if err != nil {
				fmt.Println("Unable to publish the move: ", err)
				continue
			}
			fmt.Println("the move has ben publish....")
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

func handlerPause(gs *gamelogic.GameState) func(routing.PlayingState) {
	return func(ps routing.PlayingState) {
		defer fmt.Print("> ")
		gs.HandlePause(ps)
	}
}

func handlerMove(gs *gamelogic.GameState) func(gamelogic.ArmyMove) {
	return func(am gamelogic.ArmyMove) {
		defer fmt.Print("> ")
		gs.HandleMove(am)
	}
}
