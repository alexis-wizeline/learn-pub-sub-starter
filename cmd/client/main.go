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
		routing.GenerateKey(routing.PauseKey, user),
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
		routing.GenerateKey(routing.ArmyMovesPrefix, user),
		routing.GenerateKey(routing.ArmyMovesPrefix),
		pubsub.TransientQueue,
		handlerMove(state, makeWarHandler(ch, state)),
	)
	if err != nil {
		fmt.Println("Unable to subscribe to queue for moves", err)
		os.Exit(1)
	}

	err = pubsub.SubscribeJSON(
		rabbit,
		routing.ExchangePerilTopic,
		routing.WarRecognitionsPrefix,
		routing.GenerateKey(routing.WarRecognitionsPrefix),
		pubsub.DurableQueue,
		handleWar(state))
	if err != nil {
		fmt.Println("Unable to subscribe to queue for war", err)
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

func handlerPause(gs *gamelogic.GameState) func(routing.PlayingState) pubsub.AckType {
	return func(ps routing.PlayingState) pubsub.AckType {
		defer fmt.Print("> ")
		gs.HandlePause(ps)
		return pubsub.Ack
	}
}

func handlerMove(gs *gamelogic.GameState, warHandler func(move gamelogic.ArmyMove) pubsub.AckType) func(gamelogic.ArmyMove) pubsub.AckType {
	return func(am gamelogic.ArmyMove) pubsub.AckType {
		defer fmt.Print("> ")
		outcome := gs.HandleMove(am)
		switch outcome {
		case gamelogic.MoveOutComeSafe:
			return pubsub.Ack
		case gamelogic.MoveOutcomeMakeWar:
			return warHandler(am)
		case gamelogic.MoveOutcomeSamePlayer:
		default:
		}

		return pubsub.NackDiscard
	}
}

func handleWar(gs *gamelogic.GameState) func(gamelogic.RecognitionOfWar) pubsub.AckType {
	return func(row gamelogic.RecognitionOfWar) pubsub.AckType {
		defer fmt.Print("> ")

		outcome, _, _ := gs.HandleWar(row)
		switch outcome {
		case gamelogic.WarOutcomeNotInvolved:
			return pubsub.NackReque
		case gamelogic.WarOutcomeNoUnits:
			return pubsub.NackDiscard
		case gamelogic.WarOutcomeOpponentWon,
			gamelogic.WarOutcomeYouWon,
			gamelogic.WarOutcomeDraw:
			return pubsub.Ack
		default:
			fmt.Println("Invalid outocome")
		}

		return pubsub.NackDiscard
	}
}

func makeWarHandler(ch *amqp.Channel, gs *gamelogic.GameState) func(gamelogic.ArmyMove) pubsub.AckType {
	return func(am gamelogic.ArmyMove) pubsub.AckType {
		err := pubsub.PublishJSON(
			ch,
			routing.ExchangePerilTopic,
			routing.GenerateKey(routing.WarRecognitionsPrefix, gs.GetUsername()),
			gamelogic.RecognitionOfWar{
				Attacker: am.Player,
				Defender: gs.GetPlayerSnap(),
			})
		if err != nil {
			return pubsub.NackReque
		}
		return pubsub.Ack
	}
}
