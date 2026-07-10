package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"time"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"

	amqp "github.com/rabbitmq/amqp091-go"
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
		handleWar(state, warResultHandler(ch, state)))
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
				routing.GenerateKey(routing.ArmyMovesPrefix, user),
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
			if len(input) < 2 {
				fmt.Println("we need the number of logs to generate")
				continue
			}
			publisghMaliciousLogs(ch,
				state,
				gamelogic.GetMaliciousLog(),
				input[1],
			)

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
			fmt.Println("An error ocurred:", err)
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

func handleWar(gs *gamelogic.GameState,
	warResulthandler func(gamelogic.WarOutcome, string, string) pubsub.AckType) func(gamelogic.RecognitionOfWar) pubsub.AckType {
	return func(row gamelogic.RecognitionOfWar) pubsub.AckType {
		defer fmt.Print("> ")

		outcome, winner, loser := gs.HandleWar(row)
		switch outcome {
		case gamelogic.WarOutcomeNotInvolved:
			return pubsub.NackReque
		case gamelogic.WarOutcomeNoUnits:
			return pubsub.NackDiscard
		case gamelogic.WarOutcomeOpponentWon,
			gamelogic.WarOutcomeYouWon,
			gamelogic.WarOutcomeDraw:
			return warResulthandler(outcome, winner, loser)
		default:
			fmt.Println("Invalid outcome")
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

func warResultHandler(ch *amqp.Channel, gs *gamelogic.GameState) func(gamelogic.WarOutcome, string, string) pubsub.AckType {
	return func(outcome gamelogic.WarOutcome, winner, losser string) pubsub.AckType {
		var result string
		switch outcome {
		case gamelogic.WarOutcomeDraw:
			result = fmt.Sprintf("A war between {%s} and {%s} resulted in a draw", winner, losser)
		case gamelogic.WarOutcomeOpponentWon, gamelogic.WarOutcomeYouWon:
			result = fmt.Sprintf("{%s} won a war against {%s}", winner, losser)
		default:
			result = "unknow result type"
		}

		err := pubsub.PublishGameLog(ch, routing.GameLog{
			CurrentTime: time.Now().UTC(),
			Message:     result,
			Username:    gs.GetUsername(),
		})
		if err != nil {
			return pubsub.NackReque
		}

		return pubsub.Ack
	}
}

func publisghMaliciousLogs(ch *amqp.Channel, gs *gamelogic.GameState, message, quantity string) {
	num, err := strconv.Atoi(quantity)
	if err != nil {
		return
	}

	for range num {
		pubsub.PublishGameLog(ch, routing.GameLog{
			CurrentTime: time.Now().UTC(),
			Message:     message,
			Username:    gs.GetUsername(),
		})
	}
}
