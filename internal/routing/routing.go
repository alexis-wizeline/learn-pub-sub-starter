package routing

import "strings"

const (
	ArmyMovesPrefix = "army_moves"

	WarRecognitionsPrefix = "war"

	PauseKey = "pause"

	GameLogSlug = "game_logs"
)

const (
	ExchangePerilDirect = "peril_direct"
	ExchangePerilTopic  = "peril_topic"
	ExchnagePerilDLX    = "peril_dlx"
)

func GenerateKey(tags ...string) string {
	if len(tags) == 1 {
		tags = append(tags, "*")
	}

	return strings.Join(tags, ".")
}
