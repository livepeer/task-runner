package task

import (
	"math/rand"
	"time"

	"github.com/livepeer/joy4/format"
)

func init() {
	format.RegisterAll()
	rand.Seed(time.Now().UnixNano())
}
