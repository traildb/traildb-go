package main

import (
	"fmt"
	"github.com/traildb/traildb-go"
	"os"
)

var SESSION_LIMIT = uint64(30 * 60)

func main() {
	db, err := tdb.Open(os.Args[1])
	if err != nil {
		panic(err)
	}
	trail, err := tdb.NewCursor(db)
	if err != nil {
		panic(err)
	}
	for i := uint64(0); i < db.NumTrails; i++ {
		err := tdb.GetTrail(trail, i)
		if err != nil {
			panic(err)
		}
		prev_time, _ := trail.NextTimestamp()
		num_events := 1
		num_sessions := 1
		for {
			tstamp, stop := trail.NextTimestamp()
			if stop {
				break
			}
			if tstamp-prev_time > SESSION_LIMIT {
				num_sessions += 1
			}
			prev_time = tstamp
			num_events += 1
		}
		fmt.Printf("Trail[%d] Number of Sessions: %d Number of Events: %d\n",
			i, num_sessions, num_events)
	}
}
