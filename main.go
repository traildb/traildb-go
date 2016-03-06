package main

import "fmt"

func main() {
	tdb, err := Open("output.30day.29of30.tdb")
	if err != nil {
		panic(err.Error())
	}
	fmt.Println(tdb)
	fmt.Println(tdb.Version())

	// var total int
	// for i := 0; i < tdb.numTrails; i++ {
	// 	trail, err := NewTrail(tdb, i)
	// 	if err != nil {
	// 		panic(err.Error())
	// 	}
	// 	// fmt.Println(trail)
	// 	for {
	// 		evt := trail.NextEvent()
	// 		if evt == nil {
	// 			trail.Close()
	// 			break
	// 		}
	// 		total++
	// 		// evt.Print()
	// 	}
	// }
	// fmt.Println(total)
	// trails, err := tdb.FindTrails(map[string]string{"type": "cli"})
	// if err != nil {
	// 	panic(err.Error())
	// }
	// for _, trail := range trails {
	// 	for {
	// 		evt := trail.NextEvent()
	// 		if evt == nil {
	// 			trail.Close()
	// 			break
	// 		}
	// 		evt.ToMap()
	// 	}
	// }
	// fmt.Println(len(trails))
	tdb.Close()

	cons, err := NewTrailDBConstructor("foobar.tdb", "first", "second")
	if err != nil {
		panic(err.Error())
	}
	cons.Close()
}
