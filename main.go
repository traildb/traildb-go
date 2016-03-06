package main

import (
	"fmt"
	"time"
)

func main() {
	// tdb, err := Open("output.30day.29of30.tdb")
	// if err != nil {
	// 	panic(err.Error())
	// }
	// fmt.Println(tdb)
	// fmt.Println(tdb.Version())

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
	// tdb.Close()

	cookie := "12345678123456781234567812345678"
	cons, err := NewTrailDBConstructor("test.tdb", "field1", "field2")
	if err != nil {
		panic(err.Error())
	}

	cons.Add(cookie, time.Now(), []string{"a"})
	cons.Add(cookie, time.Now(), []string{"d", "e"})
	cons.Add(cookie, time.Now(), []string{"e", "j"})
	cons.Finalize()

	cons.Close()

	tdb, err := Open("test.tdb")
	if err != nil {
		panic(err.Error())
	}
	fmt.Println(tdb)
	fmt.Println(tdb.Version())

	for i := 0; i < tdb.numTrails; i++ {
		trail, err := NewTrail(tdb, i)
		if err != nil {
			panic(err.Error())
		}
		// fmt.Println(trail)
		for {
			evt := trail.NextEvent()
			if evt == nil {
				trail.Close()
				break
			}
			evt.Print()
		}
	}

	// from traildb import TrailDB, TrailDBConstructor

	// cookie = '12345678123456781234567812345678'
	// cons = TrailDBConstructor('test.tdb', ['field1', 'field2'])
	// cons.add(cookie, 123, ['a'])
	// cons.add(cookie, 124, ['b', 'c'])
	// tdb = cons.finalize()

	// for cookie, trail in tdb.crumbs():
	//        print cookie, trail

}
