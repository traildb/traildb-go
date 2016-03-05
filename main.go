package main

import "fmt"

func main() {
	tdb, err := Open("output.30day.29of30.tdb")
	if err != nil {
		panic(err.Error())
	}
	fmt.Println(tdb)
	fmt.Println(tdb.Version())

	trail, err := NewTrail(tdb, 0)
	if err != nil {
		panic(err.Error())
	}
	fmt.Println(trail)
	evt := trail.NextEvent()
	fmt.Println(evt)
	// evt.NextItem()
	// fmt.Println(item)

	trail.Close()
	tdb.Close()
}
