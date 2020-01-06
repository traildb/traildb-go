package tests

import (
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"testing"

	"github.com/traildb/traildb-go"
)

// copied from https://github.com/benbjohnson/testing
// assert fails the test if the condition is false.
func assert(tb testing.TB, condition bool, msg string, v ...interface{}) {
	if !condition {
		_, file, line, _ := runtime.Caller(1)
		fmt.Printf("\033[31m%s:%d: "+msg+"\033[39m\n\n", append([]interface{}{filepath.Base(file), line}, v...)...)
		tb.FailNow()
	}
}

// ok fails the test if an err is not nil.
func ok(tb testing.TB, err error) {
	if err != nil {
		_, file, line, _ := runtime.Caller(1)
		fmt.Printf("\033[31m%s:%d: unexpected error: %s\033[39m\n\n", filepath.Base(file), line, err.Error())
		tb.FailNow()
	}
}

// equals fails the test if exp is not equal to act.
func equals(tb testing.TB, exp, act interface{}) {
	if !reflect.DeepEqual(exp, act) {
		_, file, line, _ := runtime.Caller(1)
		fmt.Printf("\033[31m%s:%d:\n\n\texp: %#v\n\n\tgot: %#v\033[39m\n\n", filepath.Base(file), line, exp, act)
		tb.FailNow()
	}
}

const DbName = "testtrail"
const DbPath = "testtrail.tdb"
const UUID1 = "12345678123456781234567812345678"
const UUID2 = "02345678123456781234567812345678"

func BuildDB(t *testing.T) {
	traildb, err := tdb.NewTrailDBConstructor(DbName, []string{"field1", "field2"}...)
	ok(t, err)

	traildb.Add(UUID1, 1, []string{"a", "1"})
	traildb.Add(UUID1, 2, []string{"b", "2"})
	traildb.Add(UUID1, 3, []string{"c", "3"})

	traildb.Add(UUID2, 1, []string{"d", "1"})
	traildb.Add(UUID2, 2, []string{"e", "2"})
	traildb.Add(UUID2, 3, []string{"f", "3"})
	traildb.Add(UUID2, 4, []string{"a", "4"})

	traildb.Finalize()
	traildb.Close()
}

func DeleteDB(t *testing.T) {
	err := os.Remove(DbPath)
	ok(t, err)
}

func ReadDB(t *testing.T) *tdb.TrailDB {
	db, err := tdb.Open(DbPath)
	ok(t, err)
	return db
}

func LoadDB(t *testing.T) *tdb.TrailDB {
	BuildDB(t)
	return ReadDB(t)
}

func GetTrailAt(index uint64, t *testing.T, db *tdb.TrailDB) *tdb.Trail {
	trail, err := tdb.NewCursor(db)
	ok(t, err)
	ok(t, tdb.GetTrail(trail, index))
	return trail
}

func TestConstructor(t *testing.T) {
	db := LoadDB(t)
	defer DeleteDB(t)

	equals(t, uint64(2), db.NumTrails)

	trail := GetTrailAt(0, t, db)

	AssertEvent(t, trail.NextEvent(), map[string]string{"field1": "d", "field2": "1"}, 1)
	AssertEvent(t, trail.NextEvent(), map[string]string{"field1": "e", "field2": "2"}, 2)
	AssertEvent(t, trail.NextEvent(), map[string]string{"field1": "f", "field2": "3"}, 3)
	AssertEvent(t, trail.NextEvent(), map[string]string{"field1": "a", "field2": "4"}, 4)

	trail = GetTrailAt(1, t, db)

	AssertEvent(t, trail.NextEvent(), map[string]string{"field1": "a", "field2": "1"}, 1)
	AssertEvent(t, trail.NextEvent(), map[string]string{"field1": "b", "field2": "2"}, 2)
	AssertEvent(t, trail.NextEvent(), map[string]string{"field1": "c", "field2": "3"}, 3)
}

func AssertEvent(t *testing.T, evt *tdb.Event, expectedFields map[string]string, expectedTime uint64) {
	assert(t, evt != nil, "Could not get event")
	equals(t, expectedFields, evt.ToMap())
	equals(t, uint64(expectedTime), evt.Timestamp)
}

func AssertNotEvent(t *testing.T, evt *tdb.Event) {
	assert(t, evt == nil, "Not expected event")
}

func TestApi(t *testing.T) {
	db := LoadDB(t)
	defer DeleteDB(t)

	id, err := db.GetTrailID(UUID1)
	ok(t, err)
	equals(t, uint64(1), id)

	id, err = db.GetTrailID(UUID2)
	ok(t, err)
	equals(t, uint64(0), id)

        trail := GetTrailAt(id, t, db)
        trailLength := trail.GetTrailLength()
        equals(t, trailLength, 4)

	trails, err := db.FindTrails(map[string]string{"field1": "a"})
	ok(t, err)
	equals(t, len(trails), 2)

	equals(t, []string{"field1", "field2"}, db.GetFieldNames())

	field, err := db.GetField("field1")
	ok(t, err)
	equals(t, uint64(1), field)

	field, err = db.GetField("field2")
	ok(t, err)
	equals(t, uint64(2), field)

	field, err = db.GetField("field3")
	assert(t, err != nil, "should fail if invalid field")

	uuid := db.GetUUID(0x1)
	equals(t, UUID1, uuid)

	uuid = db.GetUUID(0x0)
	equals(t, UUID2, uuid)
}

func ApplyFilter(t *testing.T, filter *tdb.EventFilter, db *tdb.TrailDB) *tdb.Trail {
	trail := GetTrailAt(0, t, db)
	trail.SetFilter(filter)
	return trail
}

func TestFiltersDisjunction(t *testing.T) {
	db := LoadDB(t)
	defer DeleteDB(t)

	term1 := tdb.FilterTerm{Field: "field1", Value: "a"}
	term2 := tdb.FilterTerm{Field: "field2", Value: "3"}
	filter := db.NewEventFilter([][]tdb.FilterTerm{{term1, term2}})

	trail := ApplyFilter(t, filter, db)
	AssertEvent(t, trail.NextEvent(), map[string]string{"field1": "f", "field2": "3"}, 3)
	AssertEvent(t, trail.NextEvent(), map[string]string{"field1": "a", "field2": "4"}, 4)
	AssertNotEvent(t, trail.NextEvent())
}

func TestFiltersConjunction(t *testing.T) {
	db := LoadDB(t)
	defer DeleteDB(t)

	term1 := tdb.FilterTerm{Field: "field1", Value: "e"}
	term2 := tdb.FilterTerm{Field: "field2", Value: "2"}
	filter := db.NewEventFilter([][]tdb.FilterTerm{{term1}, {term2}})
	trail := ApplyFilter(t, filter, db)

	AssertEvent(t, trail.NextEvent(), map[string]string{"field1": "e", "field2": "2"}, 2)
	AssertNotEvent(t, trail.NextEvent())
}

func TestFiltersNegation(t *testing.T) {
	db := LoadDB(t)
	defer DeleteDB(t)

	term1 := tdb.FilterTerm{Field: "field1", Value: "e", IsNegative: true}
	term2 := tdb.FilterTerm{Field: "field2", Value: "4", IsNegative: true}
	filter := db.NewEventFilter([][]tdb.FilterTerm{{term1}, {term2}})

	trail := ApplyFilter(t, filter, db)
	AssertEvent(t, trail.NextEvent(), map[string]string{"field1": "d", "field2": "1"}, 1)
	AssertEvent(t, trail.NextEvent(), map[string]string{"field1": "f", "field2": "3"}, 3)
	AssertNotEvent(t, trail.NextEvent())
}

func TestMultiCursors(t *testing.T) {
	db := LoadDB(t)
	defer DeleteDB(t)

	trails, err := db.FindTrails(map[string]string{"field1": "a"})
	ok(t, err)

	multiCursor, err := tdb.NewMultiCursor(trails)
	ok(t, err)
	batch := multiCursor.NextBatch()
	equals(t, 6, len(batch))

	AssertEvent(t, batch[0], map[string]string{"field1": "d", "field2": "1"}, 1)
	AssertEvent(t, batch[1], map[string]string{"field1": "a", "field2": "1"}, 1)
	AssertEvent(t, batch[2], map[string]string{"field1": "b", "field2": "2"}, 2)
	AssertEvent(t, batch[3], map[string]string{"field1": "e", "field2": "2"}, 2)
	AssertEvent(t, batch[4], map[string]string{"field1": "f", "field2": "3"}, 3)
	AssertEvent(t, batch[5], map[string]string{"field1": "c", "field2": "3"}, 3)
}
