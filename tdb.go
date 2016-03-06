package main

/*
#cgo CFLAGS: -I/usr/local/include
#cgo LDFLAGS: -L/usr/local/lib -ltraildb -lm -lJudy -ljson-c

#include <traildb.h>
#include <stdlib.h>

*/
import "C"
import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"os"
	"time"
)

import "unsafe"
import "errors"

type TrailDB struct {
	db *C.tdb

	numTrails     int
	numFields     int
	numEvents     int
	minTimestamp  int
	maxTimestamp  int
	fieldNames    []string
	fieldNameToId map[string]uint32
}

type TrailDBConstructor struct {
	cons    *C.tdb_cons
	path    string
	ofields []string
}

type Trail struct {
	db    *TrailDB
	trail *C.tdb_cursor
}

type Event struct {
	trail     *Trail
	Timestamp time.Time
	Fields    map[string]string
	items     []C.tdb_item
}

func NewTrailDBConstructor(path string, ofields ...string) (*TrailDBConstructor, error) {
	cons := C.tdb_cons_init()
	cpath := C.CString(path)
	defer C.free(unsafe.Pointer(cpath))

	var ofield_p *C.char
	ptrSize := unsafe.Sizeof(ofield_p)

	// Allocate the char** list.
	ptr := C.malloc(C.size_t(len(ofields)) * C.size_t(ptrSize))
	defer C.free(ptr)

	// Assign each byte slice to its appropriate offset.
	for i := 0; i < len(ofields); i++ {
		element := (**C.char)(unsafe.Pointer(uintptr(ptr) + uintptr(i)*ptrSize))
		cofield := C.CString(ofields[i])
		defer C.free(unsafe.Pointer(cofield))
		*element = cofield
	}

	if err := C.tdb_cons_open(cons, cpath, (**C.char)(ptr), C.uint64_t(len(ofields))); err != 0 {
		return nil, errors.New(errToString(err))
	}
	return &TrailDBConstructor{
		cons:    cons,
		path:    path,
		ofields: ofields,
	}, nil
}

func (cons *TrailDBConstructor) Add(cookie string, timestamp time.Time, values []string) error {
	if len(cookie) != 32 {
		return errors.New("Cookie in the wrong format, needs to be 32 chars: " + cookie)
	}
	cookiebin, err := hex.DecodeString(cookie)
	if err != nil {
		return err
	}
	var values_p *C.char
	value_lengths := make([]C.uint64_t, len(cons.ofields))

	ptrSize := unsafe.Sizeof(values_p)

	// Allocate the char** list.
	ptr := C.malloc(C.size_t(len(cons.ofields)) * C.size_t(ptrSize))
	defer C.free(ptr)

	// Assign each byte slice to its appropriate offset.
	var currentString string
	passedLength := len(values)
	for i := 0; i < len(cons.ofields); i++ {
		element := (**C.char)(unsafe.Pointer(uintptr(ptr) + uintptr(i)*ptrSize))
		if i+1 <= passedLength {
			currentString = values[i]
		} else {
			currentString = ""
		}
		cvalues := C.CString(currentString)
		defer C.free(unsafe.Pointer(cvalues))
		value_lengths[i] = C.uint64_t(len(currentString))
		*element = cvalues
	}
	rawCookiePtr := (*C.uint8_t)(unsafe.Pointer(&cookiebin[0]))
	valueLengthsPtr := (*C.uint64_t)(unsafe.Pointer(&value_lengths[0]))
	err1 := C.tdb_cons_add(cons.cons, rawCookiePtr, C.uint64_t(timestamp.Unix()), ptr, valueLengthsPtr)
	if err1 != 0 {
		return errors.New(errToString(err1))
	}
	return nil
}

func (cons *TrailDBConstructor) Append(db *TrailDB) error {
	if err := C.tdb_cons_append(cons.cons, db.db); err != 0 {
		return errors.New(errToString(err))
	}
	return nil
}

func (cons *TrailDBConstructor) Finalize() error {
	if err := C.tdb_cons_finalize(cons.cons); err != 0 {
		return errors.New(errToString(err))
	}
	return nil

}

// tdb_opt_key
const (
	TDB_OPT_ONLY_DIFF_ITEMS          = 100
	TDB_OPT_EVENT_FILTER             = 101
	TDB_OPT_CURSOR_EVENT_BUFFER_SIZE = 102
	TDB_OPT_CONS_OUTPUT_FORMAT       = 1001
)

func make_tdb_opt_value(cbytes []byte) (result *C.tdb_opt_value) {
	buf := bytes.NewBuffer(cbytes[:])
	var ptr uint64
	if err := binary.Read(buf, binary.BigEndian, &ptr); err == nil {
		uptr := uintptr(ptr)
		return (*C.tdb_opt_value)(unsafe.Pointer(uptr))
	}
	return nil
}

func (cons *TrailDBConstructor) SetOpt(key int, value int) error {
	var buf []byte

	binary.BigEndian.PutUint64(buf, uint64(value))
	opt_value := *make_tdb_opt_value(buf)

	err := C.tdb_cons_set_opt(cons.cons, C.tdb_opt_key(key), opt_value)
	if err != 0 {
		return errors.New(errToString(err))
	}
	return nil
}

func (cons *TrailDBConstructor) GetOpt(key int, value int) (int, error) {
	var opt_value *C.tdb_opt_value
	err := C.tdb_cons_set_opt(cons.cons, C.tdb_opt_key(key), *opt_value)
	if err != 0 {
		return -1, errors.New(errToString(err))
	}
	buf := (*C.uint64_t)(unsafe.Pointer(opt_value))
	return int(*buf), nil
}

func (cons *TrailDBConstructor) Close() {
	C.tdb_cons_close(cons.cons)
}

func errToString(err C.tdb_error) string {
	return C.GoString(C.tdb_error_str(err))
}

func exists(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return true, err
}

func Open(s string) (*TrailDB, error) {
	ok, er := exists(s)
	if er != nil {
		return nil, er
	}
	if !ok {
		return nil, errors.New(s + ": Path doesn't exist")
	}
	db := C.tdb_init()
	cs := C.CString(s)
	defer C.free(unsafe.Pointer(cs))
	err := C.tdb_open(db, cs)
	if err != 0 {
		return nil, errors.New(s + ": Failed to open traildb: " + errToString(err))
	}
	numFields := int(C.tdb_num_fields(db))
	var fields []string
	fieldNameToId := make(map[string]uint32)
	// numFields contains timestamp too
	for i := 0; i <= int(numFields)-1; i++ {
		fieldName := C.GoString(C.tdb_get_field_name(db, C.tdb_field(i)))
		fieldNameToId[fieldName] = uint32(i)
		fields = append(fields, fieldName)
	}
	return &TrailDB{
		db:            db,
		numTrails:     int(C.tdb_num_trails(db)),
		numEvents:     int(C.tdb_num_events(db)),
		numFields:     numFields,
		minTimestamp:  int(C.tdb_min_timestamp(db)),
		maxTimestamp:  int(C.tdb_max_timestamp(db)),
		fieldNames:    fields,
		fieldNameToId: fieldNameToId,
	}, nil
}
func (db *TrailDB) GetTrailID(cookie string) (uint64, error) {
	var trail_id C.uint64_t
	err := C.tdb_get_trail_id(db.db, (*C.uint8_t)(unsafe.Pointer(&cookie)), &trail_id)
	if err != 0 {
		return 0, errors.New("Error while fetching trail_id for cookie " + cookie)
	}
	return uint64(trail_id), nil
}

func (db *TrailDB) Version() int {
	return int(C.tdb_version(db.db))
}

func (db *TrailDB) Close() {
	C.tdb_close(db.db)
}

func NewTrail(db *TrailDB, trail_id int) (*Trail, error) {
	trail := C.tdb_cursor_new(db.db)
	err := C.tdb_get_trail(trail, C.uint64_t(trail_id))
	if err != 0 {
		return nil, errors.New(errToString(err) + ": Failed to open Trail with id " + string(trail_id))
	}
	return &Trail{
		db:    db,
		trail: trail,
	}, nil
}

func (trail *Trail) Close() {
	C.tdb_cursor_free(trail.trail)
}

func (db *TrailDB) FindTrails(filters map[string]string) ([]*Trail, error) {
	var items []C.tdb_item

	for k, v := range filters {
		cs := C.CString(v)
		defer C.free(unsafe.Pointer(cs))

		item := C.tdb_get_item(db.db, C.tdb_field(db.fieldNameToId[k]), cs, C.uint64_t(len(v)))
		items = append(items, item)
	}

	var result []*Trail
	for i := 0; i < db.numTrails; i++ {
		trail, err := NewTrail(db, i)
		if err != nil {
			return nil, err
		}
		for {
			evt := trail.NextEvent()
			if evt == nil {
				trail.Close()
				break
			}
			if evt.contains(items) {
				returnTrail, err := NewTrail(db, i)
				if err != nil {
					return nil, err
				}
				result = append(result, returnTrail)
				trail.Close()
				break
			}
		}
	}
	return result, nil
}
func (evt *Event) contains(filters []C.tdb_item) bool {
	results := make([]bool, len(filters))
	for _, item := range evt.items {
		for i, filter := range filters {
			if filter == item {
				results[i] = true
			}
		}
		a := true
		for _, r := range results {
			a = a && r
		}
		if a == true {
			return true
		}
	}
	return false
}

func (trail *Trail) NextEvent() *Event {
	event := C.tdb_cursor_next(trail.trail)
	if event == nil {
		return nil
	}
	items := make([]C.tdb_item, int(event.num_items))

	s := unsafe.Pointer(uintptr(unsafe.Pointer(event)) + C.sizeof_tdb_event)
	for i := 0; i < int(event.num_items); i++ {
		item := *(*C.tdb_item)(unsafe.Pointer(uintptr(s) + uintptr(i*C.sizeof_tdb_item)))
		items[i] = item
	}

	return &Event{
		trail:     trail,
		Timestamp: time.Unix(int64(event.timestamp), 0),
		items:     items,
	}
}

func (evt *Event) Print() {
	fmt.Printf("%s: %s\n", evt.Timestamp, evt.ToMap())
}

func (evt *Event) ToMap() map[string]string {
	fields := make(map[string]string)
	var vlength C.uint64_t

	for _, item := range evt.items {
		itemValue := C.tdb_get_item_value(evt.trail.db.db, item, &vlength)
		value := C.GoStringN(itemValue, C.int(vlength))
		key := C.GoString(C.tdb_get_field_name(evt.trail.db.db, C.tdb_item_field(item)))
		fields[key] = value
	}
	return fields
}

// ---------------------------------

// func (Db *Tdb) DecodeTrailStruct(trail_id uint64, t reflect.Type) interface{} {
// 	var r int

// 	for {
// 		r = int(C.tdb_decode_trail(Db.Db, C.uint64_t(trail_id), (*C.uint32_t)(&Db.buf[0]), C.uint32_t(Db.buf_size), 0))
// 		if r == Db.buf_size {
// 			Db.buf_size = Db.buf_size * 2
// 			Db.buf = make([]RawItem, Db.buf_size)
// 		} else {
// 			break
// 		}
// 	}

// 	num_fields := Db.NumFields()
// 	event_size := num_fields + 1

// 	num_events := r / event_size

// 	result := reflect.MakeSlice(reflect.SliceOf(t), num_events, num_events)

// 	struct_field_ids := make([]int, 0)
// 	tdb_field_ids := make([]Field, 0)

// 	for i := 0; i < t.NumField(); i++ {
// 		field := t.Field(i)
// 		field_name := field.Tag.Get("tdb")
// 		if field_name != "" {
// 			tdb_id := Db.GetField(field_name)
// 			if tdb_id > 0 && tdb_id != 255 {
// 				struct_field_ids = append(struct_field_ids, i)
// 				tdb_field_ids = append(tdb_field_ids, tdb_id)
// 			} else {
// 				if field_name == "timestamp" {
// 					struct_field_ids = append(struct_field_ids, i)
// 					tdb_field_ids = append(tdb_field_ids, 0)
// 				}
// 			}
// 		}
// 	}

// 	for i := 0; i < r/event_size; i++ {
// 		b := i * event_size

// 		v := result.Index(i)

// 		for k := 0; k < len(tdb_field_ids); k++ {
// 			if tdb_field_ids[k] == 0 {
// 				v.Field(struct_field_ids[k]).SetInt(int64(Db.buf[b]))
// 			} else {
// 				value := Db.GetItemValueI(Db.buf[b+int(tdb_field_ids[k])])
// 				v.Field(struct_field_ids[k]).SetString(value)
// 			}
// 		}
// 	}

// 	return result.Interface()
// }

// func (Db *Tdb) GetTrailStruct(cookie string, t interface{}) (interface{}, error) {
// 	Db.BuildTrailIndex()

// 	if cookiebin, er := hex.DecodeString(cookie); er == nil {
// 		v := binary.BigEndian.Uint64(cookiebin)
// 		if trail_id, ok := Db.trail_index[v]; ok {
// 			return Db.DecodeTrailStruct(trail_id, reflect.TypeOf(t)), nil
// 		} else {
// 			return nil, errors.New("Cannot find cookie " + cookie)
// 		}

// 	} else {
// 		return nil, er
// 	}

// }
