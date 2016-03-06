package main

/*
#cgo CFLAGS: -I/usr/local/include
#cgo LDFLAGS: -L/usr/local/lib -ltraildb -lm -lJudy -ljson-c

#include <traildb.h>
#include <stdlib.h>
*/
import "C"
import (
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
	for i := 0; i <= int(numFields); i++ {
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
	fmt.Printf("%s: %s", evt.Timestamp, evt.ToMap())
}

func (evt *Event) ToMap() map[string]string {
	fields := make(map[string]string)
	var vlength C.uint64_t

	for _, item := range evt.items {
		value := C.GoString(C.tdb_get_item_value(evt.trail.db.db, item, &vlength))
		key := C.GoString(C.tdb_get_field_name(evt.trail.db.db, C.tdb_item_field(item)))
		fields[key] = value
	}
	return fields
}

// func (Db *Tdb) GetItemValueI(item RawItem) string {
// 	res, ok := Db.interned_values[item]
// 	if ok {
// 		return res
// 	} else {
// 		res = Db.GetItemValue(item)
// 		Db.interned_values[item] = res
// 		return res
// 	}
// }

// func (Db *Tdb) GetFieldNameI(field Field) string {
// 	res, ok := Db.interned_fields[field]
// 	if ok {
// 		return res
// 	} else {
// 		res = Db.GetFieldName(field)
// 		Db.interned_fields[field] = res
// 		return res
// 	}
// }

// func (evt *Event) NextItem() {
// 	var value C.tdb_val
// 	fmt.Println(item)
// }

// var vlength C.uint64_t

// fields := make(map[string]string)
// for i, fieldName := range trail.db.fieldNames {
// 	C.tdb_get_item(event.items)
// }

// func newEvent(event *C.tdb_event) *Event {

// }

// func (db *Tdb) GetTrail(cookie string) (*Cursor, error) {
// 	if cookiebin, er := hex.DecodeString(cookie); er == nil {
// 		v := binary.BigEndian.Uint64(cookiebin)
// 		if trail_id, ok := db.trail_index[v]; ok {
// 			// return Db.DecodeTrail(trail_id), nil
// 			return NewCursor(Db, trail_id)
// 		} else {
// 			return nil, errors.New("Cannot find cookie " + cookie)
// 		}

// 	} else {
// 		return nil, er
// 	}
// }

// func (Db *Tdb) BuildTrailIndex() {
// 	if Db.trail_index != nil {
// 		return
// 	}

// 	Db.trail_index = make(map[uint64]uint64)
// 	for i := uint64(0); i < Db.NumTrails(); i++ {
// 		v := binary.BigEndian.Uint64(Db.GetTrail(i))
// 		Db.trail_index[v] = i
// 	}
// }

// func timeTrack(start time.Time, name string) {
// 	elapsed := time.Since(start)
// 	log.Printf("%s took %s", name, elapsed)
// }

// func (Db *Tdb) NumTrails() uint64 {
// 	return uint64(C.tdb_num_trails(Db.Db))
// }

// func (Db *Tdb) NumFields() int {
// 	return int(C.tdb_num_fields(Db.Db))
// }

// func (Db *Tdb) GetTrail(trail_id uint64) []byte {
// 	c := C.tdb_get_trail(Db.Db, C.uint64_t(trail_id))
// 	return C.GoBytes(unsafe.Pointer(c), 8)
// }

// func (Db *Tdb) GetValue(field Field, value Value) string {
// 	return C.GoString(C.tdb_get_value(Db.Db, C.tdb_field(field), C.tdb_val(value)))
// }

// func (Db *Tdb) GetItemValue(item RawItem) string {
// 	return C.GoString(C.tdb_get_item_value(Db.Db, C.tdb_item(item)))
// }

// func (Db *Tdb) GetItemValueI(item RawItem) string {
// 	res, ok := Db.interned_values[item]
// 	if ok {
// 		return res
// 	} else {
// 		res = Db.GetItemValue(item)
// 		Db.interned_values[item] = res
// 		return res
// 	}
// }

// func (Db *Tdb) GetFieldNameI(field Field) string {
// 	res, ok := Db.interned_fields[field]
// 	if ok {
// 		return res
// 	} else {
// 		res = Db.GetFieldName(field)
// 		Db.interned_fields[field] = res
// 		return res
// 	}
// }

// func (Db *Tdb) GetItem(field Field, value string) RawItem {
// 	cs := C.CString(value)
// 	defer C.free(unsafe.Pointer(cs))
// 	return RawItem(C.tdb_get_item(Db.Db, C.tdb_field(field), cs))
// }

// func (Db *Tdb) GetField(field_name string) Field {
// 	cs := C.CString(field_name)
// 	defer C.free(unsafe.Pointer(cs))
// 	return Field(C.tdb_get_field(Db.Db, cs))
// }

// func (Db *Tdb) DecodeTrailRaw(trail_id uint64) []RawItem {
// 	var r int

// 	for {
// 		r = int(C.tdb_decode_trail(Db.Db, C.uint64_t(trail_id), (*C.uint32_t)(&Db.buf[0]), C.uint32_t(Db.buf_size), 0))
// 		if r == Db.buf_size {
// 			Db.buf_size = Db.buf_size * 2
// 			Db.buf = make([]RawItem, Db.buf_size)
// 			break
// 		}
// 	}
// 	res := make([]RawItem, r)
// 	copy(res, Db.buf)
// 	return res
// }

// func ItemField(item RawItem) Field {
// 	return Field(item & 255)
// }

// func ItemVal(item RawItem) Field {
// 	return Field(item >> 8)
// }

// func (Db *Tdb) DecodeTrail(trail_id uint64) []Item {
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

// 	result := make([]Item, r/event_size)

// 	for i := 0; i < r/event_size; i++ {
// 		var item Item
// 		b := i * event_size

// 		item.Timestamp = time.Unix(int64(Db.buf[b]), 0)
// 		item.Fields = make(map[string]string)

// 		for k := 1; k < num_fields; k++ {
// 			name := Db.GetFieldNameI(ItemField(Db.buf[b+k]))
// 			value := Db.GetItemValueI(Db.buf[b+k])
// 			item.Fields[name] = value
// 		}
// 		result[i] = item
// 	}

// 	return result
// }

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

//-----------------

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
