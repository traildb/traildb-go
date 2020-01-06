package tdb

/*
#cgo pkg-config: traildb

#cgo linux LDFLAGS: -lJudy -larchive

#include <traildb.h>
#include <stdlib.h>

*/
import "C"

import (
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"reflect"
	"unsafe"
	"strings"
)

/*
NOTE: MULTI_CURSOR_BUFFER_SIZE must be less than (1 << 30)
see MultiCursor.NextBatch for the reason
*/
var MULTI_CURSOR_BUFFER_SIZE = 1000

type TrailDB struct {
	db *C.tdb

	NumTrails     uint64
	NumFields     uint64
	NumEvents     uint64
	minTimestamp  uint64
	maxTimestamp  uint64
	fieldNames    []string
	fieldNameToId map[string]uint64
}

type TrailDBConstructor struct {
	cons    *C.tdb_cons
	path    string
	ofields []string

	valueLengths []C.uint64_t
	valuePtr     unsafe.Pointer
}

type Trail struct {
	db     *TrailDB
	trail  *C.tdb_cursor
	filter *C.struct_tdb_event_filter
}

type Event struct {
	trail     *Trail
	Timestamp uint64
	Fields    map[string]string
	items     []C.tdb_item
}

type FilterTerm struct {
	IsNegative bool
	Value      string
	Field      string
}

type EventFilter struct {
	filter *C.struct_tdb_event_filter
}

type MultiCursor struct {
	mcursor           *C.tdb_multi_cursor
	cursors           []*Trail
	mevent_buffer_ptr unsafe.Pointer
	event_buffer      []*Event
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
		cons:         cons,
		path:         path,
		ofields:      ofields,
		valueLengths: make([]C.uint64_t, len(ofields)),
		valuePtr:     C.malloc(C.size_t(len(ofields)) * C.size_t(ptrSize)),
	}, nil
}

func rawCookie(cookie string) (*C.uint8_t, error) {
	cookiebin, err := hex.DecodeString(cookie)
	if err != nil {
		return nil, err
	}
	return (*C.uint8_t)(unsafe.Pointer(&cookiebin[0])), nil
}
func (cons *TrailDBConstructor) Add(cookie string, timestamp int64, values []string) error {
	if len(cookie) != 32 {
		return errors.New("Cookie in the wrong format, needs to be 32 chars: " + cookie)
	}
	cookiebin, err := rawCookie(cookie)
	if err != nil {
		return err
	}
	var values_p *C.char

	ptrSize := unsafe.Sizeof(values_p)

	// Assign each byte slice to its appropriate offset.
	var currentString string
	passedLength := len(values)
	for i := 0; i < len(cons.ofields); i++ {
		element := (**C.char)(unsafe.Pointer(uintptr(cons.valuePtr) + uintptr(i)*ptrSize))
		if i+1 <= passedLength {
			currentString = values[i]
		} else {
			currentString = ""
		}
		cvalues := C.CString(currentString)
		defer C.free(unsafe.Pointer(cvalues))
		cons.valueLengths[i] = C.uint64_t(len(currentString))
		*element = cvalues
	}
	valueLengthsPtr := (*C.uint64_t)(unsafe.Pointer(&cons.valueLengths[0]))
	err1 := C.tdb_cons_add(cons.cons, cookiebin, C.uint64_t(timestamp), (**C.char)(cons.valuePtr), valueLengthsPtr)
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
	TDB_OPT_CONS_NO_BIGRAMS          = 1002
)

func (cons *TrailDBConstructor) SetOpt(key int, value int) error {
	value64 := uint64(value)
	opt_value := (*C.tdb_opt_value)(unsafe.Pointer(&value64))

	err := C.tdb_cons_set_opt(cons.cons, C.tdb_opt_key(key), *opt_value)
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
	defer C.free(cons.valuePtr)
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
	if !strings.HasSuffix(s, ".tdb") {
		s = s + ".tdb"
	}

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
	numFields := uint64(C.tdb_num_fields(db))
	var fields []string
	fieldNameToId := make(map[string]uint64)
	// numFields contains timestamp too
	for i := uint64(0); i <= uint64(numFields)-1; i++ {
		fieldName := C.GoString(C.tdb_get_field_name(db, C.tdb_field(i)))
		fieldNameToId[fieldName] = uint64(i)
		fields = append(fields, fieldName)
	}
	return &TrailDB{
		db:            db,
		NumTrails:     uint64(C.tdb_num_trails(db)),
		NumEvents:     uint64(C.tdb_num_events(db)),
		NumFields:     numFields,
		minTimestamp:  uint64(C.tdb_min_timestamp(db)),
		maxTimestamp:  uint64(C.tdb_max_timestamp(db)),
		fieldNames:    fields,
		fieldNameToId: fieldNameToId,
	}, nil
}

func (db *TrailDB) GetFieldNames() []string {
	return db.fieldNames[1:]
}

func (db *TrailDB) SetFilter(filter *EventFilter) error {
	var val C.tdb_opt_value
	ptr := (*uintptr)(unsafe.Pointer(&val[0]))
	*ptr = (uintptr)(unsafe.Pointer(filter.filter))
	err := C.tdb_set_opt(db.db, C.tdb_opt_key(TDB_OPT_EVENT_FILTER), val)
	if err != 0 {
		return errors.New("Could not set event filter")
	}
	return nil
}

func (db *TrailDB) GetTrailID(cookie string) (uint64, error) {
	var trail_id C.uint64_t
	cookiebin, err := rawCookie(cookie)
	if err != nil {
		return 0, err
	}
	err1 := C.tdb_get_trail_id(db.db, cookiebin, &trail_id)
	if err1 != 0 {
		return 0, errors.New("Error while fetching trail_id for cookie " + cookie)
	}
	return uint64(trail_id), nil
}

func (db *TrailDB) GetUUID(trail_id uint64) string {
	uuid := C.tdb_get_uuid(db.db, C.uint64_t(trail_id))
	if uuid == nil {
		return ""
	} else {
		return hex.EncodeToString(C.GoBytes(unsafe.Pointer(uuid), 16))
	}
}

func (db *TrailDB) GetField(field_name string) (uint64, error) {
	field := C.tdb_field(0)
	err := C.tdb_get_field(db.db, C.CString(field_name), &field)
	if err != 0 {
		return 0, errors.New(errToString(err))
	}
	return uint64(field), nil
}

func (db *TrailDB) Version() uint64 {
	return uint64(C.tdb_version(db.db))
}

func (db *TrailDB) Close() {
	C.tdb_close(db.db)
}

func NewCursor(db *TrailDB) (*Trail, error) {
	trail := C.tdb_cursor_new(db.db)
	if trail == nil {
		return nil, errors.New("Could not create a new cursor (out of memory?)")
	}
	return &Trail{db: db, trail: trail}, nil
}

func GetTrail(trail *Trail, trail_id uint64) error {
	err := C.tdb_get_trail(trail.trail, C.uint64_t(trail_id))
	if err != 0 {
		return errors.New(errToString(err) + ": Failed to open Trail with id " + string(trail_id))
	}
	return nil
}

func NewTrail(db *TrailDB, trail_id uint64) (*Trail, error) {
	trail, err := NewCursor(db)
	if err != nil {
		return trail, err
	}
	err = GetTrail(trail, trail_id)
	return trail, err
}

func (trail *Trail) SetFilter(filter *EventFilter) error {
	err := C.tdb_cursor_set_event_filter(trail.trail, filter.filter)
	if err != 0 {
		return errors.New(errToString(err))
	}
	/*
	   we need to keep the filter alive for the lifetime of the cursor
	*/
	trail.filter = filter.filter
	return nil
}

func (trail *Trail) UnsetFilter() {
	C.tdb_cursor_unset_event_filter(trail.trail)
	trail.filter = nil
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
	for i := uint64(0); i < db.NumTrails; i++ {
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

func (trail *Trail) NextTimestamp() (uint64, bool) {
	event := C.tdb_cursor_next(trail.trail)
	if event == nil {
		return 0, true
	}
	return uint64(event.timestamp), false
}

func makeEvent(event *C.tdb_event, trail *Trail) *Event {
	items := make([]C.tdb_item, int(event.num_items))

	s := unsafe.Pointer(uintptr(unsafe.Pointer(event)) + C.sizeof_tdb_event)
	for i := uint64(0); i < uint64(event.num_items); i++ {
		item := *(*C.tdb_item)(unsafe.Pointer(uintptr(s) + uintptr(i*C.sizeof_tdb_item)))
		items[i] = item
	}

	return &Event{
		trail:     trail,
		Timestamp: uint64(event.timestamp),
		items:     items,
	}
}

func (trail *Trail) NextEvent() *Event {
	event := C.tdb_cursor_next(trail.trail)
	if event == nil {
		return nil
	} else {
		return makeEvent(event, trail)
	}
}

func (trail *Trail) GetTrailLength() int {
    tlength := C.tdb_get_trail_length(trail.trail)
    return int(tlength)
}

func (evt *Event) Print() {
	fmt.Printf("%d: %s\n", evt.Timestamp, evt.ToMap())
}

func (evt *Event) Get(index int) string {
	var vlength C.uint64_t
	itemValue := C.tdb_get_item_value(evt.trail.db.db, evt.items[index], &vlength)
	value := C.GoStringN(itemValue, C.int(vlength))
	return value
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

func (evt *Event) ToStruct(data interface{}) interface{} {
	t := reflect.TypeOf(data)

	struct_field_ids := make([]uint64, 0)
	tdb_field_ids := make([]uint64, 0)

	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		field_name := field.Tag.Get("tdb")
		if field_name != "" {
			tdb_id := evt.trail.db.fieldNameToId[field_name]
			struct_field_ids = append(struct_field_ids, uint64(i))
			tdb_field_ids = append(tdb_field_ids, tdb_id)
		}
	}

	v := reflect.New(t)
	v.Elem().Field(0).SetInt(int64(evt.Timestamp))
	for k := 1; k < len(tdb_field_ids); k++ {
		var vlength C.uint64_t
		itemValue := C.tdb_get_item_value(evt.trail.db.db, evt.items[tdb_field_ids[k]-1], &vlength)
		value := C.GoStringN(itemValue, C.int(vlength))
		v.Elem().Field(int(struct_field_ids[k])).SetString(value)
	}
	return v
}

func (db *TrailDB) NewEventFilter(query [][]FilterTerm) *EventFilter {
	filter := EventFilter{filter: C.tdb_event_filter_new()}
	for i, clause := range query {
		if i > 0 {
			err := C.tdb_event_filter_new_clause(filter.filter)
			if err != 0 {
				return nil
			}
		}
		for _, term := range clause {
			item := C.tdb_item(0)
			field_id, err := db.GetField(term.Field)
			if err == nil {
				cs := C.CString(term.Value)
				defer C.free(unsafe.Pointer(cs))
				item = C.tdb_get_item(db.db,
					C.tdb_field(field_id),
					cs,
					C.uint64_t(len(term.Value)))
			}
			isNegative := C.int(0)
			if term.IsNegative {
				isNegative = 1
			}
			ret := C.tdb_event_filter_add_term(filter.filter, item, isNegative)
			if ret != 0 {
				return nil
			}
		}
	}
	return &filter
}

func FreeEventFilter(filter *EventFilter) {
	C.tdb_event_filter_free(filter.filter)
}

func NewMultiCursor(cursors []*Trail) (*MultiCursor, error) {
	cursor_ptrs := make([]*C.tdb_cursor, len(cursors))
	for i, cursor := range cursors {
		cursor_ptrs[i] = cursor.trail
	}
	mcursor := C.tdb_multi_cursor_new(&cursor_ptrs[0], C.uint64_t(len(cursors)))
	if mcursor == nil {
		return nil, errors.New("Could not create a new multi cursor (out of memory?)")
	}
	/*
	   allocate an event buffer using malloc instead of using a Go slice.
	   Passing Go slices over CGo is SLOW
	*/
	mevent_buffer_ptr := C.malloc(C.sizeof_tdb_multi_event *
		C.size_t(MULTI_CURSOR_BUFFER_SIZE))
	if mevent_buffer_ptr == nil {
		return nil, errors.New("out of memory - malloc failed")
	}
	event_buffer := make([]*Event, MULTI_CURSOR_BUFFER_SIZE)
	return &MultiCursor{mcursor: mcursor,
		cursors:           cursors,
		mevent_buffer_ptr: mevent_buffer_ptr,
		event_buffer:      event_buffer}, nil
}

func FreeMultiCursor(mcursor *MultiCursor) {
	C.free(mcursor.mevent_buffer_ptr)
	C.tdb_multi_cursor_free(mcursor.mcursor)
}

func (mcursor *MultiCursor) Reset() {
	C.tdb_multi_cursor_reset(mcursor.mcursor)
}

func (mcursor *MultiCursor) NextBatch() []*Event {
	cnum := C.tdb_multi_cursor_next_batch(mcursor.mcursor,
		(*C.tdb_multi_event)(mcursor.mevent_buffer_ptr),
		C.uint64_t(MULTI_CURSOR_BUFFER_SIZE))
	num := uint64(cnum)
	/* NOTE: MULTI_CURSOR_BUFFER_SIZE must be less than (1 << 30) */
	mevents := (*[1 << 30]C.tdb_multi_event)(mcursor.mevent_buffer_ptr)[:num:num]

	for i := uint64(0); i < num; i++ {
		cursor_idx := mevents[i].cursor_idx
		mcursor.event_buffer[i] = makeEvent(mevents[i].event,
			mcursor.cursors[cursor_idx])
	}

	return mcursor.event_buffer[:num]
}
