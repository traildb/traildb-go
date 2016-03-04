package traildb

/*
#cgo CFLAGS: -I/usr/local/include
#cgo LDFLAGS: -L/usr/local/lib -ltraildb -lm -lJudy -ljson-c

#include <traildb.h>
#include <stdlib.h>
*/
import "C"
import "time"
import "unsafe"
import "errors"
import "encoding/binary"
import "encoding/hex"
import "log"
import "reflect"

type Tdb struct {
	Db       *C.tdb
	buf      []RawItem
	buf_size int

	interned_fields map[Field]string
	interned_values map[RawItem]string

	trail_index map[uint64]uint64
}

type Field uint8
type Value uint32
type RawItem uint32

type Item struct {
	Timestamp time.Time
	Fields    map[string]string
}

func Open(s string) (*Tdb, error) {
	cs := C.CString(s)
	Db := C.tdb_open(cs)
	C.free(unsafe.Pointer(cs))
	if Db == nil {
		return nil, errors.New("Failed to open traildb " + s)
	}
	return &Tdb{Db: Db,
		buf_size:        1,
		buf:             make([]RawItem, 1),
		interned_fields: make(map[Field]string),
		interned_values: make(map[RawItem]string)}, nil
}

func (Db *Tdb) BuildTrailIndex() {
	if Db.trail_index != nil {
		return
	}

	Db.trail_index = make(map[uint64]uint64)
	for i := uint64(0); i < Db.NumTrails(); i++ {
		v := binary.BigEndian.Uint64(Db.GetTrail(i))
		Db.trail_index[v] = i
	}
}

func timeTrack(start time.Time, name string) {
	elapsed := time.Since(start)
	log.Printf("%s took %s", name, elapsed)
}

func (Db *Tdb) GetTrail(cookie string) ([]Item, error) {
	Db.BuildTrailIndex()

	if cookiebin, er := hex.DecodeString(cookie); er == nil {
		v := binary.BigEndian.Uint64(cookiebin)
		if trail_id, ok := Db.trail_index[v]; ok {
			return Db.DecodeTrail(trail_id), nil
		} else {
			return nil, errors.New("Cannot find cookie " + cookie)
		}

	} else {
		return nil, er
	}
}

func (Db *Tdb) GetTrailStruct(cookie string, t interface{}) (interface{}, error) {
	Db.BuildTrailIndex()

	if cookiebin, er := hex.DecodeString(cookie); er == nil {
		v := binary.BigEndian.Uint64(cookiebin)
		if trail_id, ok := Db.trail_index[v]; ok {
			return Db.DecodeTrailStruct(trail_id, reflect.TypeOf(t)), nil
		} else {
			return nil, errors.New("Cannot find cookie " + cookie)
		}

	} else {
		return nil, er
	}
}

func (Db *Tdb) Close() {
	C.tdb_close(Db.Db)
}

func (Db *Tdb) NumTrails() uint64 {
	return uint64(C.tdb_num_trails(Db.Db))
}

func (Db *Tdb) NumFields() int {
	return int(C.tdb_num_fields(Db.Db))
}

func (Db *Tdb) Error() string {
	return C.GoString(C.tdb_error(Db.Db))
}

func (Db *Tdb) GetTrail(trail_id uint64) []byte {
	c := C.tdb_get_trail(Db.Db, C.uint64_t(trail_id))
	return C.GoBytes(unsafe.Pointer(c), 8)
}

func (Db *Tdb) GetValue(field Field, value Value) string {
	return C.GoString(C.tdb_get_value(Db.Db, C.tdb_field(field), C.tdb_val(value)))
}

func (Db *Tdb) GetItemValue(item RawItem) string {
	return C.GoString(C.tdb_get_item_value(Db.Db, C.tdb_item(item)))
}

func (Db *Tdb) GetItemValueI(item RawItem) string {
	res, ok := Db.interned_values[item]
	if ok {
		return res
	} else {
		res = Db.GetItemValue(item)
		Db.interned_values[item] = res
		return res
	}
}

func (Db *Tdb) GetFieldName(field Field) string {
	return C.GoString(C.tdb_get_field_name(Db.Db, C.tdb_field(field)))
}

func (Db *Tdb) GetFieldNameI(field Field) string {
	res, ok := Db.interned_fields[field]
	if ok {
		return res
	} else {
		res = Db.GetFieldName(field)
		Db.interned_fields[field] = res
		return res
	}
}

func (Db *Tdb) GetItem(field Field, value string) RawItem {
	cs := C.CString(value)
	defer C.free(unsafe.Pointer(cs))
	return RawItem(C.tdb_get_item(Db.Db, C.tdb_field(field), cs))
}

func (Db *Tdb) GetField(field_name string) Field {
	cs := C.CString(field_name)
	defer C.free(unsafe.Pointer(cs))
	return Field(C.tdb_get_field(Db.Db, cs))
}

func (Db *Tdb) DecodeTrailRaw(trail_id uint64) []RawItem {
	var r int

	for {
		r = int(C.tdb_decode_trail(Db.Db, C.uint64_t(trail_id), (*C.uint32_t)(&Db.buf[0]), C.uint32_t(Db.buf_size), 0))
		if r == Db.buf_size {
			Db.buf_size = Db.buf_size * 2
			Db.buf = make([]RawItem, Db.buf_size)
			break
		}
	}
	res := make([]RawItem, r)
	copy(res, Db.buf)
	return res
}

func ItemField(item RawItem) Field {
	return Field(item & 255)
}

func ItemVal(item RawItem) Field {
	return Field(item >> 8)
}

func (Db *Tdb) DecodeTrail(trail_id uint64) []Item {
	var r int

	for {
		r = int(C.tdb_decode_trail(Db.Db, C.uint64_t(trail_id), (*C.uint32_t)(&Db.buf[0]), C.uint32_t(Db.buf_size), 0))
		if r == Db.buf_size {
			Db.buf_size = Db.buf_size * 2
			Db.buf = make([]RawItem, Db.buf_size)
		} else {
			break
		}
	}

	num_fields := Db.NumFields()
	event_size := num_fields + 1

	result := make([]Item, r/event_size)

	for i := 0; i < r/event_size; i++ {
		var item Item
		b := i * event_size

		item.Timestamp = time.Unix(int64(Db.buf[b]), 0)
		item.Fields = make(map[string]string)

		for k := 1; k < num_fields; k++ {
			name := Db.GetFieldNameI(ItemField(Db.buf[b+k]))
			value := Db.GetItemValueI(Db.buf[b+k])
			item.Fields[name] = value
		}
		result[i] = item
	}

	return result
}

func (Db *Tdb) DecodeTrailStruct(trail_id uint64, t reflect.Type) interface{} {
	var r int

	for {
		r = int(C.tdb_decode_trail(Db.Db, C.uint64_t(trail_id), (*C.uint32_t)(&Db.buf[0]), C.uint32_t(Db.buf_size), 0))
		if r == Db.buf_size {
			Db.buf_size = Db.buf_size * 2
			Db.buf = make([]RawItem, Db.buf_size)
		} else {
			break
		}
	}

	num_fields := Db.NumFields()
	event_size := num_fields + 1

	num_events := r / event_size

	result := reflect.MakeSlice(reflect.SliceOf(t), num_events, num_events)

	struct_field_ids := make([]int, 0)
	tdb_field_ids := make([]Field, 0)

	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		field_name := field.Tag.Get("tdb")
		if field_name != "" {
			tdb_id := Db.GetField(field_name)
			if tdb_id > 0 && tdb_id != 255 {
				struct_field_ids = append(struct_field_ids, i)
				tdb_field_ids = append(tdb_field_ids, tdb_id)
			} else {
				if field_name == "timestamp" {
					struct_field_ids = append(struct_field_ids, i)
					tdb_field_ids = append(tdb_field_ids, 0)
				}
			}
		}
	}

	for i := 0; i < r/event_size; i++ {
		b := i * event_size

		v := result.Index(i)

		for k := 0; k < len(tdb_field_ids); k++ {
			if tdb_field_ids[k] == 0 {
				v.Field(struct_field_ids[k]).SetInt(int64(Db.buf[b]))
			} else {
				value := Db.GetItemValueI(Db.buf[b+int(tdb_field_ids[k])])
				v.Field(struct_field_ids[k]).SetString(value)
			}
		}
	}

	return result.Interface()
}
