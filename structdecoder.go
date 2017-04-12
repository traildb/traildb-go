package tdb

/*
#cgo pkg-config: traildb

#include <traildb.h>
#include <stdlib.h>

*/
import "C"

import (
	"reflect"
)

type TrailDecoder struct {
	struct_field_ids []int
	tdb_field_ids    []uint64
	interned_values  map[C.tdb_item]string
	db               *TrailDB
	prepared_type	 reflect.Type
}

func (this *TrailDecoder) Prepare(t reflect.Type) {
	this.struct_field_ids = make([]int, 0)
	this.tdb_field_ids = make([]uint64, 0)

	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		field_name := field.Tag.Get("tdb")
		if field_name != "" {
			tdb_id, err := this.db.GetField(field_name)
			if err == nil {
				this.struct_field_ids = append(this.struct_field_ids, i)
				this.tdb_field_ids = append(this.tdb_field_ids, tdb_id)
			} else {
				if field_name == "timestamp" {
					this.struct_field_ids = append(this.struct_field_ids, i)
					this.tdb_field_ids = append(this.tdb_field_ids, 0)
				}
			}
		}
	}
	this.prepared_type = t
}

// Create a TrailDecoder object. Its purpose it to cache information about
// mapping traildb events to struct fields, passed to Decode() calls.
func NewDecoder(Db *TrailDB) (res *TrailDecoder) {
	res = &TrailDecoder{}

	res.interned_values = make(map[C.tdb_item]string)
	res.db = Db
	res.prepared_type = nil

	return res;
}

func (this *TrailDecoder) GetItemValueI(evt *Event, item C.tdb_item) string {
	res, ok := this.interned_values[item]
	if ok {
		return res
	} else {
		var vlength C.uint64_t
		itemValue := C.tdb_get_item_value(this.db.db, item, &vlength)
		value := C.GoStringN(itemValue, C.int(vlength))

		this.interned_values[item] = value
		return value
	}
}

// Decode an event. Pass a pointer to an annotated structure as `out`.
//
// Since reflection information about the structure is cached internally, it
// is highly recommended that you always pass structures of the same type as
// out, or have a separate decoder object per structure type.
//
// Also, note that decoder does string interning for field values; decoder
// stores interned string table in memory. In some cases entire traildb
// lexicon may end up in that table.
func (this *TrailDecoder) Decode(evt *Event, out interface{}) {
	v := reflect.Indirect(reflect.ValueOf(out))

	if v.Type() != this.prepared_type {
		this.Prepare(v.Type())
	}

	v.Field(0).SetInt(int64(evt.Timestamp))
	for k := 1; k < len(this.tdb_field_ids); k++ {
		value := this.GetItemValueI(evt, evt.items[this.tdb_field_ids[k]-1])
		v.Field(int(this.struct_field_ids[k])).SetString(value)
	}
}
