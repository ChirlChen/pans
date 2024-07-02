package index

import (
	"fmt"
	"reflect"

	"github.com/araddon/qlbridge/value"
)

// IndexType
type IndexType int32

const (
	IndexTypeInvalid IndexType = 0
	IndexTypeOn      IndexType = 1

	IndexTypeTerm IndexType = 2 + iota
	IndexTypeRange
)

var idxName = map[IndexType]string{
	IndexTypeTerm:    "term",
	IndexTypeRange:   "range",
	IndexTypeInvalid: "invalid",
}

func (i IndexType) String() string {
	if name, ok := idxName[i]; ok {
		return name
	}

	return fmt.Sprintf("unknown index type:%d", i)
}

func NewIndexType(name string) IndexType {
	switch name {
	case "term":
		return IndexTypeTerm
	case "range":
		return IndexTypeRange
	case "on":
		return IndexTypeOn

	}
	return IndexTypeInvalid
}

type Mapping struct {
	m map[string]IndexType
}

func NewMappingByDoc(doc interface{}) (*Mapping, error) {
	mp := &Mapping{
		m: make(map[string]IndexType),
	}

	err := docWalking(mp, doc, "", true, nil)
	return mp, err
}

func (mp *Mapping) DocWalking(doc interface{}) (map[string]value.Value, error) {
	fields := make(map[string]value.Value, len(mp.m))

	err := docWalking(mp, doc, "", false, &fields)
	return fields, err
}

func docWalking(mapping *Mapping, doc interface{}, path FieldPath, mappingInit bool, outFields *map[string]value.Value) error {
	val := reflect.ValueOf(doc)
	if val.IsZero() {
		return nil
	}

	typ := val.Type()
	for {
		if typ.Kind() != reflect.Pointer {
			break
		}

		val = val.Elem()
		typ = val.Type()
	}

	if typ.Kind() != reflect.Struct {
		return fmt.Errorf("type `%s` is not support index", typ.Kind())
	}

	for i := 0; i < val.NumField(); i++ {
		fval := val.Field(i)
		ftyp := typ.Field(i)
		if !fval.CanInterface() {
			return fmt.Errorf("field: `%s` is not exported", ftyp.Name)
		}

		idxTag := ftyp.Tag.Get("index")
		newPath := path.Join(ftyp.Name)
		switch ftyp.Type.Kind() {
		case reflect.String, reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Uint, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			if it, ok := mapping.m[newPath.String()]; ok && it != IndexTypeInvalid {
				(*outFields)[newPath.String()] = value.NewValue(fval.Interface())
			} else {
				checkMapping(mapping, newPath, idxTag, mappingInit)
			}

		case reflect.Struct, reflect.Pointer: // , reflect.Slice, reflect.Array
			err := docWalking(mapping, fval.Interface(), newPath, mappingInit, outFields)
			if err != nil {
				return err
			}
		default:
			if len(idxTag) != 0 {
				return fmt.Errorf("type `%s` is not support index", typ.Kind())
			}
		}
	}

	return nil
}

func checkMapping(mapping *Mapping, path FieldPath, indexTag string, mappingInit bool) {
	if mappingInit && len(indexTag) != 0 {
		mapping.m[string(path)] = NewIndexType(indexTag)
	}
}

type FieldPath string

func (p FieldPath) String() string {
	return string(p)
}

func (p FieldPath) Join(sub string) FieldPath {
	if len(p) != 0 {
		return FieldPath(fmt.Sprintf("%s.%s", p, sub))
	}

	return FieldPath(sub)
}
