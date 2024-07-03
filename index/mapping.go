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

	err := docWalking(mp, doc, "", true, "", nil)
	return mp, err
}

func (mp *Mapping) DocWalking(doc interface{}) (map[string]value.Value, error) {
	fields := make(map[string]value.Value, len(mp.m))

	err := docWalking(mp, doc, "", false, "", &fields)
	return fields, err
}

func docWalking(mapping *Mapping, doc interface{}, path FieldPath, mappingInit bool, idxTag string, outFields *map[string]value.Value) error {
	if doc == nil {
		return nil
	}

	fv, ok := doc.(FieldValuer)
	if ok {
		doc = fv.GetValue()
	}

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

	switch typ.Kind() {
	case reflect.Struct:
		for i := 0; i < val.NumField(); i++ {
			fval := val.Field(i)
			ftyp := typ.Field(i)
			if !fval.CanInterface() {
				if idxTag != "" {
					return fmt.Errorf("field: `%s` is not exported", ftyp.Name)
				}
				continue
			}

			tmpTag := func() string {
				tag := ftyp.Tag.Get("index")
				if len(tag) > 0 {
					return tag
				}

				return idxTag
			}()

			newPath := path.Join(ftyp.Name)
			err := docWalking(mapping, fval.Interface(), newPath, mappingInit, tmpTag, outFields)
			if err != nil {
				return err
			}
		}
	case reflect.String, reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Uint, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Slice, reflect.Array:
		if it, ok := mapping.m[path.String()]; ok && it != IndexTypeInvalid {
			switch typ.Kind() {
			case reflect.Slice, reflect.Array:
				sval := NewSliceValue(val)
				if err, ok := sval.(value.ErrorValue); ok {
					return err.Val()
				}

				(*outFields)[path.String()] = sval
			default:
				(*outFields)[path.String()] = value.NewValue(val.Interface())
			}
		} else {
			checkMapping(mapping, path, idxTag, mappingInit)
		}
	default: // reflect.Chan, reflect.Func, reflect.Map, reflect.Float*
		if len(idxTag) != 0 {
			return fmt.Errorf("type `%s` is not support index", typ.Kind())
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

// FieldValuer 支持对文档字段索引构建的值，进行加工返回，实现了该接口的字段，会以本接口返回的值作为字段构建索引的值
//
//	比如：时间字段 time.Time, 如果按照默认字段递归构建的话，会解析到 time.Time 内部字段，一一处理，通过实现 FieldValuer 接口后，比如返回对应时间戳, 则该字段就按照 int 类型构建索引，而不会递归解析 time.Time 内部字段
type FieldValuer interface {
	GetValue() interface{}
}
