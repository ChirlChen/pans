package index

import (
	"fmt"
	"reflect"

	"github.com/araddon/qlbridge/value"
	"github.com/spf13/cast"
)

type FloatValue = value.NumberValue

type IntSliceValue struct {
	v []int64
}

const (
	IntSliceType value.ValueType = 100
)

func (m IntSliceValue) Nil() bool                    { return m.v == nil }
func (m IntSliceValue) Err() bool                    { return false }
func (m IntSliceValue) Type() value.ValueType        { return IntSliceType }
func (m IntSliceValue) Value() interface{}           { return m.v }
func (m IntSliceValue) Val() []int64                 { return m.v }
func (m IntSliceValue) MarshalJSON() ([]byte, error) { return nil, nil }
func (m IntSliceValue) ToString() string             { return fmt.Sprintf("%v", m.v) }

type StringSliceValue value.StringsValue

func NewSliceValue(rval reflect.Value) value.Value {
	goVal := rval.Interface()
	switch val := goVal.(type) {
	case []string:
		return value.NewStringsValue(val)
	case []int, []int8, []int16, []int32, []int64, []uint, []uint8, []uint16, []uint32, []uint64:
		iarr := make([]int64, 0, rval.Len())
		for i := 0; i < rval.Len(); i++ {
			iarr = append(iarr, cast.ToInt64(rval.Index(i).Interface()))
		}

		return IntSliceValue{v: iarr}

	}
	return value.NewErrorValue(fmt.Errorf("type:%T not supported index", goVal))
}
