package index

import (
	"fmt"
	"reflect"

	"github.com/RoaringBitmap/roaring"
	"github.com/tidwall/btree"
)

type indexPostings struct {
	termToPostings map[int][]int
}

type TermPostingList struct {
	TermFrequency uint32
	postings      *roaring.Bitmap
}

func (p TermPostingList) Postings() *roaring.Bitmap {
	return p.postings
}

type RangePostingList struct {
	rangePosting btree.BTreeG[Item]
	numberKind   reflect.Kind
}

func NewRangePostingList() RangePostingList {
	return RangePostingList{
		rangePosting: *btree.NewBTreeG(bTreeLess),
	}
}

func RangePostingEqual[T numeric](rp *RangePostingList, num T) *roaring.Bitmap {
	item := Item{numeric: num, kind: reflect.TypeOf(num).Kind()}
	return rp.Equal(item)
}

func RangePostingLessThan[T numeric](rp *RangePostingList, num T) *roaring.Bitmap {
	item := Item{numeric: num, kind: reflect.TypeOf(num).Kind()}
	return rp.LessThan(item)
}

func RangePostingLessEqual[T numeric](rp *RangePostingList, num T) *roaring.Bitmap {
	item := Item{numeric: num, kind: reflect.TypeOf(num).Kind()}
	return rp.LessEqual(item)
}

func RangePostingGraterThan[T numeric](rp *RangePostingList, num T) *roaring.Bitmap {
	item := Item{numeric: num, kind: reflect.TypeOf(num).Kind()}
	return rp.GraterThan(item)
}

func RangePostingGraterEqual[T numeric](rp *RangePostingList, num T) *roaring.Bitmap {
	item := Item{numeric: num, kind: reflect.TypeOf(num).Kind()}
	return rp.GraterEqual(item)
}

func RangePostingAdd[T numeric](rp *RangePostingList, num T, docid uint32) error {
	return rp.Add(num, docid)
}

func (r *RangePostingList) Add(num interface{}, docid uint32) error {
	inKind := reflect.TypeOf(num).Kind()
	if inKind != reflect.Int64 && inKind != reflect.Float64 {
		return fmt.Errorf("num must be an int64 or float64, got %T", num)
	}

	if r.numberKind == reflect.Invalid {
		r.numberKind = inKind
	} else if inKind != r.numberKind {
		return fmt.Errorf("num:%v type mismatch. expected:%s, got:%T", num, r.numberKind, num)
	}

	r.add(Item{numeric: num, kind: r.numberKind}, docid)
	return nil
}

func (r *RangePostingList) add(item Item, docid uint32) {
	index, ok := r.rangePosting.Get(item)
	if !ok {
		index = item
		index.postings = roaring.New()
	}

	index.postings.Add(docid)
	r.rangePosting.Set(index)
}

// Equal
func (r *RangePostingList) Equal(num Item) *roaring.Bitmap {
	item, ok := r.rangePosting.Get(num)
	if !ok {
		return roaring.New()
	}

	return item.postings
}

func (r *RangePostingList) LessThan(num Item) *roaring.Bitmap {
	return r.numericRange(num, true, false)
}

func (r *RangePostingList) LessEqual(num Item) *roaring.Bitmap {
	return r.numericRange(num, true, true)
}

func (r *RangePostingList) GraterThan(num Item) *roaring.Bitmap {
	return r.numericRange(num, false, false)
}

func (r *RangePostingList) GraterEqual(num Item) *roaring.Bitmap {
	return r.numericRange(num, false, true)
}

func (r RangePostingList) numericRange(num Item, lt, includeNum bool) *roaring.Bitmap {
	posting := roaring.New()
	iter := func(item Item) bool {
		if !includeNum && bTreeEqual(num, item) { // skip pivot number
			return true
		}
		posting.Or(item.postings)
		return true
	}

	if lt {
		r.rangePosting.Descend(num, iter)
	} else {
		r.rangePosting.Ascend(num, iter)
	}

	return posting
}

type Item struct {
	numeric  interface{} // int64 float64
	kind     reflect.Kind
	postings *roaring.Bitmap
}

// bTreeLess is a comparison function that compares item keys and returns true
// when a is less than b.
func bTreeLess(a, b Item) bool {
	switch a.kind {
	case reflect.Float64:
		return a.numeric.(float64) < b.numeric.(float64)
	case reflect.Int64:
		return a.numeric.(int64) < b.numeric.(int64)
	}

	return false
}

func bTreeEqual(a, b Item) bool {
	switch a.kind {
	case reflect.Float64:
		return a.numeric.(float64) == b.numeric.(float64)
	case reflect.Int64:
		return a.numeric.(int64) == b.numeric.(int64)
	}
	return false
}

type numeric interface {
	int64 | float64
}
