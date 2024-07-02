package index_test

import (
	"context"
	"fmt"
	"hash/fnv"
	"reflect"
	"testing"
	"time"

	"github.com/RoaringBitmap/roaring"
	"github.com/araddon/qlbridge/value"
	"github.com/bmizerany/assert"
	"github.com/chirlchen/pans/index"
)

func TestIndex(t *testing.T) {
	segment := indexDoc(t)
	{ // test case - And regex query for first name kev.* and name.last manning
		res, err := index.NewQueryBuilder(context.TODO(), segment).
			And(&index.RegExTermQuery{"name.first", "kev.*"}, &index.RegExTermQuery{"name.last", "manning"}).
			Run(false)
		if err != nil {
			t.Fatalf("err:%v", err)
		}
		fmt.Printf("found Docs: %v \n", res.ExternalDocIDs)
		assert.Equalf(t, 1, len(res.ExternalDocIDs), "expected only one doc to match `kevin manning`")
	}

}

func indexDoc(t *testing.T) *index.Segment {
	docs := []index.Document{}
	now := time.Now()
	for i := 0; i < 500; i++ {
		fieldvals := map[string]value.Value{}
		fieldvals["doc_id"] = value.NewStringValue(fmt.Sprintf("%000d", i))
		fieldvals["userid"] = value.NewStringValue(hash(fmt.Sprintf("%000d", i)))
		fieldvals["hight"] = value.NewIntValue(160 + int64(i%20))
		fieldvals["age"] = value.NewIntValue(1 + int64(i%50))
		switch {
		case i%100 == 0:
			fieldvals["name.first"] = value.NewStringValue("eric")
		case i%100 == 1:
			fieldvals["name.first"] = value.NewStringValue("kevin")

		case i%100 == 2:
			fieldvals["name.first"] = value.NewStringValue("angela")
		case i%100 == 3:
			fieldvals["name.first"] = value.NewStringValue("jon")
		case i%100 == 4:
			fieldvals["name.first"] = value.NewStringValue("john")
			fieldvals["name.last"] = value.NewStringValue(fmt.Sprintf("smith-%d", i))
		case i%100 == 5:
			fieldvals["name.first"] = value.NewStringValue("james")
		default:
			fieldvals["name.first"] = value.NewStringValue("default")
		}
		if i == 101 || i == 100 {
			fieldvals["name.last"] = value.NewStringValue("manning")
		}
		if i == 102 {
			fieldvals["name.last"] = value.NewStringValue("smith")
		}

		docs = append(docs, index.NewDocument(fmt.Sprintf("doc_number:%000d", i), fieldvals, now))
	}
	segment := index.NewSegment(100)
	err := segment.IndexDocuments(context.TODO(), docs)
	if err != nil {
		t.Fatalf("err:%v", err)
	}

	return segment
}

func TestQuery(t *testing.T) {
	segment := indexDoc(t)

	tests := []struct {
		name string
		expr string
		want *roaring.Bitmap
		err  bool
	}{

		{
			name: "atom-equal",
			expr: `name.first=="eric"`,
			want: roaring.BitmapOf(0, 100, 200, 300, 400)},
		{
			name: "atom-no-equal",
			expr: `name.first!="default"`,
			want: roaring.BitmapOf(0, 1, 2, 3, 4, 5, 100, 101, 102, 103, 104, 105, 200, 201, 202, 203, 204, 205, 300, 301, 302, 303, 304, 305, 400, 401, 402, 403, 404, 405)},
		{
			name: "atom-in_array",
			expr: `in_array(name.last, []string{"smith", "smith-4", "smith-104"})`,
			want: roaring.BitmapOf(4, 102, 104)},
		{
			name: "atom-like",
			expr: `like(name.last, "smit.*")`,
			want: roaring.BitmapOf(4, 102, 104, 204, 304, 404)},
		{
			name: "or-express",
			expr: `like(name.last, "smit.*") || (name.first=="eric")`,
			want: roaring.BitmapOf(0, 4, 100, 102, 104, 200, 204, 300, 304, 400, 404)},
		{
			name: "and-express",
			expr: `name.first=="eric" && name.last=="manning"`,
			want: roaring.BitmapOf(100)},
		{
			name: "not-express",
			expr: `!(name.first=="eric" && name.last=="manning") && name.first!="default"`,
			want: roaring.BitmapOf(0, 1, 2, 3, 4, 5, 101, 102, 103, 104, 105, 200, 201, 202, 203, 204, 205, 300, 301, 302, 303, 304, 305, 400, 401, 402, 403, 404, 405)},
		{
			name: "range-eq-search",
			expr: `age==20`,
			want: roaring.BitmapOf(19, 69, 119, 169, 219, 269, 319, 369, 419, 469)},
		{
			name: "range-le-search",
			expr: `age<=2`,
			want: roaring.BitmapOf(0, 1, 50, 51, 100, 101, 150, 151, 200, 201, 250, 251, 300, 301, 350, 351, 400, 401, 450, 451)},
		{
			name: "range-lt-search",
			expr: `age<2`,
			want: roaring.BitmapOf(0, 50, 100, 150, 200, 250, 300, 350, 400, 450)},
		{
			name: "range-ge-search",
			expr: `age>=49`,
			want: roaring.BitmapOf(48, 49, 98, 99, 148, 149, 198, 199, 248, 249, 298, 299, 348, 349, 398, 399, 448, 449, 498, 499)},
		{
			name: "range-gt-search",
			expr: `age>49`,
			want: roaring.BitmapOf(49, 99, 149, 199, 249, 299, 349, 399, 449, 499)},
		{
			name: "range-ge-search",
			expr: `age >= 20 && age < 22`,
			want: roaring.BitmapOf(19, 20, 69, 70, 119, 120, 169, 170, 219, 220, 269, 270, 319, 320, 369, 370, 419, 420, 469, 470)},
		{
			name: "range-ge-search",
			expr: `in_array(age, []int{20, 21})`,
			want: roaring.BitmapOf(19, 20, 69, 70, 119, 120, 169, 170, 219, 220, 269, 270, 319, 320, 369, 370, 419, 420, 469, 470)},
		{
			name: "range-ge-search",
			expr: `(age == 1 || age == 2) && name.first!="default"`,
			want: roaring.BitmapOf(0, 1, 100, 101, 200, 201, 300, 301, 400, 401)},
		{name: "num-like-err", expr: `like(age, 22)`, err: true, want: roaring.BitmapOf()},
		{name: "str-eq-err", expr: `age>=1.5`, err: true, want: roaring.BitmapOf()},
		{name: "str-range-err", expr: `name.first > "eric"`, err: true, want: roaring.BitmapOf()},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got, err := index.DoQuery(tt.expr, segment); (err != nil) != tt.err || (!tt.err && !reflect.DeepEqual(got.String(), tt.want.String())) {
				t.Errorf("DoQuery(%s) failed, want=%s, got=%s, err=%v", tt.name, tt.want, got, err)
			}
		})
	}
	// res, err := index.DoQuery(`name.first=="eric" && in_array(name.last, []string{"manning", "smith*"})`, segment)
	// t.Errorf("res:%s, err:%v", res, err)
}

func TestXor(t *testing.T) {
	a := roaring.BitmapOf([]uint32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11}...)
	b := roaring.BitmapOf([]uint32{1, 3, 6}...)
	c := roaring.BitmapOf([]uint32{3, 6, 9, 11}...)
	b.Xor(a)
	c.Xor(a)
	t.Errorf("\nb:%s\nc:%s", b.String(), c.String())
}

func hash(s string) string {
	h := fnv.New64() // FNV hash name to int
	h.Write([]byte(s))
	key := h.Sum64()
	return fmt.Sprintf("%v", key)
}
