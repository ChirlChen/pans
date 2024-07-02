package index_test

import (
	"reflect"
	"testing"

	"github.com/RoaringBitmap/roaring"
	"github.com/chirlchen/pans/index"
)

func buildRangePosting(kind reflect.Kind) *index.RangePostingList {
	posting := index.NewRangePostingList()
	docid := uint32(0)
	for i := int32(0); i < 20; i++ {
		// num := rand.Intn(10) + i*10
		for j := 0; j < 10; j++ {
			index.RangePostingAdd(&posting, int64(i), docid)
			docid++
		}
	}

	return &posting
}

func TestNewRangePostingList(t *testing.T) {
	intPosting := buildRangePosting(reflect.Int64)
	tests := []struct {
		name    string
		num     int64
		posting *index.RangePostingList

		eqWant       *roaring.Bitmap
		lessWant     *roaring.Bitmap
		lessEqWant   *roaring.Bitmap
		graterWant   *roaring.Bitmap
		graterEqWant *roaring.Bitmap
	}{
		// TODO: Add test cases.
		{name: "int-test", num: 10, posting: intPosting,
			eqWant:       roaring.BitmapOf([]uint32{100, 101, 102, 103, 104, 105, 106, 107, 108, 109}...),
			graterWant:   roaring.BitmapOf([]uint32{110, 111, 112, 113, 114, 115, 116, 117, 118, 119, 120, 121, 122, 123, 124, 125, 126, 127, 128, 129, 130, 131, 132, 133, 134, 135, 136, 137, 138, 139, 140, 141, 142, 143, 144, 145, 146, 147, 148, 149, 150, 151, 152, 153, 154, 155, 156, 157, 158, 159, 160, 161, 162, 163, 164, 165, 166, 167, 168, 169, 170, 171, 172, 173, 174, 175, 176, 177, 178, 179, 180, 181, 182, 183, 184, 185, 186, 187, 188, 189, 190, 191, 192, 193, 194, 195, 196, 197, 198, 199}...),
			graterEqWant: roaring.BitmapOf([]uint32{100, 101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111, 112, 113, 114, 115, 116, 117, 118, 119, 120, 121, 122, 123, 124, 125, 126, 127, 128, 129, 130, 131, 132, 133, 134, 135, 136, 137, 138, 139, 140, 141, 142, 143, 144, 145, 146, 147, 148, 149, 150, 151, 152, 153, 154, 155, 156, 157, 158, 159, 160, 161, 162, 163, 164, 165, 166, 167, 168, 169, 170, 171, 172, 173, 174, 175, 176, 177, 178, 179, 180, 181, 182, 183, 184, 185, 186, 187, 188, 189, 190, 191, 192, 193, 194, 195, 196, 197, 198, 199}...),
			lessWant:     roaring.BitmapOf([]uint32{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 78, 79, 80, 81, 82, 83, 84, 85, 86, 87, 88, 89, 90, 91, 92, 93, 94, 95, 96, 97, 98, 99}...),
			lessEqWant:   roaring.BitmapOf([]uint32{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 78, 79, 80, 81, 82, 83, 84, 85, 86, 87, 88, 89, 90, 91, 92, 93, 94, 95, 96, 97, 98, 99, 100, 101, 102, 103, 104, 105, 106, 107, 108, 109}...),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := index.RangePostingEqual(tt.posting, tt.num); !reflect.DeepEqual(got.String(), tt.eqWant.String()) {
				t.Errorf("equal(%d) not equal. got: %v, want: %v", tt.num, got, tt.eqWant)
			}
			if got := index.RangePostingGraterThan(tt.posting, tt.num); !reflect.DeepEqual(got.String(), tt.graterWant.String()) {
				t.Errorf("GraterThan(%d) not equal. got: %v, want: %v", tt.num, got, tt.graterWant)
			}
			if got := index.RangePostingGraterEqual(tt.posting, tt.num); !reflect.DeepEqual(got.String(), tt.graterEqWant.String()) {
				t.Errorf("GraterEqual(%d) not equal. got: %v, want: %v", tt.num, got, tt.graterEqWant)
			}
			if got := index.RangePostingLessThan(tt.posting, tt.num); !reflect.DeepEqual(got.String(), tt.lessWant.String()) {
				t.Errorf("LessThan(%d) not equal. got: %v, want: %v", tt.num, got, tt.lessWant)
			}
			if got := index.RangePostingLessEqual(tt.posting, tt.num); !reflect.DeepEqual(got.String(), tt.lessEqWant.String()) {
				t.Errorf("LessEqual(%d) not equal. got: %v, want: %v", tt.num, got, tt.lessEqWant)
			}
		})
	}
}
