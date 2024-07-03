package index

import (
	"bytes"
	"context"
	"fmt"
	"sort"

	"github.com/RoaringBitmap/roaring"
	"github.com/araddon/qlbridge/value"
	"github.com/blevesearch/vellum"
)

type Segment struct {
	// field ID
	fieldIdInt     uint32
	fieldToFieldId map[string]uint32 // external-FieldID --> internal-FieldID

	// fileds for term index： suport regex match and term macth
	termIdInc       uint32                     // term FieldID to Term Dic
	termDicBytes    map[uint32][]byte          // internal-FieldId --> (bytes) TermDic (FST( Term -- > TermID ))
	termDicFstCache map[uint32]*vellum.FST     // for regex matching
	fieldsTermDic   IndexableFields            // for term matching
	postings        map[uint32]TermPostingList // termID --> list of doc Ids // TODO replace with roaring bitmaps...

	// for range index
	rangePostings map[uint32]RangePostingList // fieldID --> btree(numeric --> posting list)

	fullDocIDBits *roaring.Bitmap // store all doc IDs， using to handle not expression

	// docid to doc
	docIdInc                uint32
	docIDInternalToExternal map[uint32]string
	docIDExternalToInternal map[string]uint32
}

func NewSegment(n int32) *Segment {
	return &Segment{
		fieldsTermDic:  make(IndexableFields),
		fieldToFieldId: make(map[string]uint32, 10),
		// fieldIdToTermDicBuilder: map[uint32]*vellum.Builder{},
		termDicBytes:            make(map[uint32][]byte, n),
		postings:                make(map[uint32]TermPostingList, n),
		rangePostings:           make(map[uint32]RangePostingList, 5),
		docIDInternalToExternal: make(map[uint32]string, n),
		docIDExternalToInternal: make(map[string]uint32, n),
		fullDocIDBits:           roaring.New(),

		termDicFstCache: make(map[uint32]*vellum.FST, n),
	}
}

// IndexDocuments All documents must be processed at once
func (seg *Segment) IndexDocuments(ctx context.Context, docs []Document) error {
	// TODO for performance pass in a count of docs * fields so we can presize the array?
	// TODO is it faster to count the terms first, so the array can be an exact size
	// seg.fieldsTermDic = make(IndexableFields)
	var err error
	for _, doc := range docs {
		// TODO For performance we'll assume that each docId is unique?  That way we can just increment
		//      the docIdInc counter on each doc without checking if the doc already exists.
		inDocID := uint32(0) // internal document id
		if did, ok := seg.docIDExternalToInternal[doc.ID()]; ok {
			inDocID = did
		} else {
			inDocID = seg.docIdInc
			seg.docIDExternalToInternal[doc.ID()] = inDocID
			seg.docIDInternalToExternal[inDocID] = doc.ID()
			// fmt.Println(doc.DocID)
			seg.docIdInc++
		}

		seg.fullDocIDBits.Add(inDocID)
		for field, fieldTerm := range doc.Row() {
			if fieldTerm.Nil() {
				// TODO log and continue
				continue
			}
			if fieldTerm.Err() {
				// TODO log and continue
				continue
			}
			// TODO add a mappings setting for the index, and look up the field's mappings
			//      to ensure that the term type match's the mapping type.
			switch fieldTerm.Type() {
			case value.StringType:
				seg.processStringTerm(seg.fieldsTermDic, inDocID, field, fieldTerm.Value().(string))
			case value.IntType: // 整数
				seg.processNumberFields(inDocID, field, fieldTerm.Value().(int64))
			case value.StringsType: //
				vals := fieldTerm.Value().([]string)
				for _, term := range vals {
					seg.processStringTerm(seg.fieldsTermDic, inDocID, field, term)
				}
			case IntSliceType:
				vals := fieldTerm.Value().([]int64)
				for _, term := range vals {
					seg.processNumberFields(inDocID, field, term)
				}
			// case value.NumberType: // 浮点数
			// case value.BoolType:
			// case value.TimeType: //
			default:
				err = fmt.Errorf("type %v isn't currently supported", fieldTerm.Type())
				continue
			}
		}
	}

	//
	// Build the Term Dictionary, using an FST (vellum) for string types
	//
	// f, err := os.Create("/tmp/term.test.dic")
	// if err != nil {
	// 	return nil, err
	// }

	mkFst := func() (*vellum.Builder, *bytes.Buffer, error) {
		buff := bytes.NewBuffer([]byte{})
		var vellumOptions *vellum.BuilderOpts
		dic, err := vellum.New(buff, vellumOptions)
		if err != nil {
			return nil, nil, err
		}
		return dic, buff, nil
	}

	for _, field := range seg.fieldsTermDic {
		sort.Sort(field.Terms)

		// TODO lets stop saving the map of term dics builders on index and
		// save them onto the IndexableField struct

		fst, buff, err := mkFst()
		if err != nil {
			return fmt.Errorf("failed to create FST builder: %v", err)
		}
		for _, term := range field.Terms {
			err := fst.Insert([]byte(term.Term), uint64(term.TermID))
			if err != nil {
				return err
			}
		}

		if err := fst.Close(); err != nil {
			return fmt.Errorf("vellum close failed:%v", err)
		}
		seg.termDicBytes[field.FieldID] = buff.Bytes()
		field.Terms = nil // 清理内存
	}

	return err
}

func (seg *Segment) fieldID(field string) uint32 {
	if fid, ok := seg.fieldToFieldId[field]; ok {
		return fid
	} else {
		fid = seg.fieldIdInt
		seg.fieldToFieldId[field] = fid
		seg.fieldIdInt++
		return fid
	}
}

// processStringTerm processes value.Values of type string, if the wrong type is passed in then we'll get a panic
func (seg *Segment) processStringTerm(fields IndexableFields, inDocID uint32, field string, term string) {
	fieldID := seg.fieldID(field)

	// TODO is this the best way to index strutured data ?
	iField, ok := fields[fieldID]
	if !ok {
		iField = NewIndexableField(field, fieldID)
		fields[fieldID] = iField
	}

	// Term ids are uniq to this instance (aka construction of) a segment.  They are not uniq across segments.
	termID := uint32(0)
	if tid, ok := iField.termToTermID[term]; ok {
		termID = tid
	} else {
		// termID = iField.terminIdInt
		termID = seg.termIdInc
		iField.termToTermID[term] = termID
		// iField.terminIdInt++
		seg.termIdInc++
	}

	iField.Terms = append(iField.Terms, &Term{Term: term, TermID: termID, InternalDocId: inDocID})

	// fields = append(fields, &IndexableField{InternalDocId: docID, FieldID: fieldID, Term: term, TermID: termID})
	if _, ok := seg.postings[termID]; ok {
		postingList := seg.postings[termID]
		postingList.Postings().Add(inDocID)
		postingList.TermFrequency++
	} else {
		list := TermPostingList{1, roaring.New()}
		list.Postings().Add(inDocID)
		seg.postings[termID] = list
	}
}

func (seg *Segment) processNumberFields(inDocID uint32, field string, term int64) {
	fieldID := seg.fieldID(field)

	// fields = append(fields, &IndexableField{InternalDocId: docID, FieldID: fieldID, Term: term, TermID: termID})
	if _, ok := seg.rangePostings[fieldID]; ok {
		filedPostings := seg.rangePostings[fieldID]
		RangePostingAdd(&filedPostings, term, inDocID)
	} else {
		filedPostings := NewRangePostingList()
		RangePostingAdd(&filedPostings, term, inDocID)
		seg.rangePostings[fieldID] = filedPostings
	}
}
