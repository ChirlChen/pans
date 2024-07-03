package index

import (
	"context"
	"fmt"
	"go/token"

	"github.com/araddon/gou"
	"github.com/araddon/qlbridge/value"

	"github.com/RoaringBitmap/roaring"
	"github.com/blevesearch/vellum"
	"github.com/blevesearch/vellum/regexp"
)

type Query interface {
	Type() QType
}

type QType int

const (
	TypeRegExQuery QType = 10 // 正则匹配
	TypeTermQuery  QType = 11 // 词项精确匹配

	TypeRangeEQQuery QType = QType(token.EQL) // 范围查询:=
	TypeRangeLEQuery QType = QType(token.LEQ) // 范围查询:<=
	TypeRangeLTQuery QType = QType(token.LSS) // 范围查询:<
	TypeRangeGEQuery QType = QType(token.GEQ) // 范围查询:>=
	TypeRangeGTQuery QType = QType(token.GTR) // 范围查询:>
)

type QueryBuilder struct {
	ctx context.Context
	seg *Segment
	ops func() (*SearchResults, error) // TODO drop SearchResults in favor of using a Roaring bitmap to gather results in.
}

func NewQueryBuilder(ctx context.Context, seg *Segment) *QueryBuilder {
	return &QueryBuilder{ctx, seg, nil}
}

func (q *QueryBuilder) And(queries ...Query) *QueryBuilder {
	return q.op(false, queries...)
}

func (q *QueryBuilder) Or(queries ...Query) *QueryBuilder {
	return q.op(true, queries...)
}

func (q *QueryBuilder) op(or bool, queries ...Query) *QueryBuilder {
	currentOps := q.ops
	op := func() (*SearchResults, error) {
		// TODO and all the queries together.
		res := &SearchResults{roaring.New(), nil}
		firstRun := true
		for _, query := range queries {
			var results *SearchResults
			var err error
			switch query.Type() {
			case TypeRegExQuery:
				tmpq := query.(*RegExTermQuery)
				results, err = q.seg.QueryRegEx(q.ctx, tmpq)

			case TypeTermQuery:
				tmpq := query.(*TermQuery)
				results, err = q.seg.QueryTerm(q.ctx, tmpq)

			case TypeRangeEQQuery, TypeRangeLEQuery, TypeRangeLTQuery, TypeRangeGEQuery, TypeRangeGTQuery:
				tmpq := query.(*RangeQuery)
				results, err = q.seg.QueryRange(q.ctx, tmpq)
			default:
				return nil, fmt.Errorf("unsupported query type")
			}
			if err != nil {
				return nil, err
			}
			if firstRun {
				res.internalDocIds.Or(results.internalDocIds) // Add all of them on the first loop
				firstRun = false
			} else if or { // or
				res.internalDocIds.Or(results.internalDocIds)
			} else { // and
				res.internalDocIds.And(results.internalDocIds)
			}
		}

		// TODO this isn't write, for now we're treating all children as OR statements with this batch of AND blocks
		//      We need to rethink this and consider this and decide of a better way to build an AST ?
		if currentOps != nil {
			// TODO This whole block is all wrong, but I need to revisit it later.
			children, err := currentOps()
			if err != nil {
				return nil, err
			}
			res.internalDocIds.Or(children.internalDocIds)
		}
		return res, nil
	}

	q.ops = op
	return q
}

func (q *QueryBuilder) Run(dry bool) (*SearchResults, error) {
	results, err := q.ops()
	if err != nil {
		gou.Errorf("error running query: err:%v", err)
		return nil, err
	}
	if dry {
		return results, nil
	}

	return results.BuildExternalIDs(q.seg)
}

// GetExternalIDs takes a bitmap of internal ids and converts them to an array of external ids
// extacted from the segment.
func GetExternalIDs(seg *Segment, internalDocIds *roaring.Bitmap) ([]string, error) {
	array := make([]string, internalDocIds.GetCardinality())
	postingIter := internalDocIds.Iterator()
	i := 0
	for postingIter.HasNext() {
		internalDocID := postingIter.Next()
		externalDocID, ok := seg.docIDInternalToExternal[internalDocID]
		if !ok {
			return nil, fmt.Errorf("found an internal docID without an external doc ID mapping: id:%v", internalDocID)
		}
		array[i] = externalDocID
		i++
	}
	return array, nil
}

func NewQuery(qtype QType, field string, val value.Value) (Query, error) {
	switch qtype {
	case TypeRegExQuery:
		if val.Type() == value.StringType {
			return &RegExTermQuery{field, val.Value().(string)}, nil
		} else {
			return nil, fmt.Errorf("filed:`%s` not surport `like` query, only accepts string fields", field)
		}
	case TypeTermQuery:
		if val.Type() == value.StringType {
			return &TermQuery{field, val.Value().(string)}, nil
		} else if val.Type() == value.IntType {
			return &RangeQuery{field, val.Value().(int64), TypeRangeEQQuery}, nil
		} else {
			return nil, fmt.Errorf("filed:`%s` not surport `=` and `in_array` query, only accept int and string fields", field)
		}
	case TypeRangeEQQuery, TypeRangeLEQuery, TypeRangeLTQuery, TypeRangeGEQuery, TypeRangeGTQuery:
		if val.Type() != value.IntType {
			return nil, fmt.Errorf("filed:`%s` not surport `%s` query, only accept int fields", field, token.Token(qtype))
		}
		return &RangeQuery{field, val.Value().(int64), qtype}, nil
	}

	return nil, fmt.Errorf("unsupported query type: %v for field:%s", qtype, field)
}

type SearchResults struct {
	internalDocIds *roaring.Bitmap

	ExternalDocIDs []string
}

type RegExTermQuery struct {
	FieldName string
	RegEx     string
}

func (q *RegExTermQuery) Type() QType {
	return TypeRegExQuery
}
func (seg *Segment) QueryRegEx(ctx context.Context, query *RegExTermQuery) (*SearchResults, error) {
	field := query.FieldName
	regEx := query.RegEx

	var err error
	fieldId, ok := seg.fieldToFieldId[field]
	if !ok {
		return nil, fmt.Errorf("no field-id found for field: %v", field)
	}

	termDictionary, ok := seg.termDicFstCache[fieldId]
	if !ok {
		tbytes, ok := seg.termDicBytes[fieldId]
		if !ok {
			return nil, fmt.Errorf("no term dictionary found for field: %v", field)
		}
		termDictionary, err = vellum.Load(tbytes)
		if err != nil {
			return nil, fmt.Errorf("failed loading term dictionary: err:%v", err)
		}
		seg.termDicFstCache[fieldId] = termDictionary
	}
	//
	// Query the Term Dic
	//
	r, err := regexp.New(regEx)
	if err != nil {
		return nil, err
	}

	var res *SearchResults = &SearchResults{roaring.New(), nil}
	itr, err := termDictionary.Search(r, nil, nil)
	for ; err == nil; err = itr.Next() {
		_, termID := itr.Current()
		postingList := seg.postings[uint32(termID)]
		postings := postingList.Postings()
		res.internalDocIds.Or(postings)
	}

	return res, nil
}

type TermQuery struct {
	FieldName string
	Term      string
}

func (q *TermQuery) Type() QType {
	return TypeTermQuery
}

func (seg *Segment) QueryTerm(ctx context.Context, query *TermQuery) (*SearchResults, error) {
	field := query.FieldName
	term := query.Term

	fieldId, ok := seg.fieldToFieldId[field]
	if !ok {
		return nil, fmt.Errorf("no field-id found for field: %v", field)
	}

	termDictionary, ok := seg.fieldsTermDic[fieldId]
	if !ok {
		return nil, fmt.Errorf("no term dictionary found for field: %v", field)
	}
	//
	// Query the Term Dic
	//
	termID, ok := termDictionary.termToTermID[term]
	var res *SearchResults = &SearchResults{roaring.New(), nil}
	if !ok {
		return res, nil
	}

	postingList := seg.postings[uint32(termID)]
	postings := postingList.Postings()
	res.internalDocIds.Or(postings)

	return res, nil
}

type RangeQuery struct {
	FieldName string
	Num       int64
	qtype     QType
}

func (q *RangeQuery) Type() QType {
	return q.qtype
}

func (seg *Segment) QueryRange(ctx context.Context, query *RangeQuery) (*SearchResults, error) {
	field := query.FieldName
	term := query.Num

	fieldId, ok := seg.fieldToFieldId[field]
	if !ok {
		return nil, fmt.Errorf("no field-id found for field: %v", field)
	}

	fieldPostings, ok := seg.rangePostings[fieldId]
	if !ok {
		return nil, fmt.Errorf("no term dictionary found for field: %v", field)
	}

	var res *SearchResults = &SearchResults{roaring.New(), nil}
	switch query.qtype {
	case TypeRangeEQQuery:
		res.internalDocIds = RangePostingEqual(&fieldPostings, term)
	case TypeRangeGEQuery:
		res.internalDocIds = RangePostingGraterEqual(&fieldPostings, term)
	case TypeRangeGTQuery:
		res.internalDocIds = RangePostingGraterThan(&fieldPostings, term)
	case TypeRangeLEQuery:
		res.internalDocIds = RangePostingLessEqual(&fieldPostings, term)
	case TypeRangeLTQuery:
		res.internalDocIds = RangePostingLessThan(&fieldPostings, term)
	}

	return res, nil
}

func (s *SearchResults) Not(seg *Segment) *SearchResults {
	s.internalDocIds.Xor(seg.fullDocIDBits)
	return s
}

func (s *SearchResults) And(o *SearchResults) *SearchResults {
	s.internalDocIds.And(o.internalDocIds)
	return s
}

func (s *SearchResults) Or(o *SearchResults) *SearchResults {
	s.internalDocIds.Or(o.internalDocIds)
	return s
}

func (s *SearchResults) String() string {
	return s.internalDocIds.String()
}

func (s *SearchResults) BuildExternalIDs(seg *Segment) (*SearchResults, error) {
	array, err := GetExternalIDs(seg, s.internalDocIds)
	if err != nil {
		gou.Errorf("error from GetExternalIDs: err:%v", err)
		return nil, err
	}
	s.ExternalDocIDs = array

	return s, nil
}
