package index

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"
)

var (
	ErrEOF = errors.New("EOF") // end of paging query, no more results
)

type Index struct {
	docs map[string]interface{} // external doc id ->  data
	raw  []interface{}          // original docs

	index *Segment // index of docs
}

func NewIndex(keys []string, docs []interface{}, preprocFn ...Preprocess) (Index, error) {
	docCnt := int32(len(keys))
	idx := Index{docs: make(map[string]interface{}, docCnt), index: NewSegment(docCnt)}

	err := idx.insertDocs(keys, docs, preprocFn...)
	return idx, err
}

// Query 查询满足条件的数据
//
//	TODO: 阐述查询语法
func (i *Index) Query(query string, opts ...OptionFunc) ([]interface{}, error) {
	res, err := DoQuery(query, i.index)
	if err != nil {
		return nil, err
	}

	return i.GetDocs(res.ExternalDocIDs, opts...)
}

func (i *Index) GetDocs(docIDs []string, opts ...OptionFunc) ([]interface{}, error) {
	opt := NewOptions(opts...)
	// from docid to doc object
	queryResult := make([]interface{}, 0, len(docIDs))
	for _, did := range docIDs {
		doc, ok := i.docs[did]
		if !ok {
			continue
		}

		if opt.filerFn == nil || !opt.filerFn(doc) {
			queryResult = append(queryResult, doc)
		}
	}

	// order by less function
	if opt.lessFn != nil {
		sort.Slice(queryResult, func(i, j int) bool {
			return opt.lessFn(queryResult[i], queryResult[j])
		})
	}

	// paging results
	if opt.from != 0 || opt.size != 0 {
		qrcnt := int32(len(queryResult))
		if opt.from >= qrcnt {
			return []interface{}{}, ErrEOF
		}

		end := opt.from + opt.size
		if end > qrcnt {
			end = qrcnt
		}
		return queryResult[opt.from:end], nil
	}

	return queryResult, nil
}

func (idx *Index) insertDocs(ids []string, docs []interface{}, preprocFn ...Preprocess) error {
	if len(ids) != len(docs) {
		return fmt.Errorf("length not match between ids and documents")
	}
	if len(docs) == 0 {
		return fmt.Errorf("no documents")
	}

	mapping, err := NewMappingByDoc(docs[0])
	if err != nil {
		return err
	}

	now := time.Now()
	idx.raw = make([]interface{}, 0, len(ids))
	docsToInsert := make([]Document, 0, len(docs))
	for i := 0; i < len(ids); i++ {
		key := ids[i]
		doc := docs[i]

		for _, pfn := range preprocFn {
			if pfn == nil {
				continue
			}
			doc = pfn(doc)
		}

		// save raw documents
		idx.raw = append(idx.raw, doc)
		idx.docs[key] = doc

		// parse doc to construct index
		fields, err := mapping.DocWalking(doc)
		if err != nil {
			return fmt.Errorf("doc:%s index failed. err:%s", key, err.Error())
		}

		docsToInsert = append(docsToInsert, NewDocument(key, fields, now))
	}

	return idx.index.IndexDocuments(context.TODO(), docsToInsert)
}

type (
	Options struct {
		// sort options, if `orderby` and `lessFn` are both provided. use `orderby` to sort
		orderby *OrderBy
		lessFn  Less // function to sort, return by ascending order

		// filter options, query results will be filtered by the given filter function
		filerFn Filter

		// page options: paging on query result, return results[from:from+size]
		from int32
		size int32
	}

	Less       func(a, b interface{}) bool
	Filter     func(a interface{}) bool // return true if the given `a` should be filtered from the results
	Preprocess func(in interface{}) (got interface{})

	OrderBy struct {
		FieldName string // order by field specified by FieldName
		Ascend    bool   // true->ascending, false->descending
	}

	OptionFunc func(o *Options)
)

// NewOptions
func NewOptions(opts ...OptionFunc) *Options {
	o := &Options{}

	for _, opt := range opts {
		opt(o)
	}
	return o
}

// WithOrderBy
func WithOrderBy(orderBy *OrderBy) OptionFunc {
	return func(o *Options) {
		o.orderby = orderBy
	}
}

// WithFilter
func WithFilter(filter func(a interface{}) bool) OptionFunc {
	return func(o *Options) {
		o.filerFn = filter
	}
}

// WithLess
func WithLess(less Less) OptionFunc {
	return func(o *Options) {
		o.lessFn = less
	}
}

// WithFrom
func WithFrom(from int32) OptionFunc {
	return func(o *Options) {
		o.from = from
	}
}

// WithSize
func WithSize(size int32) OptionFunc {
	return func(o *Options) {
		o.size = size
	}
}
