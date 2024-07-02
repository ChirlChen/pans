package index_test

import (
	"testing"

	"github.com/bmizerany/assert"
	"github.com/chirlchen/pans/index"
)

type Cfg struct {
	ID     int `json:"auto_id"`
	Age    int `json:"Age" index:"range"`
	Height int `json:"Height" index:"range"`

	Name    *Name
	Content *string `json:"Content"`
	Map     map[string]interface{}
	Friends []Name
}

type Name struct {
	First string `json:"First" index:"term"`
	Last  string `json:"Last" index:"term"`
}

var (
	emptyStr = ""
	keys     = []string{"1", "2", "3", "4", "5", "6", "7"}
	d1       = Cfg{1, 12, 170, &Name{"chirl", "chen"}, &emptyStr, map[string]interface{}{"hello": "world"}, nil}
	d2       = Cfg{2, 12, 175, &Name{"grey", "zhu"}, &emptyStr, nil, nil}
	d3       = Cfg{3, 22, 175, &Name{"vicki", "zhu"}, &emptyStr, nil, nil}
	d4       = Cfg{4, 22, 178, &Name{"vicky", "chu"}, &emptyStr, nil, nil}
	d5       = Cfg{5, 25, 170, &Name{"zhengyu", "chen"}, &emptyStr, nil, nil}
	d6       = Cfg{6, 26, 178, &Name{"zhenhai", "zhu"}, &emptyStr, nil, nil}
	d7       = Cfg{7, 26, 175, &Name{"lucky", "chu"}, &emptyStr, nil, nil}

	docs = []interface{}{
		d1, d2, d3, d4, d5, d6, d7,
	}
	docs1 = []interface{}{
		&d1, &d2, &d3, &d4, &d5, &d6, &d7,
	}
)

func buildIndex(t *testing.T, keys []string, docs []interface{}, preprocFn index.Preprocess) index.Index {
	idx, err := index.NewIndex(keys, docs, preprocFn)

	if err != nil {
		t.Errorf("index.NewIndex() failed: %v", err)
	}

	return idx
}

func TestIndex_Query(t *testing.T) {
	type args struct {
		query string
		opts  []index.OptionFunc
	}
	tests := []struct {
		name    string
		args    args
		want    []interface{}
		wantErr bool
	}{
		{
			name: "query = ",
			args: args{
				query: "Age == 12",
				opts: []index.OptionFunc{
					index.WithFilter(func(a interface{}) bool {
						val := a.(Cfg)
						return val.ID == 1
					}),
				},
			}, want: []interface{}{d2}, wantErr: false,
		},
		{
			name: "query >",
			args: args{
				query: "Age > 22",
				opts: []index.OptionFunc{
					index.WithLess(func(a, b interface{}) bool {
						va := a.(Cfg)
						vb := b.(Cfg)
						return va.Height < vb.Height
					}),
				},
			}, want: []interface{}{d5, d7, d6}, wantErr: false,
		},
		{
			name: "query >= & <",
			args: args{
				query: "Age >= 22 && Age < 26",
			}, want: []interface{}{d3, d4, d5}, wantErr: false,
		},
		{
			name: "query-in-array1",
			args: args{
				query: `in_array(Age, []int32{12,22,25})`,
			}, want: []interface{}{d1, d2, d3, d4, d5}, wantErr: false,
		},
		{
			name: "query-in-array",
			args: args{
				query: `in_array(Age, []int32{12,22,25}) && Name.Last == "zhu"`,
			}, want: []interface{}{d2, d3}, wantErr: false,
		},
		{
			name: "query-like",
			args: args{
				query: `like( Name.First, "vic.*") || in_array(Name.Last, []string{"zhu", "chu"})`,
			}, want: []interface{}{d2, d3, d4, d6, d7}, wantErr: false,
		},
	}
	i := buildIndex(t, keys, docs, func(in interface{}) (got interface{}) {
		val := in.(Cfg)
		// 在对配置进行缓存之前，进行业务逻辑处理
		// val.Content = fmt.Sprintf("%s.%s", val.Name.First, val.Name.Last)
		return val
	})
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := i.Query(tt.args.query, tt.args.opts...)
			if (err != nil) != tt.wantErr {
				t.Errorf("Index.Query() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			assert.Equalf(t, tt.want, got, "Index.Query() got: %v, want: %v", got, tt.want)
		})
	}
}

func TestIndex1_Query(t *testing.T) {
	type args struct {
		query string
		opts  []index.OptionFunc
	}
	tests := []struct {
		name    string
		args    args
		want    []interface{}
		wantErr bool
	}{
		{
			name: "query = ",
			args: args{
				query: "Age == 12",
				opts: []index.OptionFunc{
					index.WithFilter(func(a interface{}) bool {
						val := a.(*Cfg)
						return val.ID == 1
					}),
				},
			}, want: []interface{}{&d2}, wantErr: false,
		},
		{
			name: "query >",
			args: args{
				query: "Age > 22",
				opts: []index.OptionFunc{
					index.WithLess(func(a, b interface{}) bool {
						va := a.(*Cfg)
						vb := b.(*Cfg)
						return va.Height < vb.Height
					}),
				},
			}, want: []interface{}{&d5, &d7, &d6}, wantErr: false,
		},
		{
			name: "query >= & <",
			args: args{
				query: "Age >= 22 && Age < 26",
			}, want: []interface{}{&d3, &d4, &d5}, wantErr: false,
		},
		{
			name: "query-in-array1",
			args: args{
				query: `in_array(Age, []int32{12,22,25})`,
			}, want: []interface{}{&d1, &d2, &d3, &d4, &d5}, wantErr: false,
		},
		{
			name: "query-in-array",
			args: args{
				query: `in_array(Age, []int32{12,22,25}) && Name.Last == "zhu"`,
			}, want: []interface{}{&d2, &d3}, wantErr: false,
		},
		{
			name: "query-like",
			args: args{
				query: `like( Name.First, "vic.*") || in_array(Name.Last, []string{"zhu", "chu"})`,
			}, want: []interface{}{&d2, &d3, &d4, &d6, &d7}, wantErr: false,
		},
	}
	i := buildIndex(t, keys, docs1, func(in interface{}) (got interface{}) {
		val := in.(*Cfg)
		// 在对配置进行缓存之前，进行业务逻辑处理
		// val.Content = fmt.Sprintf("%s.%s", val.Name.First, val.Name.Last)
		return val
	})
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := i.Query(tt.args.query, tt.args.opts...)
			if (err != nil) != tt.wantErr {
				t.Errorf("Index.Query() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			assert.Equalf(t, tt.want, got, "Index.Query() got: %v, want: %v", got, tt.want)
		})
	}
}
