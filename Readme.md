# Introduction

> inspired by [sidonia](https://github.com/epsniff/sidonia)

pans 是一款基于内存的检索组件，支持以下特性：

- 遵循 golang 语法的查询 DSL，如：`Age >= 22 && Age < 26 && like( Name.First, "vic.*")`，DSL 支持常用`比较操作符`、`取反`、`函数`以及`括号`，详情如下：
  - [x] 比较操作符：`==` `!=` `>=` `<=` `>` `<=`
  - [x] 函数：所有内置函数都遵循第一个参数传字段名，后续参数传比较值的准则。目前支持的函数有：
    1. `in_array`: 当前字段是否包含数组中的值，与多个 `==` `||` 组合等价。使用示例： `in_array(Age, []int32{12,22,25})`
    2. `like`: 当前字段是否模糊匹配对应值，模糊匹配支持正则表达式。使用示例：`like(Name, "vic.*")`
    3. `括号`: 实现匹配优先级，如： `(Age == 1 || Age == 2) && Name.First!="default"`
    4. `取反`: 即对查询结果取反，如： `!(Age == 1 || Age == 2)`
- 支持索引字段类型包括：`int` `string` `[]int` `[]string` `struct 子字段`，其中
  - [x] `int`/`[]int` 类型支持检索操作有： `==` `!=` `>=` `<=` `>` `<=` 以及函数操作 `in_array`
  - [x] `string` / `[]string` 类型支持检索操作有： `==` `!=` 以及函数操作 `like`
  - [ ] `bool` 待支持，当前可以通过后置过滤实现
  - [ ] `time.Time` 待支持时间类型，当前可以通过后置过滤实现
  - [ ] `float` 待支持浮点数，当前可以通过后置过滤实现
- 索引构建：支持简单传入待构建索引的文档（go struct）列表即可, 对应字段是否开启索引，通过字段 tag 中加 `index:"on"` 即可

## 使用示例

- 结构体设置字段开启索引方式如下如：

  ```golang
  type Cfg struct {
      ID int `json:"auto_id"`
      Age int `json:"age" index:"on"`   // 开启字段索引
      Height int `index:"on"`
      Name    *Name  // 结构体字段，其子字段也支持索引，在子字段设置对应 tag 即可
      Content *[]string `index:"on"`
      Map     map[string]interface{}  // map 字段不支持索引
      Friends []Name  // slice 结构体字段，不支持索引
      Scores []int32 `index:"on"`
  }

  type Name struct {
      First string `index:"on"`  // 结构体子字段支持索引
      Last string `index:"on"`
  }
  ```

- 新建一个索引：

  ```golang
  d1 := Cfg{1, 12, 175, &Name{"grey", "zhu"}, nil, nil, nil, []int32{1, 2, 3}}
  // 声明变量 d2 ... d7
  keys := []string{"1", "2", "3", "4", "5", "6", "7"}
  docs := []interface{}{d1, d2, d3, d4, d5, d6, d7}
  preprocFn := func(in interface{}) (got interface{}) {
  	val := in.(Cfg)
  	// 在对配置进行索引构建之前，进行业务逻辑处理
  	// val.Content = fmt.Sprintf("%s.%s", val.Name.First, val.Name.Last)
  	return val
  }
  idx, err := index.NewIndex(keys, docs, preprocFn)
  ```

- 查询数据：

  ```golang
  results, err := idx.Query(`in_array(Age, []int32{12,22,25}) && Name.Last == "zhu"`,
  	index.WithLess(func(a, b interface{}) bool {  // 对返回结果进行排序、还支持传过滤函数、分页参数等
  		va := a.(Cfg)
  		vb := b.(Cfg)
  		return va.Height < vb.Height
  	}))

  ```
