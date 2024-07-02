package index

import (
	"context"
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"strconv"
	"strings"

	"github.com/araddon/qlbridge/value"
)

func init() {
	funcNameMap = map[string]qFunc{
		"in_array": inArray,
		"like":     like,
	}
}

// DoQuery parse query to ast and do the query
func DoQuery(query string, seg *Segment) (*SearchResults, error) {
	qryExpr, err := parser.ParseExpr(query)
	if err != nil {
		return nil, err
	}

	res, err := qeval(qryExpr, seg)
	if err != nil {
		return nil, err
	}

	return res.BuildExternalIDs(seg)
}

func qeval(expr ast.Expr, seg *Segment) (*SearchResults, error) {
	switch expr := expr.(type) {
	case *ast.BinaryExpr: // binary expression
		op := expr.Op
		switch op {
		case token.LAND, token.LOR: // && ||
			xres, xerr := qeval(expr.X, seg)
			yres, yerr := qeval(expr.Y, seg)
			if xerr != nil || yerr != nil {
				return nil, fmt.Errorf("eval expression: %+v failed. xerr:%v, yerr:%v", expr, xerr, yerr)
			}

			switch op {
			case token.LAND:
				return xres.And(yres), nil
			case token.LOR:
				return xres.Or(yres), nil
			}
		case token.EQL, token.NEQ, token.LSS, token.LEQ, token.GEQ, token.GTR: // == != using tag index
			ident, err := parseIdent(expr.X)
			if err != nil {
				return nil, err
			}

			lit, err := parseBasicLit(expr.Y)
			if err != nil {
				return nil, fmt.Errorf("`%s` expression: %s", op, err)
			}

			switch op {
			case token.EQL, token.NEQ: // == != using tag index
				query, err := NewQuery(TypeTermQuery, ident, lit)
				if err != nil {
					return nil, err
				}

				qres, err := NewQueryBuilder(context.TODO(), seg).And(query).Run(true)
				if err != nil {
					return qres, err
				}

				if op == token.NEQ { // not equal
					qres.Not(seg)
				}

				return qres, nil
			case token.LSS, token.LEQ, token.GEQ, token.GTR: // < <=  >= > using range index
				query, err := NewQuery(QType(op), ident, lit)
				if err != nil {
					return nil, err
				}

				return NewQueryBuilder(context.TODO(), seg).And(query).Run(true)
			}

		default:
			return nil, fmt.Errorf("operator:%s not implemented", op)
		}

	case *ast.CallExpr: // function call
		return calculateForFunc(expr.Fun.(*ast.Ident).Name, expr.Args, seg)
	case *ast.ParenExpr:
		return qeval(expr.X, seg)
	case *ast.UnaryExpr:
		xres, err := qeval(expr.X, seg)
		if xres == nil || err != nil {
			return nil, fmt.Errorf("%+v is nil", expr.X)
		}
		op := expr.Op
		switch op {
		case token.NOT:
			return xres.Not(seg), nil
		}
		return nil, fmt.Errorf("%x type is not support", expr)

	default:
	}
	return nil, fmt.Errorf("%x type is not support", expr)
}

func parseIdent(expr ast.Expr) (string, error) {
	switch expr := expr.(type) {
	case *ast.Ident:
		return expr.Name, nil
	case *ast.SelectorExpr:
		ident := strings.Builder{}
		chrilIdent, err := parseIdent(expr.X)
		if err != nil {
			return "", err
		}
		ident.Grow(len(chrilIdent) + len(expr.Sel.Name) + 1)
		ident.WriteString(chrilIdent)
		ident.WriteString(".")
		ident.WriteString(expr.Sel.Name)
		return ident.String(), nil
	default:
		return "", fmt.Errorf("expr must be a *ast.Ident, got %x", expr)
	}
}

func parseBasicLit(expr ast.Expr) (value.Value, error) {
	switch expr := expr.(type) {
	case *ast.BasicLit:
		switch expr.Kind {
		case token.INT:
			num, err := strconv.ParseInt(expr.Value, 10, 64)
			if err != nil {
				return value.NilValueVal, err
			}
			return value.NewIntValue(num), nil //
		case token.STRING:
			str, err := strconv.Unquote(expr.Value)
			return value.NewStringValue(str), err
		default:
			return value.NilValueVal, fmt.Errorf("unsupport type:%s", expr.Kind)
		}
	default:
		return value.NilValueVal, fmt.Errorf("expr must be a *ast.BasicLit, got %x", expr)
	}
}

// calculateForFunc 计算函数表达式
func calculateForFunc(funcName string, args []ast.Expr, seg *Segment) (*SearchResults, error) {
	// 根据funcName分发逻辑
	handler, ok := funcNameMap[funcName]
	if !ok {
		return nil, fmt.Errorf("func:%s not support", funcName)
	}
	return handler(args, seg)
}

// 注册可执行函数
var funcNameMap = map[string]qFunc{}

type qFunc func(args []ast.Expr, seg *Segment) (*SearchResults, error)

// RegisterFunc 用户可注册自定义条件判断函数。对应函数返回值，如果输入参数导致程序发生错误，则返回 error，如果能正常判断则返回 true/false
func RegisterFunc(name string, fun qFunc) error {
	if _, ok := funcNameMap[name]; ok {
		return fmt.Errorf("func %s() already registered", name)
	}

	funcNameMap[name] = fun
	return nil
}

// inArray 判断变量是否存在在数组中,
//
//	函数调用语法：in_array(location, []string{"南山", "福田"})
//	 - 其中第一个参数为变量名，第二个参数为 golang slice, 支持 []int32/64/uint...{} \ []string{}
func inArray(args []ast.Expr, seg *Segment) (*SearchResults, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf(`func in_array: expected 2 arguments, example: in_array(name, []string{"chirl", "minute"})`)
	}

	ident, err := parseIdent(args[0])
	if err != nil {
		return nil, err
	}
	vRange, ok := args[1].(*ast.CompositeLit)
	if !ok {
		return nil, errors.New("func in_array 2ed params is not a composite lit")
	}

	// 规则表达式中数组里的元素
	queries := make([]Query, 0, len(vRange.Elts))
	for _, p := range vRange.Elts {
		elt, err := parseBasicLit(p)
		if err != nil {
			return nil, err
		}

		q, err := NewQuery(TypeTermQuery, ident, elt)
		if err != nil {
			return nil, err
		}
		queries = append(queries, q)
	}

	return NewQueryBuilder(context.TODO(), seg).Or(queries...).Run(true)
}

func like(args []ast.Expr, seg *Segment) (*SearchResults, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf(`func like: expected 2 arguments, example: in_array(name, []string{"chirl", "minute"})`)
	}
	ident, err := parseIdent(args[0])
	if err != nil {
		return nil, err
	}
	elt, err := parseBasicLit(args[1])
	if err != nil {
		return nil, err
	}
	query, err := NewQuery(TypeRegExQuery, ident, elt)
	if err != nil {
		return nil, err
	}

	return NewQueryBuilder(context.TODO(), seg).Or(query).Run(true)
}
