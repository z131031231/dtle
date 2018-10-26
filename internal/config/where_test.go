package config

import (
	"github.com/actiontech/dtle/internal/config/mysql"
	"math"
	"testing"
)

func newTableContextWithWhere(t *testing.T,
	schemaName string, tableName string, where string, columnNames ...string) *TableContext {

	table := NewTable(schemaName, tableName)
	table.OriginalTableColumns = mysql.NewColumnList(mysql.NewColumns(columnNames))
	whereCtx, err := NewWhereCtx(where, table)
	if err != nil {
		t.Fatal(err)
	}
	tbCtx := NewTableContext(table, whereCtx)
	return tbCtx
}
func buildColumnValues(vals ...interface{}) *mysql.ColumnValues {
	return mysql.ToColumnValues(vals)
}

func TestWhereTrue(t *testing.T) {
	var tbCtx *TableContext

	tbCtx = newTableContextWithWhere(t, "db1", "tb1", "a % 2 = 0", "id", "a")
	for i := 0; i < 1000; i++ {
		r, err := tbCtx.WhereTrue(buildColumnValues(i, i))
		if err != nil {
			t.Fatal(err)
		}
		if r != (i % 2 == 0) {
			t.Fatalf("i: %v, r: %v", i, r)
		}
	}

	////////

	tbCtx = newTableContextWithWhere(t, "db1", "tb1", "a < 5", "id", "a")
	for i := -100; i < 1000; i++ {
		r, err := tbCtx.WhereTrue(buildColumnValues(i, i))
		if err != nil {
			t.Fatal(err)
		}
		if r != (i < 5) {
			t.Fatalf("i: %v, r: %v", i, r)
		}
	}

	////////

	tbCtx = newTableContextWithWhere(t, "db1", "tb1", "a >= 5", "id", "a")
	for i := -100; i < 1000; i++ {
		r, err := tbCtx.WhereTrue(buildColumnValues(i, i))
		if err != nil {
			t.Fatal(err)
		}
		if r != (i >= 5) {
			t.Fatalf("i: %v, r: %v", i, r)
		}
	}

	////////
	tbCtx = newTableContextWithWhere(t, "db1", "tb1", "a >= 5 or b <=5 ", "id", "a", "b")
	{
		a,b := 2,3
		r, err := tbCtx.WhereTrue(buildColumnValues(1,2,3))
		if err != nil {
			t.Fatal(err)
		}
		if r != (a >= 5 || b <= 5) {
			t.Fatalf("r: %v", r)
		}
	}

	////////
	tbCtx = newTableContextWithWhere(t, "db1", "tb1", "a in (2,4,6,8,10,12,14,16) ", "id", "a")
	{
		for a := 0; a < 20; a++ {
			r, err := tbCtx.WhereTrue(buildColumnValues(1,a))
			if err != nil {
				t.Fatal(err)
			}
			var expect bool
			switch a {
			case 2,4,6,8,10,12,14,16:
				expect = true
			default:
				expect = false
			}
			if r != expect {
				t.Fatalf("r: %v", r)
			}
		}
	}
}
func TestWhereTrueFunc(t *testing.T) {
	var tbCtx *TableContext

	////////
	tbCtx = newTableContextWithWhere(t, "db1", "tb1", "pow(a,4)=16", "id", "a")
	{
		r, err := tbCtx.WhereTrue(buildColumnValues(1,2))
		if err != nil {
			t.Fatal(err)
		}
		if r != true {
			t.Fatalf("r: %v", r)
		}
	}

	////////

	tbCtx = newTableContextWithWhere(t, "db1", "tb1", "unix_timestamp(a) > unix_timestamp('2018-09-07 10:33:25')", "id", "a")
	{
		r, err := tbCtx.WhereTrue(buildColumnValues(1,"'2018-09-07 10:33:26'"))
		if err != nil {
			t.Fatal(err)
		}
		// TODO
		t.Logf("r: %v", r)
	}

}

func TestWhereTrueText(t *testing.T) {
	var tbCtx *TableContext

	tbCtx = newTableContextWithWhere(t, "db1", "tb1", "a = 'hello'", "id", "a")
	r, err := tbCtx.WhereTrue(buildColumnValues(1, []byte("hello")))
	if err != nil {
		t.Fatal(err)
	}
	if r != true {
		t.Fatalf("it is hello")
	}

	tbCtx = newTableContextWithWhere(t, "db1", "tb1", "a = 'hello'", "id", "a")
	r, err = tbCtx.WhereTrue(buildColumnValues(2, "hello2"))
	if err != nil {
		t.Fatal(err)
	}
	if r != false {
		t.Fatalf("it is not hello")
	}
}
