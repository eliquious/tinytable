package tiny

import (
	"os"
	"testing"

	"github.com/boltdb/bolt"
	"github.com/stretchr/testify/assert"
)

func TestOpen(t *testing.T) {
	filename := "tinytable.tt"
	tdb, err := Open(filename, 600)
	assert.Nil(t, err)
	defer tdb.Close()
	defer os.Remove(filename)
	assert.Nil(t, err)

	_, err = os.Stat(filename)
	assert.Nil(t, err)
}

func TestGetOrCreateTable(t *testing.T) {
	filename := "tinytable.tt"
	tdb, err := Open(filename, 600)
	defer tdb.Close()
	defer os.Remove(filename)
	assert.Nil(t, err)

	tablename := []byte("table")
	table, err := tdb.GetOrCreateTable(tablename)
	assert.Nil(t, err, "Error should be nil")
	assert.Equal(t, tablename, table.GetName())

	tdb.(db).internal.View(func(tx *bolt.Tx) error {
		tbl := tx.Bucket(tablename)
		assert.NotNil(t, tbl)
		return nil
	})
}

func TestDeleteTable(t *testing.T) {
	filename := "tinytable.tt"
	tdb, err := Open(filename, 600)
	defer tdb.Close()
	defer os.Remove(filename)
	assert.Nil(t, err)

	tablename := []byte("table")
	tdb.(db).internal.Update(func(tx *bolt.Tx) error {
		tx.CreateBucket(tablename)
		return nil
	})

	err = tdb.DropTable(tablename)
	assert.Nil(t, err, "Error should be nil")

	tdb.(db).internal.View(func(tx *bolt.Tx) error {
		tbl := tx.Bucket(tablename)
		assert.Nil(t, tbl)
		return nil
	})
}

func TestWriteRows(t *testing.T) {
	filename := "tinytable.tt"
	tdb, err := Open(filename, 600)
	defer tdb.Close()
	defer os.Remove(filename)
	assert.Nil(t, err)

	tablename := []byte("table")
	tbl, err := tdb.GetOrCreateTable(tablename)
	assert.Nil(t, err, "Error should be nil")

	rows := []Row{
		Row{
			RowKey: []byte("2aaeb119bc5b8985951dba1106519611"),
			ColumnFamilies: []ColumnFamily{
				ColumnFamily{
					Name: []byte("props"),
					Columns: []Column{
						Column{[]byte("1ce91cfa3da18d8c"), []byte("value")},
					},
				},
			},
		}, Row{
			RowKey: []byte("ea6be0c8baef24546e2fba9a03136025"),
			ColumnFamilies: []ColumnFamily{
				ColumnFamily{
					Name: []byte("props"),
					Columns: []Column{
						Column{[]byte("3f5ee52f9ca720b5"), []byte("value")},
					},
				},
			},
		},
	}
	err = tbl.WriteRows(rows...)
	assert.Nil(t, err)

	tdb.(db).internal.View(func(tx *bolt.Tx) error {
		tbl := tx.Bucket(tablename)
		assert.NotNil(t, tbl)

		for _, r := range rows {
			row := tbl.Bucket(r.RowKey)
			assert.NotNil(t, tbl)

			for _, cf := range r.ColumnFamilies {
				cfBucket := row.Bucket(cf.Name)
				assert.NotNil(t, cfBucket)

				for _, c := range cf.Columns {
					val := cfBucket.Get(c.Key)
					assert.Equal(t, c.Value, val)
				}
			}
		}
		return nil
	})
}

func TestWriteColumns(t *testing.T) {
	filename := "tinytable.tt"
	tdb, err := Open(filename, 600)
	defer tdb.Close()
	defer os.Remove(filename)
	assert.Nil(t, err)

	tablename := []byte("table")
	tbl, err := tdb.GetOrCreateTable(tablename)
	assert.Nil(t, err, "Error should be nil")

	cols := []Column{
		Column{[]byte("3f5ee52f9ca720b5"), []byte("value")},
		Column{[]byte("1ce91cfa3da18d8c"), []byte("value")},
	}

	err = tbl.WriteColumns([]byte("2aaeb119bc5b8985951dba1106519611"), []byte("props"), cols...)
	assert.Nil(t, err)

	tdb.(db).internal.View(func(tx *bolt.Tx) error {
		tbl := tx.Bucket(tablename)
		assert.NotNil(t, tbl)

		row := tbl.Bucket([]byte("2aaeb119bc5b8985951dba1106519611"))
		assert.NotNil(t, tbl)

		cfBucket := row.Bucket([]byte("props"))
		assert.NotNil(t, cfBucket)

		for _, c := range cols {
			val := cfBucket.Get(c.Key)
			assert.Equal(t, c.Value, val)
		}

		return nil
	})
}

func TestReadRow(t *testing.T) {
	filename := "tinytable.tt"
	tdb, err := Open(filename, 600)
	defer tdb.Close()
	defer os.Remove(filename)
	assert.Nil(t, err)

	tablename := []byte("table")
	tbl, err := tdb.GetOrCreateTable(tablename)
	assert.Nil(t, err, "Error should be nil")

	row := Row{
		RowKey: []byte("2aaeb119bc5b8985951dba1106519611"),
		ColumnFamilies: []ColumnFamily{
			ColumnFamily{
				Name: []byte("props"),
				Columns: []Column{
					Column{[]byte("1ce91cfa3da18d8c"), []byte("value")},
				},
			},
		},
	}
	err = tbl.WriteRows(row)
	assert.Nil(t, err)

	r, err := tbl.ReadRow([]byte("2aaeb119bc5b8985951dba1106519611"))
	assert.Nil(t, err)
	assert.Equal(t, row, r)
}

func TestScanRows(t *testing.T) {
	filename := "tinytable.tt"
	tdb, err := Open(filename, 600)
	defer tdb.Close()
	defer os.Remove(filename)
	assert.Nil(t, err)

	tablename := []byte("table")
	tbl, err := tdb.GetOrCreateTable(tablename)
	assert.Nil(t, err, "Error should be nil")

	rows := []Row{
		Row{
			RowKey: []byte("69f357d86100962eacb1e43c28b5ad18"),
			ColumnFamilies: []ColumnFamily{
				ColumnFamily{
					Name: []byte("props"),
					Columns: []Column{
						Column{[]byte("6f9148c1fcec5e23"), []byte("value")},
					},
				},
			},
		},
		Row{
			RowKey: []byte("ea6be0c8baef24546e2fba9a03136025"),
			ColumnFamilies: []ColumnFamily{
				ColumnFamily{
					Name: []byte("props"),
					Columns: []Column{
						Column{[]byte("3f5ee52f9ca720b5"), []byte("value")},
					},
				},
			},
		},
		Row{
			RowKey: []byte("ea6be0c8baef2454951dba1106519611"),
			ColumnFamilies: []ColumnFamily{
				ColumnFamily{
					Name: []byte("props"),
					Columns: []Column{
						Column{[]byte("1ce91cfa3da18d8c"), []byte("value")},
					},
				},
			},
		},
	}
	err = tbl.WriteRows(rows...)
	assert.Nil(t, err)

	var rs []Row
	for r := range tbl.ScanRows([]byte("ea6be0c8baef2454")) {
		rs = append(rs, r)
	}
	assert.Equal(t, rows[1:], rs)
}

func TestScanColumns(t *testing.T) {
	filename := "tinytable.tt"
	tdb, err := Open(filename, 600)
	defer tdb.Close()
	defer os.Remove(filename)
	assert.Nil(t, err)

	tablename := []byte("table")
	tbl, err := tdb.GetOrCreateTable(tablename)
	assert.Nil(t, err, "Error should be nil")

	row := Row{
		RowKey: []byte("69f357d86100962eacb1e43c28b5ad18"),
		ColumnFamilies: []ColumnFamily{
			ColumnFamily{
				Name: []byte("props"),
				Columns: []Column{
					Column{[]byte("6f9148c1fcec5e23"), []byte("value")},
					Column{[]byte("3f5ee52f3da18d8c"), []byte("value")},
					Column{[]byte("3f5ee52f9ca720b5"), []byte("value")},
				},
			},
		},
	}
	err = tbl.WriteRows(row)
	assert.Nil(t, err)

	var cs []Column
	for c := range tbl.ScanColumns([]byte("69f357d86100962eacb1e43c28b5ad18"), []byte("props"), []byte("3f5ee52f")) {
		cs = append(cs, c)
	}
	assert.Equal(t, row.ColumnFamilies[0].Columns[1:], cs)
}
