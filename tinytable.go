package tiny

import (
	"bytes"
	"errors"
	"os"

	"github.com/boltdb/bolt"
)

// DB is a small database abstraction built on top of Bolt DB modelled after Google's Big Table.
type DB interface {
	GetOrCreateTable(name []byte) (Table, error)
	DropTable(name []byte) error
}

// Table represents a table in the database.
type Table interface {
	WriteRow(r Row) error
	WriteColumn(r []byte, cf []byte, c Column) error
	IncrementColumn(r, cf, c []byte, v int) error
	ReadRow(key []byte) (Row, error)
	ScanRows(prefix []byte) chan Row
	ScanColumns(r, cf []byte, prefix []byte) chan Column
}

// Row represents a single record in a Table.
type Row struct {

	// RowKey is the unique record ID.
	RowKey []byte

	// ColumnFamilies is a list of column families for a single row.
	ColumnFamilies []ColumnFamily
}

// ColumnFamily is a group of columns for a single record.
type ColumnFamily struct {

	// Name is the name of the column family.
	Name []byte

	// Columns is a list of columns in the column family.
	Columns []Column
}

// Column represents a single column for a record.
type Column struct {
	Key   []byte
	Value []byte
}

// Open creates a new database if it doesn't exist or opens an existing database if it does.
func Open(filename string, mode os.FileMode) (DB, error) {
	b, err := bolt.Open(filename, mode, nil)
	if err != nil {
		return nil, err
	}
	return db{b}, nil
}

type db struct {
	internal *bolt.DB
}

func (db db) GetOrCreateTable(name []byte) (Table, error) {
	err := db.internal.Update(func(tx *bolt.Tx) error {
		_, e := tx.CreateBucketIfNotExists(name)
		return e
	})
	return table{db.internal, name}, err
}

func (db db) DropTable(name []byte) error {
	return db.internal.Update(func(tx *bolt.Tx) error {
		return tx.DeleteBucket(name)
	})
}

type table struct {
	internal *bolt.DB
	Name     []byte
}

func (t table) WriteRow(r Row) error {
	return t.internal.Update(func(tx *bolt.Tx) error {
		tbl := tx.Bucket(t.Name)
		if tbl == nil {
			return errors.New("Table does not exist")
		}

		row, err := tbl.CreateBucketIfNotExists(r.RowKey)
		if err != nil {
			return err
		}

		for i := range r.ColumnFamilies {
			cf, err := row.CreateBucketIfNotExists(r.ColumnFamilies[i].Name)
			if err != nil {
				return err
			}
			for _, c := range r.ColumnFamilies[i].Columns {
				if err := cf.Put(c.Key, c.Value); err != nil {
					return err
				}
			}
		}
		return nil
	})
}

func (t table) WriteColumn(r []byte, cf []byte, c Column) error {
	return t.internal.Update(func(tx *bolt.Tx) error {
		tbl := tx.Bucket(t.Name)
		if tbl == nil {
			return errors.New("Table does not exist")
		}

		row, err := tbl.CreateBucketIfNotExists(r)
		if err != nil {
			return err
		}

		cf, err := row.CreateBucketIfNotExists(cf)
		if err != nil {
			return err
		}
		return cf.Put(c.Key, c.Value)
	})
}

func (t table) IncrementColumn(r, cf, c []byte, v int) error {
	return nil
}

func (t table) ReadRow(key []byte) (Row, error) {
	var r Row
	r.RowKey = key
	err := t.internal.View(func(tx *bolt.Tx) error {
		tbl := tx.Bucket(t.Name)
		if tbl == nil {
			return errors.New("Table does not exist")
		}

		row := tbl.Bucket(key)
		if row == nil {
			return errors.New("Row does not exist")
		}

		row.ForEach(func(cfname, val []byte) error {
			if val == nil {
				cf := row.Bucket(cfname)
				var cols []Column
				cf.ForEach(func(c, v []byte) error {
					cols = append(cols, Column{c, v})
					return nil
				})
				r.ColumnFamilies = append(r.ColumnFamilies, ColumnFamily{
					Name:    cfname,
					Columns: cols,
				})
			}
			return nil
		})
		return nil
	})
	return r, err
}

func (t table) ScanRows(prefix []byte) chan Row {
	out := make(chan Row)

	go func() {
		t.internal.View(func(tx *bolt.Tx) error {
			// Assume bucket exists and has keys
			tbl := tx.Bucket(t.Name)
			if tbl == nil {
				return errors.New("Table does not exist")
			}

			c := tbl.Cursor()
			for k, v := c.Seek(prefix); bytes.HasPrefix(k, prefix); k, v = c.Next() {
				if v == nil {
					row := tbl.Bucket(k)
					if row != nil {
						var r Row
						r.RowKey = k
						row.ForEach(func(cfname, val []byte) error {
							if val == nil {
								cf := row.Bucket(cfname)
								var cols []Column
								cf.ForEach(func(c, v []byte) error {
									cols = append(cols, Column{c, v})
									return nil
								})
								r.ColumnFamilies = append(r.ColumnFamilies, ColumnFamily{
									Name:    cfname,
									Columns: cols,
								})
							}
							return nil
						})
						out <- r
					}
				}
			}
			return nil
		})
	}()

	return out
}

func (t table) ScanColumns(r, cfname []byte, prefix []byte) chan Column {
	out := make(chan Column)
	go func() {
		t.internal.View(func(tx *bolt.Tx) error {
			tbl := tx.Bucket(t.Name)
			if tbl == nil {
				return errors.New("Table does not exist")
			}

			row := tbl.Bucket(r)
			if row == nil {
				return errors.New("Row does not exist")
			}

			cf := row.Bucket(cfname)
			if cf == nil {
				return errors.New("Column Family does not exist")
			}

			c := cf.Cursor()
			for k, v := c.Seek(prefix); bytes.HasPrefix(k, prefix); k, v = c.Next() {
				out <- Column{k, v}
			}
			return nil
		})
	}()
	return out
}