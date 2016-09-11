package tiny

import (
	"bytes"
	"encoding/binary"
	"errors"
	"os"

	"github.com/boltdb/bolt"
)

// DB is a small database abstraction built on top of Bolt DB modelled after Google's Big Table.
type DB interface {
	GetOrCreateTable(name []byte) (Table, error)
	DropTable(name []byte) error
	Close() error
}

// Table represents a table in the database.
type Table interface {
	GetName() []byte
	WriteRows(r ...Row) error
	WriteColumns(r []byte, cf []byte, c ...Column) error
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

// GetColumnFamily returns a column family by name if it exists for this row. Otherwise an error is returned.
func (r Row) GetColumnFamily(name []byte) (ColumnFamily, error) {
	for _, cf := range r.ColumnFamilies {
		if bytes.Equal(cf.Name, name) {
			return cf, nil
		}
	}
	return ColumnFamily{}, errors.New("Column Family does not exist")
}

// ColumnFamily is a group of columns for a single record.
type ColumnFamily struct {

	// Name is the name of the column family.
	Name []byte

	// Columns is a list of columns in the column family.
	Columns []Column
}

// GetColumn returns a column by key if it exists for this row. Otherwise, an error is returned.
func (cf ColumnFamily) GetColumn(key []byte) (Column, error) {
	for _, c := range cf.Columns {
		if bytes.Equal(c.Key, key) {
			return c, nil
		}
	}
	return Column{}, errors.New("Column does not exist")
}

// Column represents a single column for a record.
type Column struct {
	Key   []byte
	Value []byte
}

// Uint64 reads a uint64 from the value. If the length of the value is not 8 bytes, 0 will be returned.
func (c Column) Uint64() uint64 {
	if len(c.Value) != 8 {
		return binary.LittleEndian.Uint64(c.Value)
	}
	return 0
}

// Int64 reads a int64 from the value. If the length of the value is not 8 bytes, 0 will be returned.
func (c Column) Int64() int64 {
	return int64(c.Uint64())
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

func (db db) Close() error {
	return db.internal.Close()
}

type table struct {
	internal *bolt.DB
	Name     []byte
}

// GetName returns the name of the table.
func (t table) GetName() []byte {
	return t.Name
}

// WriteRows writes a list of rows to the table overwritting any existing values that overlap.
func (t table) WriteRows(rows ...Row) error {
	return t.internal.Update(func(tx *bolt.Tx) error {
		tbl := tx.Bucket(t.Name)
		if tbl == nil {
			return errors.New("Table does not exist")
		}

		for _, r := range rows {
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
		}
		return nil
	})
}

// WriteColumns writes a column to a row and column family returning an error if there is one. The row and column family
// will be created if they do not exist.
func (t table) WriteColumns(r []byte, cf []byte, cols ...Column) error {
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

		for _, c := range cols {
			if err := cf.Put(c.Key, c.Value); err != nil {
				return err
			}
		}
		return nil
	})
}

// IncrementColumn increments the value of a column by one. If the row or column family does not exist, they will be
// created and return an error if it fails. If the column does not exist, a value will be created to store an
// unsigned 64-bit integer. If the existing value is not 8 bytes, an error will be returned. Otherwise the value will be
// incremented.
func (t table) IncrementColumn(r, cf, c []byte, v int) error {
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

		val := cf.Get(c)
		if val == nil {
			val = make([]byte, 8)
			binary.LittleEndian.PutUint64(val, uint64(v))
		} else if len(val) != 8 {
			return errors.New("Invalid value length")
		} else {
			tmp := binary.LittleEndian.Uint64(val)
			binary.LittleEndian.PutUint64(val, tmp+uint64(v))
		}
		return cf.Put(c, val)
	})
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
	out := make(chan Row, 1)

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
		close(out)
	}()

	return out
}

func (t table) ScanColumns(r, cfname []byte, prefix []byte) chan Column {
	out := make(chan Column, 1)
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
		close(out)
	}()
	return out
}
