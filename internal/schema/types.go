package schema

// Column represents a database column.
type Column struct {
	Name     string
	DataType string // PostgreSQL type name (e.g. "int4", "text", "bool")
	Nullable bool
	OrdPos   int // ordinal position (1-based)
}

// PrimaryKey represents a table's primary key.
type PrimaryKey struct {
	Columns []string
}

// ForeignKey represents a foreign key constraint.
type ForeignKey struct {
	Name           string
	ChildSchema    string
	ChildTable     string
	ChildColumns   []string
	ParentSchema   string
	ParentTable    string
	ParentColumns  []string
	IsSelfRef      bool
}

// Table represents a database table with its columns, PK, and FKs.
type Table struct {
	Schema     string
	Name       string
	Columns    []Column
	PrimaryKey *PrimaryKey
	ForeignKeys []ForeignKey
}

// FullName returns schema-qualified table name.
func (t *Table) FullName() string {
	return t.Schema + "." + t.Name
}

// ColumnNames returns all column names in ordinal order.
func (t *Table) ColumnNames() []string {
	names := make([]string, len(t.Columns))
	for i, c := range t.Columns {
		names[i] = c.Name
	}
	return names
}

// PKColumnNames returns the primary key column names, or nil if no PK.
func (t *Table) PKColumnNames() []string {
	if t.PrimaryKey == nil {
		return nil
	}
	return t.PrimaryKey.Columns
}
