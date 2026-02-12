package schema

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Introspect queries PostgreSQL catalogs and returns all tables with columns, PKs, and FKs.
func Introspect(ctx context.Context, pool *pgxpool.Pool, schemas []string) (map[string]*Table, error) {
	tables, err := queryTablesAndColumns(ctx, pool, schemas)
	if err != nil {
		return nil, fmt.Errorf("querying tables and columns: %w", err)
	}

	if err := queryPrimaryKeys(ctx, pool, schemas, tables); err != nil {
		return nil, fmt.Errorf("querying primary keys: %w", err)
	}

	if err := queryForeignKeys(ctx, pool, schemas, tables); err != nil {
		return nil, fmt.Errorf("querying foreign keys: %w", err)
	}

	return tables, nil
}

func queryTablesAndColumns(ctx context.Context, pool *pgxpool.Pool, schemas []string) (map[string]*Table, error) {
	query := `
		SELECT
			n.nspname AS schema_name,
			c.relname AS table_name,
			a.attname AS column_name,
			t.typname AS data_type,
			NOT a.attnotnull AS is_nullable,
			a.attnum AS ordinal_position
		FROM pg_class c
		JOIN pg_namespace n ON n.oid = c.relnamespace
		JOIN pg_attribute a ON a.attrelid = c.oid
		JOIN pg_type t ON t.oid = a.atttypid
		WHERE c.relkind = 'r'
			AND a.attnum > 0
			AND NOT a.attisdropped
			AND n.nspname = ANY($1)
		ORDER BY n.nspname, c.relname, a.attnum
	`

	rows, err := pool.Query(ctx, query, schemas)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	tables := make(map[string]*Table)
	for rows.Next() {
		var schemaName, tableName, colName, dataType string
		var nullable bool
		var ordPos int
		if err := rows.Scan(&schemaName, &tableName, &colName, &dataType, &nullable, &ordPos); err != nil {
			return nil, err
		}

		key := schemaName + "." + tableName
		tbl, ok := tables[key]
		if !ok {
			tbl = &Table{
				Schema: schemaName,
				Name:   tableName,
			}
			tables[key] = tbl
		}
		tbl.Columns = append(tbl.Columns, Column{
			Name:     colName,
			DataType: dataType,
			Nullable: nullable,
			OrdPos:   ordPos,
		})
	}

	return tables, rows.Err()
}

func queryPrimaryKeys(ctx context.Context, pool *pgxpool.Pool, schemas []string, tables map[string]*Table) error {
	query := `
		SELECT
			n.nspname AS schema_name,
			c.relname AS table_name,
			a.attname AS column_name,
			u.ord AS key_position
		FROM pg_constraint con
		JOIN pg_class c ON c.oid = con.conrelid
		JOIN pg_namespace n ON n.oid = c.relnamespace
		CROSS JOIN LATERAL unnest(con.conkey) WITH ORDINALITY AS u(attnum, ord)
		JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum = u.attnum
		WHERE con.contype = 'p'
			AND n.nspname = ANY($1)
		ORDER BY n.nspname, c.relname, u.ord
	`

	rows, err := pool.Query(ctx, query, schemas)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var schemaName, tableName, colName string
		var keyPos int
		if err := rows.Scan(&schemaName, &tableName, &colName, &keyPos); err != nil {
			return err
		}

		key := schemaName + "." + tableName
		tbl, ok := tables[key]
		if !ok {
			continue
		}
		if tbl.PrimaryKey == nil {
			tbl.PrimaryKey = &PrimaryKey{}
		}
		tbl.PrimaryKey.Columns = append(tbl.PrimaryKey.Columns, colName)
	}

	return rows.Err()
}

func queryForeignKeys(ctx context.Context, pool *pgxpool.Pool, schemas []string, tables map[string]*Table) error {
	query := `
		SELECT
			con.conname AS fk_name,
			cn.nspname AS child_schema,
			cc.relname AS child_table,
			ca.attname AS child_column,
			pn.nspname AS parent_schema,
			pc.relname AS parent_table,
			pa.attname AS parent_column,
			u.ord AS key_position
		FROM pg_constraint con
		JOIN pg_class cc ON cc.oid = con.conrelid
		JOIN pg_namespace cn ON cn.oid = cc.relnamespace
		JOIN pg_class pc ON pc.oid = con.confrelid
		JOIN pg_namespace pn ON pn.oid = pc.relnamespace
		CROSS JOIN LATERAL unnest(con.conkey, con.confkey) WITH ORDINALITY AS u(child_attnum, parent_attnum, ord)
		JOIN pg_attribute ca ON ca.attrelid = cc.oid AND ca.attnum = u.child_attnum
		JOIN pg_attribute pa ON pa.attrelid = pc.oid AND pa.attnum = u.parent_attnum
		WHERE con.contype = 'f'
			AND cn.nspname = ANY($1)
		ORDER BY con.conname, u.ord
	`

	rows, err := pool.Query(ctx, query, schemas)
	if err != nil {
		return err
	}
	defer rows.Close()

	// Collect FK columns grouped by constraint name
	type fkEntry struct {
		name         string
		childSchema  string
		childTable   string
		childCol     string
		parentSchema string
		parentTable  string
		parentCol    string
	}

	fksByName := make(map[string][]fkEntry)
	var fkOrder []string

	for rows.Next() {
		var e fkEntry
		var keyPos int
		if err := rows.Scan(&e.name, &e.childSchema, &e.childTable, &e.childCol,
			&e.parentSchema, &e.parentTable, &e.parentCol, &keyPos); err != nil {
			return err
		}
		if _, exists := fksByName[e.name]; !exists {
			fkOrder = append(fkOrder, e.name)
		}
		fksByName[e.name] = append(fksByName[e.name], e)
	}
	if err := rows.Err(); err != nil {
		return err
	}

	for _, name := range fkOrder {
		entries := fksByName[name]
		first := entries[0]
		fk := ForeignKey{
			Name:         name,
			ChildSchema:  first.childSchema,
			ChildTable:   first.childTable,
			ParentSchema: first.parentSchema,
			ParentTable:  first.parentTable,
		}
		for _, e := range entries {
			fk.ChildColumns = append(fk.ChildColumns, e.childCol)
			fk.ParentColumns = append(fk.ParentColumns, e.parentCol)
		}
		fk.IsSelfRef = (fk.ChildSchema == fk.ParentSchema && fk.ChildTable == fk.ParentTable)

		childKey := fk.ChildSchema + "." + fk.ChildTable
		if tbl, ok := tables[childKey]; ok {
			tbl.ForeignKeys = append(tbl.ForeignKeys, fk)
		}
	}

	return nil
}
