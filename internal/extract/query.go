package extract

import (
	"fmt"
	"strings"

	"github.com/hurou927/db-sub-data/internal/schema"
)

// buildRootQuery builds a SELECT query for a root table with a WHERE clause.
func buildRootQuery(table *schema.Table, where string) string {
	q := fmt.Sprintf("SELECT * FROM %s", table.FullName())
	if where != "" {
		q += " WHERE " + where
	}
	return q
}

// buildChildQuery builds a SELECT query for a child table based on collected parent PKs.
// parentPKs maps parent full name → list of PK value tuples.
func buildChildQuery(table *schema.Table, g fkGraph, parentPKs map[string][][]any) (string, []any) {
	var conditions []string
	var args []any
	argIdx := 1

	for _, fk := range table.ForeignKeys {
		if fk.IsSelfRef {
			continue
		}
		parentKey := fk.ParentSchema + "." + fk.ParentTable
		pks, ok := parentPKs[parentKey]
		if !ok || len(pks) == 0 {
			continue
		}

		// Check if this FK is nullable
		nullable := isFKNullable(table, fk)

		if len(fk.ChildColumns) == 1 {
			// Single column FK: col IN ($1, $2, ...)
			cond, newArgs, nextIdx := buildSingleColumnIN(fk, pks, nullable, argIdx)
			conditions = append(conditions, cond)
			args = append(args, newArgs...)
			argIdx = nextIdx
		} else {
			// Composite FK: (col1, col2) IN (($1,$2), ($3,$4), ...)
			cond, newArgs, nextIdx := buildCompositeIN(fk, pks, nullable, argIdx)
			conditions = append(conditions, cond)
			args = append(args, newArgs...)
			argIdx = nextIdx
		}
	}

	if len(conditions) == 0 {
		return "", nil
	}

	q := fmt.Sprintf("SELECT * FROM %s WHERE %s",
		table.FullName(), strings.Join(conditions, " AND "))
	return q, args
}

func buildSingleColumnIN(fk schema.ForeignKey, pks [][]any, nullable bool, argIdx int) (string, []any, int) {
	col := fk.ChildColumns[0]

	if len(pks) > 10000 {
		// For large value sets, we'll still use IN but the caller should
		// use temp tables. For now, cap at reasonable size.
		pks = pks[:10000]
	}

	placeholders := make([]string, len(pks))
	args := make([]any, len(pks))
	for i, pk := range pks {
		placeholders[i] = fmt.Sprintf("$%d", argIdx)
		args[i] = pk[0] // single column, first value
		argIdx++
	}

	cond := fmt.Sprintf("%s IN (%s)", col, strings.Join(placeholders, ", "))
	if nullable {
		cond = fmt.Sprintf("(%s OR %s IS NULL)", cond, col)
	}
	return cond, args, argIdx
}

func buildCompositeIN(fk schema.ForeignKey, pks [][]any, nullable bool, argIdx int) (string, []any, int) {
	cols := strings.Join(fk.ChildColumns, ", ")

	if len(pks) > 10000 {
		pks = pks[:10000]
	}

	var tuples []string
	var args []any
	for _, pk := range pks {
		placeholders := make([]string, len(fk.ChildColumns))
		for j := range fk.ChildColumns {
			placeholders[j] = fmt.Sprintf("$%d", argIdx)
			if j < len(pk) {
				args = append(args, pk[j])
			} else {
				args = append(args, nil)
			}
			argIdx++
		}
		tuples = append(tuples, "("+strings.Join(placeholders, ", ")+")")
	}

	cond := fmt.Sprintf("(%s) IN (%s)", cols, strings.Join(tuples, ", "))
	if nullable {
		nullChecks := make([]string, len(fk.ChildColumns))
		for i, c := range fk.ChildColumns {
			nullChecks[i] = c + " IS NULL"
		}
		cond = fmt.Sprintf("(%s OR (%s))", cond, strings.Join(nullChecks, " AND "))
	}
	return cond, args, argIdx
}

// buildSelfRefQuery builds a recursive CTE for self-referencing tables.
func buildSelfRefQuery(table *schema.Table, fk schema.ForeignKey, seedPKs [][]any) (string, []any) {
	if table.PrimaryKey == nil || len(seedPKs) == 0 {
		return "", nil
	}

	pkCols := table.PrimaryKey.Columns
	fkChildCols := fk.ChildColumns
	fkParentCols := fk.ParentColumns

	var args []any
	argIdx := 1

	// Build seed condition: PK IN (...)
	var seedCond string
	if len(pkCols) == 1 {
		placeholders := make([]string, len(seedPKs))
		for i, pk := range seedPKs {
			placeholders[i] = fmt.Sprintf("$%d", argIdx)
			args = append(args, pk[0])
			argIdx++
		}
		seedCond = fmt.Sprintf("%s IN (%s)", pkCols[0], strings.Join(placeholders, ", "))
	} else {
		var tuples []string
		for _, pk := range seedPKs {
			phs := make([]string, len(pkCols))
			for j := range pkCols {
				phs[j] = fmt.Sprintf("$%d", argIdx)
				args = append(args, pk[j])
				argIdx++
			}
			tuples = append(tuples, "("+strings.Join(phs, ", ")+")")
		}
		seedCond = fmt.Sprintf("(%s) IN (%s)",
			strings.Join(pkCols, ", "), strings.Join(tuples, ", "))
	}

	// Build recursive join condition
	joinConds := make([]string, len(fkChildCols))
	for i := range fkChildCols {
		joinConds[i] = fmt.Sprintf("t.%s = r.%s", fkParentCols[i], fkChildCols[i])
	}

	q := fmt.Sprintf(`WITH RECURSIVE tree AS (
  SELECT t.* FROM %s t WHERE %s
  UNION ALL
  SELECT t.* FROM %s t JOIN tree r ON %s
)
SELECT DISTINCT * FROM tree`,
		table.FullName(), seedCond,
		table.FullName(), strings.Join(joinConds, " AND "))

	return q, args
}

func isFKNullable(table *schema.Table, fk schema.ForeignKey) bool {
	colMap := make(map[string]*schema.Column)
	for i := range table.Columns {
		colMap[table.Columns[i].Name] = &table.Columns[i]
	}
	for _, colName := range fk.ChildColumns {
		if col, ok := colMap[colName]; ok && col.Nullable {
			return true
		}
	}
	return false
}

// fkGraph is a minimal interface for buildChildQuery to avoid circular imports.
type fkGraph interface{}
