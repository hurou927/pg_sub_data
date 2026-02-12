package extract

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/hurou927/db-sub-data/internal/schema"
)

// fetchSelfRefRows retrieves all rows from a self-referencing table using
// a recursive CTE starting from the given seed PK values.
func fetchSelfRefRows(ctx context.Context, pool *pgxpool.Pool, table *schema.Table, fk schema.ForeignKey, seedPKs [][]any, verbose bool) ([][]any, error) {
	query, args := buildSelfRefQuery(table, fk, seedPKs)
	if query == "" {
		return nil, nil
	}

	if verbose {
		fmt.Printf("  [self-ref] %s: %s (args: %v)\n", table.FullName(), query, args)
	}

	rows, err := pool.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("self-ref query for %s: %w", table.FullName(), err)
	}
	defer rows.Close()

	var result [][]any
	for rows.Next() {
		values, err := rows.Values()
		if err != nil {
			return nil, err
		}
		result = append(result, values)
	}
	return result, rows.Err()
}
