package output

import (
	"fmt"
	"io"
	"strings"

	"github.com/hurou927/db-sub-data/internal/schema"
)

// Writer writes COPY-format SQL output.
type Writer struct {
	w io.Writer
}

// NewWriter creates a new COPY output writer.
func NewWriter(w io.Writer) *Writer {
	return &Writer{w: w}
}

// WriteHeader writes the BEGIN and session_replication_role setting.
func (cw *Writer) WriteHeader() error {
	_, err := fmt.Fprintln(cw.w, "BEGIN;")
	if err != nil {
		return err
	}
	_, err = fmt.Fprintln(cw.w, "SET session_replication_role = 'replica';")
	if err != nil {
		return err
	}
	_, err = fmt.Fprintln(cw.w)
	return err
}

// WriteFooter writes the session_replication_role reset and COMMIT.
func (cw *Writer) WriteFooter() error {
	_, err := fmt.Fprintln(cw.w, "SET session_replication_role = 'origin';")
	if err != nil {
		return err
	}
	_, err = fmt.Fprintln(cw.w, "COMMIT;")
	return err
}

// WriteTableData writes a COPY block for a single table.
func (cw *Writer) WriteTableData(table *schema.Table, rows [][]any) error {
	if len(rows) == 0 {
		return nil
	}

	colNames := table.ColumnNames()
	_, err := fmt.Fprintf(cw.w, "COPY %s (%s) FROM stdin;\n",
		table.FullName(), strings.Join(colNames, ", "))
	if err != nil {
		return err
	}

	for _, row := range rows {
		vals := make([]string, len(row))
		for i, v := range row {
			vals[i] = EscapeCopyValue(v)
		}
		_, err := fmt.Fprintln(cw.w, strings.Join(vals, "\t"))
		if err != nil {
			return err
		}
	}

	_, err = fmt.Fprintln(cw.w, `\.`)
	if err != nil {
		return err
	}
	_, err = fmt.Fprintln(cw.w)
	return err
}
