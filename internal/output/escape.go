package output

import (
	"encoding/hex"
	"fmt"
	"strings"
	"time"
)

// EscapeCopyValue escapes a single value for PostgreSQL COPY text format.
// NULL is represented as \N.
func EscapeCopyValue(val any) string {
	if val == nil {
		return `\N`
	}

	switch v := val.(type) {
	case bool:
		if v {
			return "t"
		}
		return "f"
	case []byte:
		// bytea: output as hex-encoded with \x prefix
		return `\\x` + hex.EncodeToString(v)
	case time.Time:
		return escapeString(v.Format("2006-01-02 15:04:05.999999-07"))
	case string:
		return escapeString(v)
	case fmt.Stringer:
		return escapeString(v.String())
	default:
		return escapeString(fmt.Sprintf("%v", v))
	}
}

// escapeString applies COPY text format escaping.
func escapeString(s string) string {
	var b strings.Builder
	b.Grow(len(s))
	for _, r := range s {
		switch r {
		case '\\':
			b.WriteString(`\\`)
		case '\n':
			b.WriteString(`\n`)
		case '\r':
			b.WriteString(`\r`)
		case '\t':
			b.WriteString(`\t`)
		default:
			b.WriteRune(r)
		}
	}
	return b.String()
}
