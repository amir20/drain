package migrate

import "strings"

// Split breaks a SQL file into individual statements.
//
// A naive split on ';' is wrong here: the migrations contain dollar-quoted function and
// procedure bodies ($$ ... $$, $proc$ ... $proc$) that are full of semicolons, and DO
// blocks that nest the same way. This walks the text tracking whether it is inside a
// line comment, a block comment, a single-quoted literal or a dollar-quoted string, and
// only cuts on semicolons at the top level.
func Split(sql string) []string {
	var (
		statements []string
		current    strings.Builder
		i          int
	)

	flush := func() {
		s := strings.TrimSpace(current.String())
		current.Reset()
		// Stray semicolons and whitespace are not statements.
		if strings.Trim(s, "; \t\r\n") == "" {
			return
		}
		statements = append(statements, s)
	}

	for i < len(sql) {
		switch {
		case strings.HasPrefix(sql[i:], "--"):
			end := strings.IndexByte(sql[i:], '\n')
			if end < 0 {
				current.WriteString(sql[i:])
				i = len(sql)
			} else {
				current.WriteString(sql[i : i+end+1])
				i += end + 1
			}

		case strings.HasPrefix(sql[i:], "/*"):
			end := strings.Index(sql[i+2:], "*/")
			if end < 0 {
				current.WriteString(sql[i:])
				i = len(sql)
			} else {
				current.WriteString(sql[i : i+2+end+2])
				i += 2 + end + 2
			}

		case sql[i] == '\'':
			j := i + 1
			for j < len(sql) {
				if sql[j] == '\'' {
					// '' is an escaped quote, not the end of the literal.
					if j+1 < len(sql) && sql[j+1] == '\'' {
						j += 2
						continue
					}
					break
				}
				j++
			}
			if j >= len(sql) {
				current.WriteString(sql[i:])
				i = len(sql)
			} else {
				current.WriteString(sql[i : j+1])
				i = j + 1
			}

		case sql[i] == '$':
			if tag, ok := dollarTag(sql[i:]); ok {
				end := strings.Index(sql[i+len(tag):], tag)
				if end < 0 {
					current.WriteString(sql[i:])
					i = len(sql)
				} else {
					stop := i + len(tag) + end + len(tag)
					current.WriteString(sql[i:stop])
					i = stop
				}
			} else {
				current.WriteByte(sql[i])
				i++
			}

		case sql[i] == ';':
			current.WriteByte(';')
			flush()
			i++

		default:
			current.WriteByte(sql[i])
			i++
		}
	}

	flush()
	return statements
}

// dollarTag reports the dollar-quote tag starting at s, e.g. "$$" or "$proc$".
func dollarTag(s string) (string, bool) {
	if len(s) == 0 || s[0] != '$' {
		return "", false
	}
	for i := 1; i < len(s); i++ {
		c := s[i]
		if c == '$' {
			return s[:i+1], true
		}
		isLetter := (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || c == '_'
		isDigit := c >= '0' && c <= '9'
		// The tag body is an identifier; a digit may not start it.
		if !isLetter && !(isDigit && i > 1) {
			return "", false
		}
	}
	return "", false
}
