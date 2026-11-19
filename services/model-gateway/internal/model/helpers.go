package model

import "strconv"

// jsonItoa renders an int for SQL placeholder construction.
func jsonItoa(n int) string { return strconv.Itoa(n) }
