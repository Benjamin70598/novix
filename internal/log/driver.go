package log

// driverName isolates the SQLite driver choice to one place.
//
// DECISIONS.md 005 targets modernc.org/sqlite (pure Go, no cgo) so that
// cross-compiling for small edge hardware stays trivial:
//
//	import _ "modernc.org/sqlite"
//	const driverName = "sqlite"
//
// This scaffold was built and crash-tested in an environment that could only
// reach github.com, so it ships with mattn/go-sqlite3 (cgo). Swapping to
// modernc is this file plus one go.mod line; the schema, pragmas, and tests
// are identical.
import _ "github.com/mattn/go-sqlite3"

const driverName = "sqlite3"
