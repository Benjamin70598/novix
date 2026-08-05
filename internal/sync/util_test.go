package sync

import "os"

func writeAll(path, content string) error {
	return os.WriteFile(path, []byte(content), 0o644)
}
