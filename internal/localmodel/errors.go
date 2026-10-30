package localmodel

import "fmt"

type diskError struct{ free, need int64 }

func (e *diskError) Error() string {
	return fmt.Sprintf("not enough free disk space: need about %d MB, %d MB available", (e.need>>20)+256, e.free>>20)
}
