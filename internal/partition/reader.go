package partition

import (
	"eventbus/internal/model"
)

// Reader walks messages of a segment from a starting offset.
type Reader struct {
	store   *Store
	seg     string
	cursor  int64
}

// NewReader creates a segment reader.
func NewReader(s *Store, seg string, from int64) *Reader {
	return &Reader{store: s, seg: seg, cursor: from}
}

// Next returns the next message at or after the cursor.
func (r *Reader) Next(pid int) (*model.Message, bool) {
	for _, m := range r.store.Read(pid, r.seg, r.cursor) {
		if m.Offset >= r.cursor {
			r.cursor = m.Offset + 1
			return m, true
		}
	}
	return nil, false
}

// Cursor returns the current read cursor.
func (r *Reader) Cursor() int64 {
	return r.cursor
}
