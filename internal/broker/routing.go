package broker

// Router assigns a key to a partition deterministically.
type Router struct {
	set *PartitionSet
}

// NewRouter creates a partition router for a topic.
func NewRouter(set *PartitionSet) *Router {
	return &Router{set: set}
}

// Route picks a partition for a key by summing bytes modulo partition count.
func (r *Router) Route(key string) (int, bool) {
	list := r.set.List()
	if len(list) == 0 {
		return 0, false
	}
	h := uint32(0)
	for i := 0; i < len(key); i++ {
		h = h*31 + uint32(key[i])
	}
	return list[int(h)%len(list)], true
}
