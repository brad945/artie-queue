package queue

// The two heaps. A message lives in exactly one of them (or in the in-flight
// heap, or the dead-letter slice) at any moment, so Message.index is
// unambiguous and arbitrary removal is O(log n).
//
// Keeping delayed messages in their own heap ordered by VisibleAt is what lets
// promotion be a single timer set to the next deadline rather than a poll. The
// tempting alternative — leave everything in the ready heap and skip
// not-yet-visible messages at pop time — breaks the heap invariant: the top of
// the heap is the only element you can look at in O(1), so "skip it and look
// at the next one" degrades to a linear scan and, worse, makes the comparator
// no longer a total order over what the heap contains.

type readyHeap struct {
	items []*Message
	cfg   *Config
}

func (h *readyHeap) Len() int           { return len(h.items) }
func (h *readyHeap) Less(i, j int) bool { return h.cfg.less(h.items[i], h.items[j]) }
func (h *readyHeap) Swap(i, j int) {
	h.items[i], h.items[j] = h.items[j], h.items[i]
	h.items[i].index = i
	h.items[j].index = j
}
func (h *readyHeap) Push(x any) {
	m := x.(*Message)
	m.index = len(h.items)
	h.items = append(h.items, m)
}
func (h *readyHeap) Pop() any {
	old := h.items
	n := len(old)
	m := old[n-1]
	old[n-1] = nil
	h.items = old[:n-1]
	m.index = -1
	return m
}

// delayedHeap orders by VisibleAt, with Seq as a deterministic tie-break so
// that two messages that become visible in the same nanosecond still promote
// in enqueue order.
type delayedHeap struct{ items []*Message }

func (h *delayedHeap) Len() int { return len(h.items) }
func (h *delayedHeap) Less(i, j int) bool {
	a, b := h.items[i], h.items[j]
	if !a.VisibleAt.Equal(b.VisibleAt) {
		return a.VisibleAt.Before(b.VisibleAt)
	}
	return a.Seq < b.Seq
}
func (h *delayedHeap) Swap(i, j int) {
	h.items[i], h.items[j] = h.items[j], h.items[i]
	h.items[i].index = i
	h.items[j].index = j
}
func (h *delayedHeap) Push(x any) {
	m := x.(*Message)
	m.index = len(h.items)
	h.items = append(h.items, m)
}
func (h *delayedHeap) Pop() any {
	old := h.items
	n := len(old)
	m := old[n-1]
	old[n-1] = nil
	h.items = old[:n-1]
	m.index = -1
	return m
}

// leaseHeap orders in-flight messages by visibility deadline, so expiry is
// also a single timer rather than a sweep over everything outstanding.
type leaseHeap struct{ items []*Message }

func (h *leaseHeap) Len() int { return len(h.items) }
func (h *leaseHeap) Less(i, j int) bool {
	a, b := h.items[i], h.items[j]
	if !a.deadline.Equal(b.deadline) {
		return a.deadline.Before(b.deadline)
	}
	return a.Seq < b.Seq
}
func (h *leaseHeap) Swap(i, j int) {
	h.items[i], h.items[j] = h.items[j], h.items[i]
	h.items[i].index = i
	h.items[j].index = j
}
func (h *leaseHeap) Push(x any) {
	m := x.(*Message)
	m.index = len(h.items)
	h.items = append(h.items, m)
}
func (h *leaseHeap) Pop() any {
	old := h.items
	n := len(old)
	m := old[n-1]
	old[n-1] = nil
	h.items = old[:n-1]
	m.index = -1
	return m
}
