package config

// ByWindow is used to sort retention definitions by window duration.
type ByWindow []RollupWindow

// Implementation of sort.Interface.
func (w ByWindow) Len() int {
	return len(w)
}
func (w ByWindow) Swap(i, j int) {
	w[i], w[j] = w[j], w[i]
}
func (w ByWindow) Less(i, j int) bool {
	return w[i].Window < w[j].Window
}

// ByPriority is used to provide a consistent order for processing path expressions.
type ByPriority []string

// Implementation of sort.Interface.
func (p ByPriority) Len() int {
	return len(p)
}
func (p ByPriority) Swap(i, j int) {
	p[i], p[j] = p[j], p[i]
}
func (p ByPriority) Less(i, j int) bool {

	// "default" is always last in priority.
	if p[i] == CATCHALL_EXPRESSION {
		return false
	}
	if p[j] == CATCHALL_EXPRESSION {
		return true
	}

	// Longer strings sort earlier.
	if len(p[i]) > len(p[j]) {
		return true
	} else if len(p[i]) < len(p[j]) {
		return false
	}

	// Same-length strings are ordered lexically.
	return p[i] < p[j]
}
