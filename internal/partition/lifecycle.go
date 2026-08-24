package partition

import "eventbus/internal/model"

// Lifecycle drives partition state transitions with validation.
type Lifecycle struct{}

// NewLifecycle creates a partition lifecycle helper.
func NewLifecycle() *Lifecycle {
	return &Lifecycle{}
}

// Seal transitions an active partition to sealed.
func (l *Lifecycle) Seal(p *model.Partition) bool {
	if p.State() != model.PartitionActive {
		return false
	}
	p.SetState(model.PartitionSealed)
	return true
}

// Retire transitions a sealed partition to retired.
func (l *Lifecycle) Retire(p *model.Partition) bool {
	if p.State() != model.PartitionSealed && p.State() != model.PartitionActive {
		return false
	}
	p.SetState(model.PartitionRetired)
	return true
}

// Writable reports whether a partition accepts appends.
func (l *Lifecycle) Writable(p *model.Partition) bool {
	return p.State() == model.PartitionActive
}
