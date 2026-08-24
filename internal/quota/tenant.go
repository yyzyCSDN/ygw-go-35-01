package quota

// Tenant describes a quota customer.
type Tenant struct {
	ID    string
	Rate  int
	Burst int
}

// Registry maps tenant ids to quota settings.
type Registry struct {
	tenants map[string]Tenant
}

// NewRegistry creates a tenant registry.
func NewRegistry() *Registry {
	return &Registry{tenants: make(map[string]Tenant)}
}

// Register adds or updates a tenant.
func (r *Registry) Register(t Tenant) {
	r.tenants[t.ID] = t
}

// Get returns a tenant by id.
func (r *Registry) Get(id string) (Tenant, bool) {
	t, ok := r.tenants[id]
	return t, ok
}
