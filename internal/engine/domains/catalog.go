package domains

import "strings"

type Catalog struct {
	domains map[string]map[string]struct{}
}

func NewCatalog() *Catalog {
	return &Catalog{domains: make(map[string]map[string]struct{})}
}

func (catalog *Catalog) EnsureDomain(name string) {
	domain := strings.ToLower(strings.TrimSpace(name))
	if domain == "" {
		return
	}

	if _, exists := catalog.domains[domain]; exists {
		return
	}

	catalog.domains[domain] = make(map[string]struct{})
}

func (catalog *Catalog) RegisterTable(domain, table string) {
	domain = strings.ToLower(strings.TrimSpace(domain))
	table = strings.ToLower(strings.TrimSpace(table))
	if domain == "" || table == "" {
		return
	}

	catalog.EnsureDomain(domain)
	catalog.domains[domain][table] = struct{}{}
}

// UnregisterTable removes a table from a domain's catalog.
func (catalog *Catalog) UnregisterTable(domain, table string) {
	domain = strings.ToLower(strings.TrimSpace(domain))
	table = strings.ToLower(strings.TrimSpace(table))
	if domain == "" || table == "" {
		return
	}

	tables, exists := catalog.domains[domain]
	if !exists {
		return
	}

	delete(tables, table)
}

func (catalog *Catalog) HasTable(domain, table string) bool {
	domain = strings.ToLower(strings.TrimSpace(domain))
	table = strings.ToLower(strings.TrimSpace(table))

	tables, exists := catalog.domains[domain]
	if !exists {
		return false
	}

	_, exists = tables[table]
	return exists
}

// Domains returns the internal domains map for read-only inspection.
func (catalog *Catalog) Domains() map[string]map[string]struct{} {
	return catalog.domains
}
