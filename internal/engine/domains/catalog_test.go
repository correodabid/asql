package domains

import "testing"

func TestCatalogRegisterAndLookup(t *testing.T) {
	catalog := NewCatalog()
	catalog.EnsureDomain("accounts")
	catalog.RegisterTable("accounts", "users")

	if !catalog.HasTable("accounts", "users") {
		t.Fatal("expected table to be registered in catalog")
	}

	if catalog.HasTable("loans", "users") {
		t.Fatal("unexpected table found in foreign domain")
	}
}
