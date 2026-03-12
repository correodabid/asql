package main

import "testing"

func TestBeginScopeSQL(t *testing.T) {
	tests := []struct {
		name    string
		mode    txMode
		domains []string
		want    string
		wantErr bool
	}{
		{
			name:    "domain",
			mode:    txModeDomain,
			domains: []string{"identity"},
			want:    "BEGIN DOMAIN identity",
		},
		{
			name:    "cross domain",
			mode:    txModeCross,
			domains: []string{"payments", "ledger"},
			want:    "BEGIN CROSS DOMAIN payments, ledger",
		},
		{
			name:    "missing domain",
			mode:    txModeDomain,
			domains: nil,
			wantErr: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := beginScopeSQL(test.mode, test.domains)
			if test.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("beginScopeSQL() error = %v", err)
			}
			if got != test.want {
				t.Fatalf("beginScopeSQL() = %q, want %q", got, test.want)
			}
		})
	}
}
