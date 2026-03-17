package studioapp

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	api "asql/pkg/adminapi"
)

func TestSecurityListPrincipalsUsesAdminAuthToken(t *testing.T) {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/security/principals" {
			t.Fatalf("unexpected path: %s", r.URL.Path)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer studio-secret" {
			t.Fatalf("unexpected authorization header: %q", got)
		}
		_ = json.NewEncoder(w).Encode(api.ListPrincipalsResponse{
			Principals: []api.PrincipalRecord{{Name: "admin", Kind: "USER", Enabled: true}},
		})
	}))
	defer server.Close()

	app := &App{adminEndpoints: []string{server.URL}, adminToken: "studio-secret"}
	resp, err := app.SecurityListPrincipals()
	if err != nil {
		t.Fatalf("SecurityListPrincipals: %v", err)
	}
	principals, ok := resp["principals"].([]interface{})
	if !ok || len(principals) != 1 {
		t.Fatalf("unexpected principals payload: %+v", resp)
	}
}

func TestSecurityMutationsPostJSON(t *testing.T) {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer studio-secret" {
			t.Fatalf("unexpected authorization header: %q", got)
		}
		if r.Method != http.MethodPost {
			t.Fatalf("unexpected method: %s", r.Method)
		}
		var payload map[string]any
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		switch r.URL.Path {
		case "/api/v1/security/bootstrap-admin":
			if payload["principal"] != "admin" || payload["password"] != "secret-pass" {
				t.Fatalf("unexpected bootstrap payload: %+v", payload)
			}
			_ = json.NewEncoder(w).Encode(api.SecurityMutationResponse{Status: "ok", Principal: &api.PrincipalRecord{Name: "admin", Kind: "USER", Enabled: true}})
		case "/api/v1/security/users":
			if payload["principal"] != "analyst" || payload["password"] != "analyst-pass" {
				t.Fatalf("unexpected create user payload: %+v", payload)
			}
			_ = json.NewEncoder(w).Encode(api.SecurityMutationResponse{Status: "ok", Principal: &api.PrincipalRecord{Name: "analyst", Kind: "USER", Enabled: true}})
		case "/api/v1/security/roles":
			if payload["principal"] != "history_readers" {
				t.Fatalf("unexpected create role payload: %+v", payload)
			}
			_ = json.NewEncoder(w).Encode(api.SecurityMutationResponse{Status: "ok", Principal: &api.PrincipalRecord{Name: "history_readers", Kind: "ROLE", Enabled: true}})
		case "/api/v1/security/privileges/grant":
			if payload["principal"] != "history_readers" || payload["privilege"] != "SELECT_HISTORY" {
				t.Fatalf("unexpected grant privilege payload: %+v", payload)
			}
			_ = json.NewEncoder(w).Encode(api.SecurityMutationResponse{Status: "ok", Principal: &api.PrincipalRecord{Name: "history_readers", Kind: "ROLE", Enabled: true}})
		case "/api/v1/security/roles/grant":
			if payload["principal"] != "analyst" || payload["role"] != "history_readers" {
				t.Fatalf("unexpected grant role payload: %+v", payload)
			}
			_ = json.NewEncoder(w).Encode(api.SecurityMutationResponse{Status: "ok", Principal: &api.PrincipalRecord{Name: "analyst", Kind: "USER", Enabled: true}})
		case "/api/v1/security/privileges/revoke":
			if payload["principal"] != "history_readers" || payload["privilege"] != "SELECT_HISTORY" {
				t.Fatalf("unexpected revoke privilege payload: %+v", payload)
			}
			_ = json.NewEncoder(w).Encode(api.SecurityMutationResponse{Status: "ok", Principal: &api.PrincipalRecord{Name: "history_readers", Kind: "ROLE", Enabled: true}})
		case "/api/v1/security/principals/disable":
			if payload["principal"] != "analyst" {
				t.Fatalf("unexpected disable principal payload: %+v", payload)
			}
			_ = json.NewEncoder(w).Encode(api.SecurityMutationResponse{Status: "ok", Principal: &api.PrincipalRecord{Name: "analyst", Kind: "USER", Enabled: false}})
		default:
			t.Fatalf("unexpected path: %s", r.URL.Path)
		}
	}))
	defer server.Close()

	app := &App{adminEndpoints: []string{server.URL}, adminToken: "studio-secret"}
	if _, err := app.SecurityBootstrapAdmin("admin", "secret-pass"); err != nil {
		t.Fatalf("SecurityBootstrapAdmin: %v", err)
	}
	if _, err := app.SecurityCreateUser("analyst", "analyst-pass"); err != nil {
		t.Fatalf("SecurityCreateUser: %v", err)
	}
	if _, err := app.SecurityCreateRole("history_readers"); err != nil {
		t.Fatalf("SecurityCreateRole: %v", err)
	}
	if _, err := app.SecurityGrantPrivilege("history_readers", "SELECT_HISTORY"); err != nil {
		t.Fatalf("SecurityGrantPrivilege: %v", err)
	}
	if _, err := app.SecurityGrantRole("analyst", "history_readers"); err != nil {
		t.Fatalf("SecurityGrantRole: %v", err)
	}
	if _, err := app.SecurityRevokePrivilege("history_readers", "SELECT_HISTORY"); err != nil {
		t.Fatalf("SecurityRevokePrivilege: %v", err)
	}
	if _, err := app.SecurityDisablePrincipal("analyst"); err != nil {
		t.Fatalf("SecurityDisablePrincipal: %v", err)
	}
}
