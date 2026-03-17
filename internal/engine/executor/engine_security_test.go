package executor

import (
	"context"
	"path/filepath"
	"strings"
	"testing"

	"asql/internal/engine/planner"
	"asql/internal/storage/wal"
)

func TestAuthorizePlanUsesAuthenticatedReadAndAdminMutationModel(t *testing.T) {
	ctx := context.Background()
	baseDir := t.TempDir()
	walPath := filepath.Join(baseDir, "security-authz.wal")

	store, err := wal.NewSegmentedLogStore(walPath, wal.AlwaysSync{})
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	defer store.Close()

	engine, err := New(ctx, store, "")
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}

	if err := engine.BootstrapAdminPrincipal(ctx, "admin", "secret"); err != nil {
		t.Fatalf("bootstrap admin: %v", err)
	}
	if err := engine.CreateUser(ctx, "analyst", "analyst-secret"); err != nil {
		t.Fatalf("create analyst: %v", err)
	}

	if err := engine.AuthorizePlan("analyst", planner.Plan{Operation: planner.OperationSelect}); err != nil {
		t.Fatalf("authorize current read: %v", err)
	}
	if err := engine.AuthorizePlan("admin", planner.Plan{Operation: planner.OperationCreateTable}); err != nil {
		t.Fatalf("authorize admin schema mutation: %v", err)
	}
	err = engine.AuthorizePlan("analyst", planner.Plan{Operation: planner.OperationInsert})
	if err == nil || !strings.Contains(err.Error(), "ADMIN privilege required") {
		t.Fatalf("expected ADMIN privilege denial for analyst insert, got %v", err)
	}
	err = engine.AuthorizePlan("", planner.Plan{Operation: planner.OperationSelect})
	if err == nil || !strings.Contains(err.Error(), "authenticated principal required") {
		t.Fatalf("expected authenticated principal denial, got %v", err)
	}
}

func TestPrincipalCatalogReplayPersistsUsersAndPrivileges(t *testing.T) {
	ctx := context.Background()
	baseDir := t.TempDir()
	walPath := filepath.Join(baseDir, "security-replay.wal")
	snapDir := filepath.Join(baseDir, "snaps")

	store, err := wal.NewSegmentedLogStore(walPath, wal.AlwaysSync{})
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	engine, err := New(ctx, store, snapDir)
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}

	if err := engine.BootstrapAdminPrincipal(ctx, "admin", "secret"); err != nil {
		t.Fatalf("bootstrap admin: %v", err)
	}
	if err := engine.CreateUser(ctx, "analyst", "analyst-secret"); err != nil {
		t.Fatalf("create user: %v", err)
	}
	if err := engine.GrantPrivilege(ctx, "analyst", PrincipalPrivilegeSelectHistory); err != nil {
		t.Fatalf("grant history privilege: %v", err)
	}

	if _, err := engine.AuthenticatePrincipal("admin", "secret"); err != nil {
		t.Fatalf("authenticate admin before restart: %v", err)
	}
	if _, err := engine.AuthenticatePrincipal("analyst", "analyst-secret"); err != nil {
		t.Fatalf("authenticate analyst before restart: %v", err)
	}
	if !engine.HasPrincipalPrivilege("analyst", PrincipalPrivilegeSelectHistory) {
		t.Fatal("expected analyst to have SELECT_HISTORY before restart")
	}

	engine.WaitPendingSnapshots()
	_ = store.Close()

	reopenedStore, err := wal.NewSegmentedLogStore(walPath, wal.AlwaysSync{})
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	defer reopenedStore.Close()

	replayed, err := New(ctx, reopenedStore, snapDir)
	if err != nil {
		t.Fatalf("new replayed engine: %v", err)
	}

	if _, err := replayed.AuthenticatePrincipal("admin", "secret"); err != nil {
		t.Fatalf("authenticate admin after restart: %v", err)
	}
	if _, err := replayed.AuthenticatePrincipal("analyst", "analyst-secret"); err != nil {
		t.Fatalf("authenticate analyst after restart: %v", err)
	}
	if !replayed.HasPrincipalPrivilege("analyst", PrincipalPrivilegeSelectHistory) {
		t.Fatal("expected analyst to keep SELECT_HISTORY after restart")
	}
}

func TestPrincipalSnapshotBinaryRoundTripPreservesCatalog(t *testing.T) {
	ctx := context.Background()
	baseDir := t.TempDir()
	walPath := filepath.Join(baseDir, "security-snapshot.wal")

	store, err := wal.NewSegmentedLogStore(walPath, wal.AlwaysSync{})
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	defer store.Close()

	engine, err := New(ctx, store, "")
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}

	if err := engine.BootstrapAdminPrincipal(ctx, "admin", "secret"); err != nil {
		t.Fatalf("bootstrap admin: %v", err)
	}
	if err := engine.CreateUser(ctx, "reader", "reader-secret"); err != nil {
		t.Fatalf("create reader: %v", err)
	}
	if err := engine.GrantPrivilege(ctx, "reader", PrincipalPrivilegeSelectHistory); err != nil {
		t.Fatalf("grant history privilege: %v", err)
	}

	snap := captureSnapshot(engine.readState.Load(), engine.catalog)
	data, err := encodeSnapshotFileBinary(&snap, true, 0)
	if err != nil {
		t.Fatalf("encode snapshot: %v", err)
	}
	decoded, ok, err := decodeSingleFullSnapshotBinary(data)
	if err != nil {
		t.Fatalf("decode snapshot: %v", err)
	}
	if !ok || len(decoded) != 1 {
		t.Fatalf("expected single full snapshot decode, ok=%v len=%d", ok, len(decoded))
	}

	principal, ok := decoded[0].state.principals["reader"]
	if !ok || principal == nil {
		t.Fatal("expected reader principal in decoded snapshot")
	}
	if !principal.enabled {
		t.Fatal("expected decoded reader principal to be enabled")
	}
	if _, ok := principal.privileges[PrincipalPrivilegeSelectHistory]; !ok {
		t.Fatal("expected decoded reader principal to keep SELECT_HISTORY")
	}
}

func TestPrincipalRoleRevocationAndPasswordRotationPersistAcrossRestart(t *testing.T) {
	ctx := context.Background()
	baseDir := t.TempDir()
	walPath := filepath.Join(baseDir, "security-mutations.wal")
	snapDir := filepath.Join(baseDir, "snaps")

	store, err := wal.NewSegmentedLogStore(walPath, wal.AlwaysSync{})
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	engine, err := New(ctx, store, snapDir)
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}

	if err := engine.BootstrapAdminPrincipal(ctx, "admin", "secret"); err != nil {
		t.Fatalf("bootstrap admin: %v", err)
	}
	if err := engine.CreateRole(ctx, "history_readers"); err != nil {
		t.Fatalf("create role: %v", err)
	}
	if err := engine.GrantPrivilege(ctx, "history_readers", PrincipalPrivilegeSelectHistory); err != nil {
		t.Fatalf("grant privilege: %v", err)
	}
	if err := engine.CreateUser(ctx, "analyst", "analyst-pass"); err != nil {
		t.Fatalf("create user: %v", err)
	}
	if err := engine.GrantRole(ctx, "analyst", "history_readers"); err != nil {
		t.Fatalf("grant role: %v", err)
	}
	if !engine.HasPrincipalPrivilege("analyst", PrincipalPrivilegeSelectHistory) {
		t.Fatal("expected analyst to inherit SELECT_HISTORY before revoke")
	}
	if err := engine.RevokeRole(ctx, "analyst", "history_readers"); err != nil {
		t.Fatalf("revoke role: %v", err)
	}
	if engine.HasPrincipalPrivilege("analyst", PrincipalPrivilegeSelectHistory) {
		t.Fatal("expected analyst to lose SELECT_HISTORY after revoke")
	}
	if err := engine.SetPrincipalPassword(ctx, "analyst", "rotated-pass"); err != nil {
		t.Fatalf("set principal password: %v", err)
	}
	if _, err := engine.AuthenticatePrincipal("analyst", "analyst-pass"); err == nil {
		t.Fatal("expected old password to stop working after rotation")
	}
	if _, err := engine.AuthenticatePrincipal("analyst", "rotated-pass"); err != nil {
		t.Fatalf("authenticate with rotated password before restart: %v", err)
	}

	engine.WaitPendingSnapshots()
	_ = store.Close()

	reopenedStore, err := wal.NewSegmentedLogStore(walPath, wal.AlwaysSync{})
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	defer reopenedStore.Close()

	replayed, err := New(ctx, reopenedStore, snapDir)
	if err != nil {
		t.Fatalf("new replayed engine: %v", err)
	}
	if replayed.HasPrincipalPrivilege("analyst", PrincipalPrivilegeSelectHistory) {
		t.Fatal("expected replayed analyst to remain without SELECT_HISTORY after role revoke")
	}
	if _, err := replayed.AuthenticatePrincipal("analyst", "analyst-pass"); err == nil {
		t.Fatal("expected old password to fail after restart")
	}
	if _, err := replayed.AuthenticatePrincipal("analyst", "rotated-pass"); err != nil {
		t.Fatalf("authenticate with rotated password after restart: %v", err)
	}
}

func TestPrincipalEnableRestoresAuthenticationAndEffectiveRoles(t *testing.T) {
	ctx := context.Background()
	baseDir := t.TempDir()
	walPath := filepath.Join(baseDir, "security-enable.wal")

	store, err := wal.NewSegmentedLogStore(walPath, wal.AlwaysSync{})
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	defer store.Close()

	engine, err := New(ctx, store, "")
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}

	if err := engine.CreateRole(ctx, "history_readers"); err != nil {
		t.Fatalf("create role: %v", err)
	}
	if err := engine.CreateRole(ctx, "auditors"); err != nil {
		t.Fatalf("create nested role: %v", err)
	}
	if err := engine.GrantRole(ctx, "history_readers", "auditors"); err != nil {
		t.Fatalf("grant nested role: %v", err)
	}
	if err := engine.CreateUser(ctx, "analyst", "analyst-pass"); err != nil {
		t.Fatalf("create user: %v", err)
	}
	if err := engine.GrantRole(ctx, "analyst", "history_readers"); err != nil {
		t.Fatalf("grant role: %v", err)
	}
	if err := engine.DisablePrincipal(ctx, "analyst"); err != nil {
		t.Fatalf("disable principal: %v", err)
	}
	if _, err := engine.AuthenticatePrincipal("analyst", "analyst-pass"); err == nil {
		t.Fatal("expected disabled principal authentication to fail")
	}
	if err := engine.EnablePrincipal(ctx, "analyst"); err != nil {
		t.Fatalf("enable principal: %v", err)
	}
	info, err := engine.AuthenticatePrincipal("analyst", "analyst-pass")
	if err != nil {
		t.Fatalf("authenticate enabled principal: %v", err)
	}
	if len(info.EffectiveRoles) != 2 || info.EffectiveRoles[0] != "auditors" || info.EffectiveRoles[1] != "history_readers" {
		t.Fatalf("unexpected effective roles after enable: %+v", info)
	}
}

func TestPrincipalDeleteRequiresDisabledEmptyAndUnreferenced(t *testing.T) {
	ctx := context.Background()
	baseDir := t.TempDir()
	walPath := filepath.Join(baseDir, "security-delete.wal")
	snapDir := filepath.Join(baseDir, "snaps")

	store, err := wal.NewSegmentedLogStore(walPath, wal.AlwaysSync{})
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	engine, err := New(ctx, store, snapDir)
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}
	if err := engine.BootstrapAdminPrincipal(ctx, "admin", "secret"); err != nil {
		t.Fatalf("bootstrap admin: %v", err)
	}

	if err := engine.CreateRole(ctx, "history_readers"); err != nil {
		t.Fatalf("create role: %v", err)
	}
	if err := engine.CreateUser(ctx, "analyst", "analyst-pass"); err != nil {
		t.Fatalf("create user: %v", err)
	}
	if err := engine.GrantRole(ctx, "analyst", "history_readers"); err != nil {
		t.Fatalf("grant role: %v", err)
	}
	if err := engine.DisablePrincipal(ctx, "history_readers"); err != nil {
		t.Fatalf("disable role: %v", err)
	}
	if err := engine.DeletePrincipal(ctx, "history_readers"); err == nil {
		t.Fatal("expected delete to fail while role is referenced")
	}
	roleInfo, ok := engine.Principal("history_readers")
	if !ok || len(roleInfo.ReferencedBy) != 1 || roleInfo.ReferencedBy[0] != "analyst" {
		t.Fatalf("unexpected role references: %+v ok=%v", roleInfo, ok)
	}
	if err := engine.RevokeRole(ctx, "analyst", "history_readers"); err != nil {
		t.Fatalf("revoke role: %v", err)
	}
	if err := engine.DeletePrincipal(ctx, "history_readers"); err != nil {
		t.Fatalf("delete role: %v", err)
	}
	if _, ok := engine.Principal("history_readers"); ok {
		t.Fatal("expected role to be deleted")
	}
	if err := engine.DeletePrincipal(ctx, "analyst"); err == nil {
		t.Fatal("expected delete to fail while user is enabled")
	}
	if err := engine.DisablePrincipal(ctx, "analyst"); err != nil {
		t.Fatalf("disable user: %v", err)
	}
	if err := engine.DeletePrincipal(ctx, "analyst"); err != nil {
		t.Fatalf("delete user: %v", err)
	}
	if _, ok := engine.Principal("analyst"); ok {
		t.Fatal("expected user to be deleted")
	}

	engine.WaitPendingSnapshots()
	_ = store.Close()

	reopenedStore, err := wal.NewSegmentedLogStore(walPath, wal.AlwaysSync{})
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	defer reopenedStore.Close()

	replayed, err := New(ctx, reopenedStore, snapDir)
	if err != nil {
		t.Fatalf("new replayed engine: %v", err)
	}
	if _, ok := replayed.Principal("history_readers"); ok {
		t.Fatal("expected deleted role to stay deleted after restart")
	}
	if _, ok := replayed.Principal("analyst"); ok {
		t.Fatal("expected deleted user to stay deleted after restart")
	}
	if _, ok := replayed.Principal("admin"); !ok {
		t.Fatal("expected admin principal to remain after restart")
	}

	if err := replayed.DisablePrincipal(ctx, "admin"); err != nil {
		t.Fatalf("disable remaining admin: %v", err)
	}
	if err := replayed.DeletePrincipal(ctx, "admin"); err == nil {
		t.Fatal("expected delete to fail for the last remaining principal")
	}
}
