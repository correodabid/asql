package executor

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/correodabid/asql/internal/engine/parser"
	"github.com/correodabid/asql/internal/engine/planner"
	"github.com/correodabid/asql/internal/engine/ports"
	"github.com/correodabid/asql/internal/engine/sqlerr"
)

// PrincipalKind identifies the kind of durable database principal.
type PrincipalKind string

const (
	PrincipalKindUser PrincipalKind = "USER"
	PrincipalKindRole PrincipalKind = "ROLE"
)

// PrincipalPrivilege is an explicit privilege granted to a user or role.
type PrincipalPrivilege string

const (
	PrincipalPrivilegeAdmin         PrincipalPrivilege = "ADMIN"
	PrincipalPrivilegeSelectHistory PrincipalPrivilege = "SELECT_HISTORY"
)

var (
	errPrincipalExists              = sqlerr.New("42710", "principal already exists")
	errPrincipalNotFound            = sqlerr.New("28000", "principal not found")
	errPrincipalDisabled            = sqlerr.New("28000", "principal is disabled")
	errPrincipalAuthFailed          = sqlerr.New("28P01", "principal authentication failed")
	errPrincipalCatalogBootstrapped = sqlerr.New("55000", "principal catalog already initialized")
	errRoleRequired                 = sqlerr.New("42601", "role principal is required")
	errUserRequired                 = sqlerr.New("42601", "user principal is required")
	errInvalidPrincipalName         = sqlerr.New("42601", "principal name is required")
	errPasswordRequired             = sqlerr.New("42601", "password is required")
	errPrincipalHistoryDenied       = sqlerr.New("42501", "SELECT_HISTORY privilege required")
	errPrincipalDeleteEnabled       = sqlerr.New("55000", "principal must be disabled before deletion")
	errPrincipalDeleteLastPrincipal = sqlerr.New("55000", "cannot delete the last principal in the catalog")
	errPrincipalDeleteNotEmpty      = sqlerr.New("2BP01", "principal still has direct roles or privileges")
	errPrincipalDeleteReferenced    = sqlerr.New("2BP01", "principal is still granted to other principals")
	errPrincipalAuthzRequired       = sqlerr.New("42501", "permission denied: authenticated principal required")
)

type principalState struct {
	name         string
	kind         PrincipalKind
	passwordHash string
	enabled      bool
	roles        map[string]struct{}
	privileges   map[PrincipalPrivilege]struct{}
}

// PrincipalInfo is a read-only view of a durable principal.
type PrincipalInfo struct {
	Name                string
	Kind                PrincipalKind
	Enabled             bool
	Roles               []string
	EffectiveRoles      []string
	ReferencedBy        []string
	Privileges          []PrincipalPrivilege
	EffectivePrivileges []PrincipalPrivilege
}

type securityMutationPayload struct {
	Action       string             `json:"action"`
	Principal    string             `json:"principal,omitempty"`
	Kind         PrincipalKind      `json:"kind,omitempty"`
	PasswordHash string             `json:"password_hash,omitempty"`
	Enabled      *bool              `json:"enabled,omitempty"`
	Role         string             `json:"role,omitempty"`
	Privilege    PrincipalPrivilege `json:"privilege,omitempty"`
}

func normalizePrincipalName(name string) string {
	return strings.ToLower(strings.TrimSpace(name))
}

func hashPrincipalPassword(password string) string {
	sum := sha256.Sum256([]byte(password))
	return hex.EncodeToString(sum[:])
}

func encodeSecurityMutationPayload(payload securityMutationPayload) []byte {
	data, err := json.Marshal(payload)
	if err != nil {
		panic(err)
	}
	return data
}

func decodeSecurityMutationPayload(data []byte) (securityMutationPayload, error) {
	var payload securityMutationPayload
	if err := json.Unmarshal(data, &payload); err != nil {
		return securityMutationPayload{}, err
	}
	payload.Principal = normalizePrincipalName(payload.Principal)
	payload.Role = normalizePrincipalName(payload.Role)
	return payload, nil
}

func (engine *Engine) currentPrincipalState(name string) (*principalState, bool) {
	state := engine.readState.Load()
	if state == nil {
		return nil, false
	}
	principal, ok := state.principals[normalizePrincipalName(name)]
	return principal, ok
}

// Principal returns a read-only view of the named principal.
func (engine *Engine) Principal(name string) (PrincipalInfo, bool) {
	state := engine.readState.Load()
	if state == nil {
		return PrincipalInfo{}, false
	}
	principal, ok := state.principals[normalizePrincipalName(name)]
	if !ok || principal == nil {
		return PrincipalInfo{}, false
	}
	return state.principalInfo(principal), true
}

// ListPrincipals returns all known principals in deterministic order.
func (engine *Engine) ListPrincipals() []PrincipalInfo {
	state := engine.readState.Load()
	if state == nil || len(state.principals) == 0 {
		return nil
	}
	result := make([]PrincipalInfo, 0, len(state.principals))
	for _, principal := range state.principals {
		if principal == nil {
			continue
		}
		result = append(result, state.principalInfo(principal))
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Name < result[j].Name })
	return result
}

// BootstrapAdminPrincipal creates the first admin principal when the catalog is empty.
func (engine *Engine) BootstrapAdminPrincipal(ctx context.Context, username, password string) error {
	username = normalizePrincipalName(username)
	if username == "" {
		return errInvalidPrincipalName
	}
	if strings.TrimSpace(password) == "" {
		return errPasswordRequired
	}
	payload := securityMutationPayload{
		Action:       "principal_create",
		Principal:    username,
		Kind:         PrincipalKindUser,
		PasswordHash: hashPrincipalPassword(password),
	}
	if err := engine.appendSecurityMutation(ctx, payload, func(state *readableState) error {
		if len(state.principals) > 0 {
			return errPrincipalCatalogBootstrapped
		}
		if err := applySecurityMutation(state, payload); err != nil {
			return err
		}
		principal := state.principals[username]
		if principal.privileges == nil {
			principal.privileges = make(map[PrincipalPrivilege]struct{})
		}
		principal.privileges[PrincipalPrivilegeAdmin] = struct{}{}
		principal.privileges[PrincipalPrivilegeSelectHistory] = struct{}{}
		return nil
	}); err != nil {
		return err
	}
	return nil
}

// HasPrincipalCatalog reports whether durable database principals exist.
func (engine *Engine) HasPrincipalCatalog() bool {
	state := engine.readState.Load()
	return state != nil && len(state.principals) > 0
}

// AuthorizeHistoricalRead enforces explicit temporal-read authorization when a
// durable principal catalog is present.
func (engine *Engine) AuthorizeHistoricalRead(principal string) error {
	state := engine.readState.Load()
	if state == nil || len(state.principals) == 0 {
		return nil
	}
	if !state.hasPrincipalPrivilege(principal, PrincipalPrivilegeSelectHistory) {
		return errPrincipalHistoryDenied
	}
	return nil
}

// AuthorizePrincipalPrivilege requires an authenticated enabled principal to
// hold the requested explicit privilege when the durable principal catalog is
// present.
func (engine *Engine) AuthorizePrincipalPrivilege(principal string, privilege PrincipalPrivilege, capability string) error {
	state := engine.readState.Load()
	if state == nil || len(state.principals) == 0 {
		return nil
	}
	principal = normalizePrincipalName(principal)
	if principal == "" {
		return errPrincipalAuthzRequired
	}
	entry, ok := state.principals[principal]
	if !ok || entry == nil || !entry.enabled {
		return errPrincipalAuthzRequired
	}
	if !state.hasPrincipalPrivilege(principal, privilege) {
		return fmt.Errorf("permission denied: %s privilege required for %s", privilege, capability)
	}
	return nil
}

// AuthorizeSQL resolves a SQL statement into a plan and applies the current
// durable-principal privilege surface to that plan.
func (engine *Engine) AuthorizeSQL(principal, sql string, txDomains []string) (planner.Plan, error) {
	_, stripped, err := parser.ExtractImports(sql)
	if err != nil {
		return planner.Plan{}, fmt.Errorf("extract imports: %w", err)
	}
	statement, err := parser.Parse(stripped)
	if err != nil {
		return planner.Plan{}, fmt.Errorf("parse sql %q: %w", stripped, err)
	}
	txDomains = engine.expandDomainsForVFKJoins(statement, txDomains)
	plan, err := planner.BuildForDomains(statement, txDomains)
	if err != nil {
		return planner.Plan{}, fmt.Errorf("plan sql %q: %w", stripped, err)
	}
	if err := engine.AuthorizePlan(principal, plan); err != nil {
		return planner.Plan{}, err
	}
	return plan, nil
}

// AuthorizePlan applies the current MVP privilege surface to a resolved plan.
// When no durable principal catalog exists, compatibility fallback is to allow
// the operation.
func (engine *Engine) AuthorizePlan(principal string, plan planner.Plan) error {
	state := engine.readState.Load()
	if state == nil || len(state.principals) == 0 {
		return nil
	}
	principal = normalizePrincipalName(principal)
	if principal == "" {
		return errPrincipalAuthzRequired
	}
	entry, ok := state.principals[principal]
	if !ok || entry == nil || !entry.enabled {
		return errPrincipalAuthzRequired
	}
	if planRequiresAdmin(plan.Operation) && !state.hasPrincipalPrivilege(principal, PrincipalPrivilegeAdmin) {
		return fmt.Errorf("permission denied: %s privilege required for %s", PrincipalPrivilegeAdmin, describeAuthorizedOperation(plan.Operation))
	}
	return nil
}

func planRequiresAdmin(operation planner.Operation) bool {
	switch operation {
	case planner.OperationCreateTable,
		planner.OperationAlterTableAddColumn,
		planner.OperationAlterTableDropColumn,
		planner.OperationAlterTableRenameColumn,
		planner.OperationCreateIndex,
		planner.OperationInsert,
		planner.OperationUpdate,
		planner.OperationDelete,
		planner.OperationCreateEntity,
		planner.OperationDropTable,
		planner.OperationDropIndex,
		planner.OperationTruncateTable:
		return true
	default:
		return false
	}
}

func describeAuthorizedOperation(operation planner.Operation) string {
	switch operation {
	case planner.OperationCreateTable:
		return "CREATE TABLE"
	case planner.OperationAlterTableAddColumn, planner.OperationAlterTableDropColumn, planner.OperationAlterTableRenameColumn:
		return "ALTER TABLE"
	case planner.OperationCreateIndex:
		return "CREATE INDEX"
	case planner.OperationInsert:
		return "INSERT"
	case planner.OperationUpdate:
		return "UPDATE"
	case planner.OperationDelete:
		return "DELETE"
	case planner.OperationCreateEntity:
		return "CREATE ENTITY"
	case planner.OperationDropTable:
		return "DROP TABLE"
	case planner.OperationDropIndex:
		return "DROP INDEX"
	case planner.OperationTruncateTable:
		return "TRUNCATE TABLE"
	case planner.OperationSelect, planner.OperationSetOp:
		return "SELECT"
	default:
		return strings.ToUpper(strings.ReplaceAll(string(operation), "_", " "))
	}
}

// CreateUser persists a durable database user.
func (engine *Engine) CreateUser(ctx context.Context, username, password string) error {
	username = normalizePrincipalName(username)
	if username == "" {
		return errInvalidPrincipalName
	}
	if strings.TrimSpace(password) == "" {
		return errPasswordRequired
	}
	payload := securityMutationPayload{
		Action:       "principal_create",
		Principal:    username,
		Kind:         PrincipalKindUser,
		PasswordHash: hashPrincipalPassword(password),
	}
	return engine.appendSecurityMutation(ctx, payload, func(state *readableState) error {
		return applySecurityMutation(state, payload)
	})
}

// CreateRole persists a durable database role.
func (engine *Engine) CreateRole(ctx context.Context, role string) error {
	role = normalizePrincipalName(role)
	if role == "" {
		return errInvalidPrincipalName
	}
	payload := securityMutationPayload{
		Action:    "principal_create",
		Principal: role,
		Kind:      PrincipalKindRole,
	}
	return engine.appendSecurityMutation(ctx, payload, func(state *readableState) error {
		return applySecurityMutation(state, payload)
	})
}

// DisablePrincipal disables logins and privilege use for a principal.
func (engine *Engine) DisablePrincipal(ctx context.Context, username string) error {
	username = normalizePrincipalName(username)
	disabled := false
	payload := securityMutationPayload{
		Action:    "principal_disable",
		Principal: username,
		Enabled:   &disabled,
	}
	return engine.appendSecurityMutation(ctx, payload, func(state *readableState) error {
		return applySecurityMutation(state, payload)
	})
}

// EnablePrincipal re-enables logins and privilege use for a principal.
func (engine *Engine) EnablePrincipal(ctx context.Context, username string) error {
	username = normalizePrincipalName(username)
	enabled := true
	payload := securityMutationPayload{
		Action:    "principal_enable",
		Principal: username,
		Enabled:   &enabled,
	}
	return engine.appendSecurityMutation(ctx, payload, func(state *readableState) error {
		return applySecurityMutation(state, payload)
	})
}

// DeletePrincipal removes a durable principal once it is disabled, ungranted,
// and no longer referenced by other principals.
func (engine *Engine) DeletePrincipal(ctx context.Context, username string) error {
	username = normalizePrincipalName(username)
	payload := securityMutationPayload{
		Action:    "principal_delete",
		Principal: username,
	}
	return engine.appendSecurityMutation(ctx, payload, func(state *readableState) error {
		return applySecurityMutation(state, payload)
	})
}

// GrantPrivilege adds an explicit privilege to a user or role.
func (engine *Engine) GrantPrivilege(ctx context.Context, principal string, privilege PrincipalPrivilege) error {
	principal = normalizePrincipalName(principal)
	payload := securityMutationPayload{
		Action:    "privilege_grant",
		Principal: principal,
		Privilege: privilege,
	}
	return engine.appendSecurityMutation(ctx, payload, func(state *readableState) error {
		return applySecurityMutation(state, payload)
	})
}

// RevokePrivilege removes an explicit privilege from a user or role.
func (engine *Engine) RevokePrivilege(ctx context.Context, principal string, privilege PrincipalPrivilege) error {
	principal = normalizePrincipalName(principal)
	payload := securityMutationPayload{
		Action:    "privilege_revoke",
		Principal: principal,
		Privilege: privilege,
	}
	return engine.appendSecurityMutation(ctx, payload, func(state *readableState) error {
		return applySecurityMutation(state, payload)
	})
}

// GrantRole grants a role principal to a user or role.
func (engine *Engine) GrantRole(ctx context.Context, principal, role string) error {
	principal = normalizePrincipalName(principal)
	role = normalizePrincipalName(role)
	payload := securityMutationPayload{
		Action:    "role_grant",
		Principal: principal,
		Role:      role,
	}
	return engine.appendSecurityMutation(ctx, payload, func(state *readableState) error {
		return applySecurityMutation(state, payload)
	})
}

// RevokeRole removes a direct role grant from a user or role.
func (engine *Engine) RevokeRole(ctx context.Context, principal, role string) error {
	principal = normalizePrincipalName(principal)
	role = normalizePrincipalName(role)
	payload := securityMutationPayload{
		Action:    "role_revoke",
		Principal: principal,
		Role:      role,
	}
	return engine.appendSecurityMutation(ctx, payload, func(state *readableState) error {
		return applySecurityMutation(state, payload)
	})
}

// SetPrincipalPassword rotates the stored password for a durable database user.
func (engine *Engine) SetPrincipalPassword(ctx context.Context, username, password string) error {
	username = normalizePrincipalName(username)
	if username == "" {
		return errInvalidPrincipalName
	}
	if strings.TrimSpace(password) == "" {
		return errPasswordRequired
	}
	payload := securityMutationPayload{
		Action:       "principal_password_set",
		Principal:    username,
		PasswordHash: hashPrincipalPassword(password),
	}
	return engine.appendSecurityMutation(ctx, payload, func(state *readableState) error {
		return applySecurityMutation(state, payload)
	})
}

// AuthenticatePrincipal verifies stored credentials for a durable principal.
func (engine *Engine) AuthenticatePrincipal(username, password string) (PrincipalInfo, error) {
	principal, ok := engine.currentPrincipalState(username)
	if !ok || principal == nil {
		return PrincipalInfo{}, errPrincipalAuthFailed
	}
	if !principal.enabled {
		return PrincipalInfo{}, errPrincipalDisabled
	}
	if principal.kind != PrincipalKindUser {
		return PrincipalInfo{}, errPrincipalAuthFailed
	}
	if principal.passwordHash != hashPrincipalPassword(password) {
		return PrincipalInfo{}, errPrincipalAuthFailed
	}
	state := engine.readState.Load()
	if state == nil {
		return PrincipalInfo{}, errPrincipalAuthFailed
	}
	return state.principalInfo(principal), nil
}

// HasPrincipalPrivilege evaluates effective privileges against current principal state.
func (engine *Engine) HasPrincipalPrivilege(principal string, privilege PrincipalPrivilege) bool {
	state := engine.readState.Load()
	if state == nil {
		return false
	}
	return state.hasPrincipalPrivilege(principal, privilege)
}

// ParsePrincipalPrivilege converts textual privilege names into supported values.
func ParsePrincipalPrivilege(value string) (PrincipalPrivilege, error) {
	switch strings.ToUpper(strings.TrimSpace(value)) {
	case string(PrincipalPrivilegeAdmin):
		return PrincipalPrivilegeAdmin, nil
	case string(PrincipalPrivilegeSelectHistory):
		return PrincipalPrivilegeSelectHistory, nil
	default:
		return "", fmt.Errorf("unsupported privilege %q", value)
	}
}

func (state *readableState) hasPrincipalPrivilege(principal string, privilege PrincipalPrivilege) bool {
	principal = normalizePrincipalName(principal)
	if principal == "" || len(state.principals) == 0 {
		return false
	}
	visited := make(map[string]struct{})
	return state.hasPrincipalPrivilegeRecursive(principal, privilege, visited)
}

func (state *readableState) hasPrincipalPrivilegeRecursive(principal string, privilege PrincipalPrivilege, visited map[string]struct{}) bool {
	if _, seen := visited[principal]; seen {
		return false
	}
	visited[principal] = struct{}{}
	entry, ok := state.principals[principal]
	if !ok || entry == nil || !entry.enabled {
		return false
	}
	if _, ok := entry.privileges[privilege]; ok {
		return true
	}
	for role := range entry.roles {
		if state.hasPrincipalPrivilegeRecursive(role, privilege, visited) {
			return true
		}
	}
	return false
}

func (state *readableState) effectivePrincipalRoles(principal string) []string {
	principal = normalizePrincipalName(principal)
	if principal == "" || len(state.principals) == 0 {
		return nil
	}
	entry, ok := state.principals[principal]
	if !ok || entry == nil || !entry.enabled {
		return nil
	}
	visited := make(map[string]struct{})
	roles := make([]string, 0)
	state.collectPrincipalRoles(principal, visited, &roles)
	sort.Strings(roles)
	return roles
}

func (state *readableState) principalReferencedBy(principal string) []string {
	principal = normalizePrincipalName(principal)
	if principal == "" || len(state.principals) == 0 {
		return nil
	}
	references := make([]string, 0)
	for name, entry := range state.principals {
		if entry == nil {
			continue
		}
		if _, ok := entry.roles[principal]; ok {
			references = append(references, name)
		}
	}
	sort.Strings(references)
	return references
}

func (state *readableState) collectPrincipalRoles(principal string, visited map[string]struct{}, roles *[]string) {
	entry, ok := state.principals[principal]
	if !ok || entry == nil || !entry.enabled {
		return
	}
	for roleName := range entry.roles {
		roleName = normalizePrincipalName(roleName)
		if _, seen := visited[roleName]; seen {
			continue
		}
		role, ok := state.principals[roleName]
		if !ok || role == nil || !role.enabled {
			continue
		}
		visited[roleName] = struct{}{}
		*roles = append(*roles, roleName)
		state.collectPrincipalRoles(roleName, visited, roles)
	}
}

func (state *readableState) principalInfo(principal *principalState) PrincipalInfo {
	roles := make([]string, 0, len(principal.roles))
	for role := range principal.roles {
		roles = append(roles, role)
	}
	sort.Strings(roles)
	privileges := make([]PrincipalPrivilege, 0, len(principal.privileges))
	for privilege := range principal.privileges {
		privileges = append(privileges, privilege)
	}
	sort.Slice(privileges, func(i, j int) bool { return privileges[i] < privileges[j] })
	effectivePrivileges := make([]PrincipalPrivilege, 0, len(privileges))
	for _, privilege := range []PrincipalPrivilege{PrincipalPrivilegeAdmin, PrincipalPrivilegeSelectHistory} {
		if state.hasPrincipalPrivilege(principal.name, privilege) {
			effectivePrivileges = append(effectivePrivileges, privilege)
		}
	}
	effectiveRoles := state.effectivePrincipalRoles(principal.name)
	referencedBy := state.principalReferencedBy(principal.name)
	return PrincipalInfo{
		Name:                principal.name,
		Kind:                principal.kind,
		Enabled:             principal.enabled,
		Roles:               roles,
		EffectiveRoles:      effectiveRoles,
		ReferencedBy:        referencedBy,
		Privileges:          privileges,
		EffectivePrivileges: effectivePrivileges,
	}
}

func applySecurityMutation(state *readableState, payload securityMutationPayload) error {
	if state.principals == nil {
		state.principals = make(map[string]*principalState)
	}
	principalName := normalizePrincipalName(payload.Principal)
	switch payload.Action {
	case "principal_create":
		if principalName == "" {
			return errInvalidPrincipalName
		}
		if payload.Kind != PrincipalKindUser && payload.Kind != PrincipalKindRole {
			return fmt.Errorf("invalid principal kind %q", payload.Kind)
		}
		if _, exists := state.principals[principalName]; exists {
			return errPrincipalExists
		}
		state.principals[principalName] = &principalState{
			name:         principalName,
			kind:         payload.Kind,
			passwordHash: payload.PasswordHash,
			enabled:      true,
			roles:        make(map[string]struct{}),
			privileges:   make(map[PrincipalPrivilege]struct{}),
		}
		return nil
	case "principal_disable", "principal_enable":
		principal, ok := state.principals[principalName]
		if !ok {
			return errPrincipalNotFound
		}
		principal.enabled = payload.Action == "principal_enable"
		if payload.Enabled != nil {
			principal.enabled = *payload.Enabled
		}
		return nil
	case "principal_delete":
		principal, ok := state.principals[principalName]
		if !ok {
			return errPrincipalNotFound
		}
		if len(state.principals) == 1 {
			return errPrincipalDeleteLastPrincipal
		}
		if principal.enabled {
			return errPrincipalDeleteEnabled
		}
		if len(principal.roles) > 0 || len(principal.privileges) > 0 {
			return errPrincipalDeleteNotEmpty
		}
		if refs := state.principalReferencedBy(principalName); len(refs) > 0 {
			return fmt.Errorf("%w: %s", errPrincipalDeleteReferenced, strings.Join(refs, ", "))
		}
		delete(state.principals, principalName)
		return nil
	case "privilege_grant":
		principal, ok := state.principals[principalName]
		if !ok {
			return errPrincipalNotFound
		}
		if principal.privileges == nil {
			principal.privileges = make(map[PrincipalPrivilege]struct{})
		}
		principal.privileges[payload.Privilege] = struct{}{}
		return nil
	case "privilege_revoke":
		principal, ok := state.principals[principalName]
		if !ok {
			return errPrincipalNotFound
		}
		delete(principal.privileges, payload.Privilege)
		return nil
	case "role_grant":
		principal, ok := state.principals[principalName]
		if !ok {
			return errPrincipalNotFound
		}
		role, ok := state.principals[payload.Role]
		if !ok {
			return errPrincipalNotFound
		}
		if role.kind != PrincipalKindRole {
			return errRoleRequired
		}
		if principal.roles == nil {
			principal.roles = make(map[string]struct{})
		}
		principal.roles[payload.Role] = struct{}{}
		return nil
	case "role_revoke":
		principal, ok := state.principals[principalName]
		if !ok {
			return errPrincipalNotFound
		}
		delete(principal.roles, payload.Role)
		return nil
	case "principal_password_set":
		principal, ok := state.principals[principalName]
		if !ok {
			return errPrincipalNotFound
		}
		if principal.kind != PrincipalKindUser {
			return errUserRequired
		}
		principal.passwordHash = payload.PasswordHash
		return nil
	default:
		return fmt.Errorf("unsupported security action %q", payload.Action)
	}
}

func (engine *Engine) appendSecurityMutation(ctx context.Context, payload securityMutationPayload, apply func(state *readableState) error) error {
	if engine == nil {
		return errors.New("engine is required")
	}
	encoded := encodeSecurityMutationPayload(payload)

	engine.writeMu.Lock()
	defer engine.writeMu.Unlock()

	current := engine.readState.Load()
	if current == nil {
		current = &readableState{domains: make(map[string]*domainState), principals: make(map[string]*principalState)}
	}
	newState := current.cloneForMutation(allDomainKeys(current.domains))
	if newState.principals == nil {
		newState.principals = make(map[string]*principalState)
	}
	if err := apply(newState); err != nil {
		return err
	}

	timestamp := engine.nextLogicalTimestamp()
	record := ports.WALRecord{Type: walTypeSecurity, Timestamp: timestamp, Payload: encoded}
	lsn, err := engine.logStore.Append(ctx, record)
	if err != nil {
		engine.logicalTS--
		return fmt.Errorf("append security wal record: %w", err)
	}
	record.LSN = lsn
	engine.headLSN = lsn
	newState.headLSN = lsn
	newState.logicalTS = engine.logicalTS
	engine.readState.Store(newState)
	engine.appendWALRecordCache([]ports.WALRecord{record})
	engine.clearHistoricalStateCache()
	return nil
}
