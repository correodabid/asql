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

	"asql/internal/engine/ports"
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
	errPrincipalExists              = errors.New("principal already exists")
	errPrincipalNotFound            = errors.New("principal not found")
	errPrincipalDisabled            = errors.New("principal is disabled")
	errPrincipalAuthFailed          = errors.New("principal authentication failed")
	errPrincipalCatalogBootstrapped = errors.New("principal catalog already initialized")
	errRoleRequired                 = errors.New("role principal is required")
	errInvalidPrincipalName         = errors.New("principal name is required")
	errPasswordRequired             = errors.New("password is required")
	errPrincipalHistoryDenied       = errors.New("SELECT_HISTORY privilege required")
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
	Name       string
	Kind       PrincipalKind
	Enabled    bool
	Roles      []string
	Privileges []PrincipalPrivilege
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
	principal, ok := engine.currentPrincipalState(name)
	if !ok || principal == nil {
		return PrincipalInfo{}, false
	}
	return principal.info(), true
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
		result = append(result, principal.info())
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
	return principal.info(), nil
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

func (principal *principalState) info() PrincipalInfo {
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
	return PrincipalInfo{
		Name:       principal.name,
		Kind:       principal.kind,
		Enabled:    principal.enabled,
		Roles:      roles,
		Privileges: privileges,
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
	case "principal_disable":
		principal, ok := state.principals[principalName]
		if !ok {
			return errPrincipalNotFound
		}
		principal.enabled = false
		if payload.Enabled != nil {
			principal.enabled = *payload.Enabled
		}
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
