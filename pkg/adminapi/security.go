package adminapi

// PrincipalKind identifies the kind of durable database principal.
type PrincipalKind string

const (
	PrincipalKindUser PrincipalKind = "USER"
	PrincipalKindRole PrincipalKind = "ROLE"
)

// PrincipalPrivilege is a named privilege granted to a principal.
type PrincipalPrivilege string

const (
	PrincipalPrivilegeAdmin         PrincipalPrivilege = "ADMIN"
	PrincipalPrivilegeSelectHistory PrincipalPrivilege = "SELECT_HISTORY"
)

type PrincipalRecord struct {
	Name                string               `json:"name"`
	Kind                PrincipalKind        `json:"kind"`
	Enabled             bool                 `json:"enabled"`
	Roles               []string             `json:"roles,omitempty"`
	EffectiveRoles      []string             `json:"effective_roles,omitempty"`
	ReferencedBy        []string             `json:"referenced_by,omitempty"`
	Privileges          []PrincipalPrivilege `json:"privileges,omitempty"`
	EffectivePrivileges []PrincipalPrivilege `json:"effective_privileges,omitempty"`
}

type ListPrincipalsResponse struct {
	Principals []PrincipalRecord `json:"principals"`
}

type BootstrapAdminPrincipalRequest struct {
	Principal string `json:"principal"`
	Password  string `json:"password"`
}

type CreateUserRequest struct {
	Principal string `json:"principal"`
	Password  string `json:"password"`
}

type CreateRoleRequest struct {
	Principal string `json:"principal"`
}

type GrantPrivilegeRequest struct {
	Principal string `json:"principal"`
	Privilege string `json:"privilege"`
}

type RevokePrivilegeRequest struct {
	Principal string `json:"principal"`
	Privilege string `json:"privilege"`
}

type GrantRoleRequest struct {
	Principal string `json:"principal"`
	Role      string `json:"role"`
}

type RevokeRoleRequest struct {
	Principal string `json:"principal"`
	Role      string `json:"role"`
}

type SetPasswordRequest struct {
	Principal string `json:"principal"`
	Password  string `json:"password"`
}

type DisablePrincipalRequest struct {
	Principal string `json:"principal"`
}

type EnablePrincipalRequest struct {
	Principal string `json:"principal"`
}

type DeletePrincipalRequest struct {
	Principal string `json:"principal"`
}

type SecurityMutationResponse struct {
	Status    string           `json:"status"`
	Principal *PrincipalRecord `json:"principal,omitempty"`
}

type SecurityAuditEvent struct {
	TimestampUTC string         `json:"timestamp_utc"`
	Operation    string         `json:"operation"`
	Status       string         `json:"status"`
	Reason       string         `json:"reason,omitempty"`
	Attributes   map[string]any `json:"attributes,omitempty"`
}

type SecurityAuditEventsResponse struct {
	Events []SecurityAuditEvent `json:"events"`
}
