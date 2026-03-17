package adminapi

import "asql/internal/engine/executor"

type PrincipalRecord struct {
	Name                string                        `json:"name"`
	Kind                executor.PrincipalKind        `json:"kind"`
	Enabled             bool                          `json:"enabled"`
	Roles               []string                      `json:"roles,omitempty"`
	EffectiveRoles      []string                      `json:"effective_roles,omitempty"`
	ReferencedBy        []string                      `json:"referenced_by,omitempty"`
	Privileges          []executor.PrincipalPrivilege `json:"privileges,omitempty"`
	EffectivePrivileges []executor.PrincipalPrivilege `json:"effective_privileges,omitempty"`
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
