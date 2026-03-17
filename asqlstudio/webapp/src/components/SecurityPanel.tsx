import { type ReactNode, useEffect, useMemo, useState } from 'react'
import { api } from '../lib/api'
import { IconAlertTriangle, IconCheck, IconKey, IconPlus, IconRefresh, IconShield } from './Icons'

type PrincipalKind = 'USER' | 'ROLE'
type PrincipalPrivilege = 'ADMIN' | 'SELECT_HISTORY'

type PrincipalRecord = {
  name: string
  kind: PrincipalKind
  enabled: boolean
  roles?: string[]
  effective_roles?: string[]
  privileges?: PrincipalPrivilege[]
  effective_privileges?: PrincipalPrivilege[]
}

type ListPrincipalsResponse = {
  principals?: PrincipalRecord[]
}

type SecurityMutationResponse = {
  status?: string
  principal?: PrincipalRecord
}

const privilegeOptions: PrincipalPrivilege[] = ['SELECT_HISTORY', 'ADMIN']

function sortedValues(values?: string[]) {
  return [...(values ?? [])].sort((a, b) => a.localeCompare(b))
}

function sortedPrivileges(values?: PrincipalPrivilege[]) {
  return [...(values ?? [])].sort((a, b) => a.localeCompare(b))
}

export function SecurityPanel() {
  const [principals, setPrincipals] = useState<PrincipalRecord[]>([])
  const [bootstrapPrincipal, setBootstrapPrincipal] = useState('admin')
  const [bootstrapPassword, setBootstrapPassword] = useState('')
  const [newUser, setNewUser] = useState('')
  const [newUserPassword, setNewUserPassword] = useState('')
  const [newRole, setNewRole] = useState('')
  const [grantPrincipal, setGrantPrincipal] = useState('')
  const [grantPrivilege, setGrantPrivilege] = useState<PrincipalPrivilege>('SELECT_HISTORY')
  const [grantRolePrincipal, setGrantRolePrincipal] = useState('')
  const [grantRole, setGrantRole] = useState('')
  const [revokeRolePrincipal, setRevokeRolePrincipal] = useState('')
  const [revokeRole, setRevokeRole] = useState('')
  const [revokePrincipal, setRevokePrincipal] = useState('')
  const [revokePrivilege, setRevokePrivilege] = useState<PrincipalPrivilege>('SELECT_HISTORY')
  const [passwordPrincipal, setPasswordPrincipal] = useState('')
  const [passwordValue, setPasswordValue] = useState('')
  const [busy, setBusy] = useState('')
  const [error, setError] = useState('')
  const [message, setMessage] = useState('')

  const hasCatalog = principals.length > 0
  const userPrincipals = useMemo(
    () => principals.filter((principal) => principal.kind === 'USER').map((principal) => principal.name),
    [principals],
  )
  const rolePrincipals = useMemo(
    () => principals.filter((principal) => principal.kind === 'ROLE').map((principal) => principal.name),
    [principals],
  )

  const refresh = async () => {
    setError('')
    const resp = await api<ListPrincipalsResponse>('/api/security/principals', 'GET')
    setPrincipals((resp.principals ?? []).slice().sort((a, b) => a.name.localeCompare(b.name)))
  }

  useEffect(() => {
    void refresh().catch((err) => setError(err instanceof Error ? err.message : String(err)))
  }, [])

  const run = async (label: string, fn: () => Promise<void>) => {
    setBusy(label)
    setError('')
    setMessage('')
    try {
      await fn()
      await refresh()
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err))
    } finally {
      setBusy('')
    }
  }

  const submitBootstrap = () => run('bootstrap', async () => {
    const resp = await api<SecurityMutationResponse>('/api/security/bootstrap-admin', 'POST', {
      principal: bootstrapPrincipal,
      password: bootstrapPassword,
    })
    setBootstrapPassword('')
    setGrantPrincipal(resp.principal?.name ?? bootstrapPrincipal)
    setMessage(`Bootstrapped admin principal ${resp.principal?.name ?? bootstrapPrincipal}.`)
  })

  const submitCreateUser = () => run('create-user', async () => {
    const resp = await api<SecurityMutationResponse>('/api/security/users', 'POST', {
      principal: newUser,
      password: newUserPassword,
    })
    setGrantPrincipal(resp.principal?.name ?? newUser)
    setGrantRolePrincipal(resp.principal?.name ?? newUser)
    setNewUser('')
    setNewUserPassword('')
    setMessage(`Created user ${resp.principal?.name ?? newUser}.`)
  })

  const submitCreateRole = () => run('create-role', async () => {
    const resp = await api<SecurityMutationResponse>('/api/security/roles', 'POST', {
      principal: newRole,
    })
    setGrantRole(resp.principal?.name ?? newRole)
    setNewRole('')
    setMessage(`Created role ${resp.principal?.name ?? newRole}.`)
  })

  const submitGrantPrivilege = () => run('grant-privilege', async () => {
    await api<SecurityMutationResponse>('/api/security/privileges/grant', 'POST', {
      principal: grantPrincipal,
      privilege: grantPrivilege,
    })
    setMessage(`Granted ${grantPrivilege} to ${grantPrincipal}.`)
  })

  const submitGrantRole = () => run('grant-role', async () => {
    await api<SecurityMutationResponse>('/api/security/roles/grant', 'POST', {
      principal: grantRolePrincipal,
      role: grantRole,
    })
    setMessage(`Granted role ${grantRole} to ${grantRolePrincipal}.`)
  })

  const submitRevokeRole = () => run('revoke-role', async () => {
    await api<SecurityMutationResponse>('/api/security/roles/revoke', 'POST', {
      principal: revokeRolePrincipal,
      role: revokeRole,
    })
    setMessage(`Revoked role ${revokeRole} from ${revokeRolePrincipal}.`)
  })

  const submitRevokePrivilege = () => run('revoke-privilege', async () => {
    await api<SecurityMutationResponse>('/api/security/privileges/revoke', 'POST', {
      principal: revokePrincipal,
      privilege: revokePrivilege,
    })
    setMessage(`Revoked ${revokePrivilege} from ${revokePrincipal}.`)
  })

  const submitSetPassword = () => run('set-password', async () => {
    await api<SecurityMutationResponse>('/api/security/passwords/set', 'POST', {
      principal: passwordPrincipal,
      password: passwordValue,
    })
    setPasswordValue('')
    setMessage(`Updated password for ${passwordPrincipal}.`)
  })

  const submitDisablePrincipal = (principalName: string) => run(`disable-${principalName}`, async () => {
    await api<SecurityMutationResponse>('/api/security/principals/disable', 'POST', {
      principal: principalName,
    })
    setMessage(`Disabled principal ${principalName}.`)
  })

  const submitEnablePrincipal = (principalName: string) => run(`enable-${principalName}`, async () => {
    await api<SecurityMutationResponse>('/api/security/principals/enable', 'POST', {
      principal: principalName,
    })
    setMessage(`Enabled principal ${principalName}.`)
  })

  return (
    <div className="panel" style={{ margin: 16, padding: 16, display: 'grid', gap: 16 }}>
      <div style={{ display: 'flex', justifyContent: 'space-between', gap: 12, alignItems: 'center', flexWrap: 'wrap' }}>
        <div>
          <h2 style={{ margin: '0 0 8px' }}>Security</h2>
          <p className="text-muted" style={{ margin: 0 }}>
            Bootstrap principals, create durable users and roles, rotate passwords, and manage grants from Studio.
          </p>
        </div>
        <button className="toolbar-btn" disabled={busy !== ''} onClick={() => void refresh().catch((err) => setError(err instanceof Error ? err.message : String(err)))}>
          <IconRefresh /> Refresh
        </button>
      </div>

      <div style={{ display: 'grid', gap: 12, gridTemplateColumns: 'repeat(auto-fit, minmax(180px, 1fr))' }}>
        <MetricCard label="Principals" value={String(principals.length)} />
        <MetricCard label="Users" value={String(userPrincipals.length)} />
        <MetricCard label="Roles" value={String(rolePrincipals.length)} />
        <MetricCard label="Temporal readers" value={String(principals.filter((p) => (p.effective_privileges ?? []).includes('SELECT_HISTORY')).length)} />
      </div>

      {!hasCatalog && (
        <div className="panel" style={{ padding: 12, border: '1px solid var(--border)', background: 'var(--panel-2)' }}>
          <h3 style={{ marginTop: 0 }}>Bootstrap admin</h3>
          <p className="text-muted" style={{ marginTop: 0 }}>
            No durable principal catalog was found. Create the first admin principal before adding more users and roles.
          </p>
          <div style={{ display: 'grid', gap: 12, gridTemplateColumns: 'minmax(180px, 1fr) minmax(180px, 1fr) auto', alignItems: 'end' }}>
            <label style={{ display: 'grid', gap: 6 }}>
              <span>Admin principal</span>
              <input className="title-domain-select" value={bootstrapPrincipal} onChange={(e) => setBootstrapPrincipal(e.target.value)} />
            </label>
            <label style={{ display: 'grid', gap: 6 }}>
              <span>Password</span>
              <input className="title-domain-select" type="password" value={bootstrapPassword} onChange={(e) => setBootstrapPassword(e.target.value)} />
            </label>
            <button className="toolbar-btn primary" disabled={busy !== '' || !bootstrapPrincipal.trim() || !bootstrapPassword.trim()} onClick={() => void submitBootstrap()}>
              <IconShield /> Bootstrap
            </button>
          </div>
        </div>
      )}

      <div style={{ display: 'grid', gap: 16, gridTemplateColumns: 'repeat(auto-fit, minmax(280px, 1fr))' }}>
        <ActionCard
          title="Create user"
          description="Add a durable login principal for pgwire and historical authorization."
          actionLabel="Create user"
          icon={<IconPlus />}
          disabled={busy !== '' || !newUser.trim() || !newUserPassword.trim()}
          onAction={() => void submitCreateUser()}
        >
          <label style={{ display: 'grid', gap: 6 }}>
            <span>User name</span>
            <input className="title-domain-select" value={newUser} onChange={(e) => setNewUser(e.target.value)} placeholder="analyst" />
          </label>
          <label style={{ display: 'grid', gap: 6 }}>
            <span>Password</span>
            <input className="title-domain-select" type="password" value={newUserPassword} onChange={(e) => setNewUserPassword(e.target.value)} placeholder="••••••••" />
          </label>
        </ActionCard>

        <ActionCard
          title="Create role"
          description="Create a reusable role for privilege bundles such as historical access."
          actionLabel="Create role"
          icon={<IconShield />}
          disabled={busy !== '' || !newRole.trim()}
          onAction={() => void submitCreateRole()}
        >
          <label style={{ display: 'grid', gap: 6 }}>
            <span>Role name</span>
            <input className="title-domain-select" value={newRole} onChange={(e) => setNewRole(e.target.value)} placeholder="history_readers" />
          </label>
        </ActionCard>

        <ActionCard
          title="Grant privilege"
          description="Grant an explicit privilege directly to a user or role."
          actionLabel="Grant privilege"
          icon={<IconKey />}
          disabled={busy !== '' || !grantPrincipal.trim()}
          onAction={() => void submitGrantPrivilege()}
        >
          <label style={{ display: 'grid', gap: 6 }}>
            <span>Principal</span>
            <input className="title-domain-select" list="security-principal-options" value={grantPrincipal} onChange={(e) => setGrantPrincipal(e.target.value)} placeholder="history_readers" />
          </label>
          <label style={{ display: 'grid', gap: 6 }}>
            <span>Privilege</span>
            <select className="title-domain-select" value={grantPrivilege} onChange={(e) => setGrantPrivilege(e.target.value as PrincipalPrivilege)}>
              {privilegeOptions.map((option) => <option key={option} value={option}>{option}</option>)}
            </select>
          </label>
        </ActionCard>

        <ActionCard
          title="Grant role"
          description="Grant an existing role to a user or another role."
          actionLabel="Grant role"
          icon={<IconShield />}
          disabled={busy !== '' || !grantRolePrincipal.trim() || !grantRole.trim()}
          onAction={() => void submitGrantRole()}
        >
          <label style={{ display: 'grid', gap: 6 }}>
            <span>Principal</span>
            <input className="title-domain-select" list="security-principal-options" value={grantRolePrincipal} onChange={(e) => setGrantRolePrincipal(e.target.value)} placeholder="analyst" />
          </label>
          <label style={{ display: 'grid', gap: 6 }}>
            <span>Role</span>
            <input className="title-domain-select" list="security-role-options" value={grantRole} onChange={(e) => setGrantRole(e.target.value)} placeholder="history_readers" />
          </label>
        </ActionCard>

        <ActionCard
          title="Revoke role"
          description="Remove a direct role grant from a user or role."
          actionLabel="Revoke role"
          icon={<IconShield />}
          disabled={busy !== '' || !revokeRolePrincipal.trim() || !revokeRole.trim()}
          onAction={() => void submitRevokeRole()}
        >
          <label style={{ display: 'grid', gap: 6 }}>
            <span>Principal</span>
            <input className="title-domain-select" list="security-principal-options" value={revokeRolePrincipal} onChange={(e) => setRevokeRolePrincipal(e.target.value)} placeholder="analyst" />
          </label>
          <label style={{ display: 'grid', gap: 6 }}>
            <span>Role</span>
            <input className="title-domain-select" list="security-role-options" value={revokeRole} onChange={(e) => setRevokeRole(e.target.value)} placeholder="history_readers" />
          </label>
        </ActionCard>

        <ActionCard
          title="Revoke privilege"
          description="Remove a direct privilege grant from a user or role."
          actionLabel="Revoke privilege"
          icon={<IconKey />}
          disabled={busy !== '' || !revokePrincipal.trim()}
          onAction={() => void submitRevokePrivilege()}
        >
          <label style={{ display: 'grid', gap: 6 }}>
            <span>Principal</span>
            <input className="title-domain-select" list="security-principal-options" value={revokePrincipal} onChange={(e) => setRevokePrincipal(e.target.value)} placeholder="history_readers" />
          </label>
          <label style={{ display: 'grid', gap: 6 }}>
            <span>Privilege</span>
            <select className="title-domain-select" value={revokePrivilege} onChange={(e) => setRevokePrivilege(e.target.value as PrincipalPrivilege)}>
              {privilegeOptions.map((option) => <option key={option} value={option}>{option}</option>)}
            </select>
          </label>
        </ActionCard>

        <ActionCard
          title="Set password"
          description="Rotate the stored password for a durable user principal."
          actionLabel="Set password"
          icon={<IconKey />}
          disabled={busy !== '' || !passwordPrincipal.trim() || !passwordValue.trim()}
          onAction={() => void submitSetPassword()}
        >
          <label style={{ display: 'grid', gap: 6 }}>
            <span>User principal</span>
            <input className="title-domain-select" list="security-user-options" value={passwordPrincipal} onChange={(e) => setPasswordPrincipal(e.target.value)} placeholder="analyst" />
          </label>
          <label style={{ display: 'grid', gap: 6 }}>
            <span>New password</span>
            <input className="title-domain-select" type="password" value={passwordValue} onChange={(e) => setPasswordValue(e.target.value)} placeholder="••••••••" />
          </label>
        </ActionCard>
      </div>

      <datalist id="security-principal-options">
        {principals.map((principal) => <option key={principal.name} value={principal.name} />)}
      </datalist>
      <datalist id="security-user-options">
        {userPrincipals.map((userName) => <option key={userName} value={userName} />)}
      </datalist>
      <datalist id="security-role-options">
        {rolePrincipals.map((roleName) => <option key={roleName} value={roleName} />)}
      </datalist>

      {message && (
        <div style={{ display: 'flex', gap: 8, alignItems: 'center', color: 'var(--success, #10b981)' }}>
          <IconCheck />
          <span>{message}</span>
        </div>
      )}

      {error && (
        <div style={{ display: 'flex', gap: 8, alignItems: 'flex-start', color: 'var(--danger, #ef4444)' }}>
          <IconAlertTriangle />
          <span>{error}</span>
        </div>
      )}

      <div className="panel" style={{ padding: 12, border: '1px solid var(--border)', background: 'var(--panel-2)' }}>
        <h3 style={{ marginTop: 0 }}>Principal catalog</h3>
        {principals.length === 0 ? (
          <div className="text-muted">No principals registered yet.</div>
        ) : (
          <div style={{ display: 'grid', gap: 10 }}>
            {principals.map((principal) => (
              <div key={principal.name} style={{ display: 'grid', gap: 8, padding: 12, border: '1px solid var(--border)', borderRadius: 12, background: 'var(--bg-elevated, rgba(255,255,255,0.02))' }}>
                <div style={{ display: 'flex', justifyContent: 'space-between', gap: 12, alignItems: 'center', flexWrap: 'wrap' }}>
                  <div style={{ display: 'flex', gap: 8, alignItems: 'center', flexWrap: 'wrap' }}>
                    <strong>{principal.name}</strong>
                    <span className="toolbar-badge">{principal.kind}</span>
                    <span className={`toolbar-badge ${principal.enabled ? '' : 'warn'}`}>{principal.enabled ? 'ENABLED' : 'DISABLED'}</span>
                  </div>
                  <div className="text-muted" style={{ fontSize: 12 }}>
                    {(principal.roles?.length ?? 0)} role{(principal.roles?.length ?? 0) === 1 ? '' : 's'} · {(principal.privileges?.length ?? 0)} privilege{(principal.privileges?.length ?? 0) === 1 ? '' : 's'}
                  </div>
                  <div style={{ display: 'flex', gap: 8, alignItems: 'center' }}>
                    {principal.enabled ? (
                      <button
                        className="toolbar-btn"
                        disabled={busy !== ''}
                        onClick={() => void submitDisablePrincipal(principal.name)}
                      >
                        Disable
                      </button>
                    ) : (
                      <button
                        className="toolbar-btn"
                        disabled={busy !== ''}
                        onClick={() => void submitEnablePrincipal(principal.name)}
                      >
                        Enable
                      </button>
                    )}
                  </div>
                </div>
                <div style={{ display: 'grid', gap: 6 }}>
                  <div>
                    <span className="text-muted" style={{ marginRight: 8 }}>Roles</span>
                    {sortedValues(principal.roles).length > 0 ? sortedValues(principal.roles).map((roleName) => (
                      <span key={roleName} className="toolbar-badge" style={{ marginRight: 6 }}>{roleName}</span>
                    )) : <span className="text-muted">—</span>}
                  </div>
                  <div>
                    <span className="text-muted" style={{ marginRight: 8 }}>Effective roles</span>
                    {sortedValues(principal.effective_roles).length > 0 ? sortedValues(principal.effective_roles).map((roleName) => (
                      <span key={roleName} className="toolbar-badge" style={{ marginRight: 6 }}>{roleName}</span>
                    )) : <span className="text-muted">—</span>}
                  </div>
                  <div>
                    <span className="text-muted" style={{ marginRight: 8 }}>Privileges</span>
                    {sortedPrivileges(principal.privileges).length > 0 ? sortedPrivileges(principal.privileges).map((value) => (
                      <span key={value} className="toolbar-badge" style={{ marginRight: 6 }}>{value}</span>
                    )) : <span className="text-muted">—</span>}
                  </div>
                  <div>
                    <span className="text-muted" style={{ marginRight: 8 }}>Effective</span>
                    {sortedPrivileges(principal.effective_privileges).length > 0 ? sortedPrivileges(principal.effective_privileges).map((value) => (
                      <span key={value} className="toolbar-badge" style={{ marginRight: 6 }}>{value}</span>
                    )) : <span className="text-muted">—</span>}
                  </div>
                </div>
              </div>
            ))}
          </div>
        )}
      </div>
    </div>
  )
}

function MetricCard({ label, value }: { label: string; value: string }) {
  return (
    <div className="panel" style={{ padding: 12, border: '1px solid var(--border)', background: 'var(--panel-2)' }}>
      <div className="text-muted" style={{ fontSize: 12, marginBottom: 4 }}>{label}</div>
      <div style={{ fontSize: 22, fontWeight: 700 }}>{value}</div>
    </div>
  )
}

function ActionCard({
  title,
  description,
  actionLabel,
  disabled,
  onAction,
  icon,
  children,
}: {
  title: string
  description: string
  actionLabel: string
  disabled: boolean
  onAction: () => void
  icon: ReactNode
  children: ReactNode
}) {
  return (
    <div className="panel" style={{ padding: 12, border: '1px solid var(--border)', background: 'var(--panel-2)', display: 'grid', gap: 12 }}>
      <div>
        <div style={{ display: 'flex', gap: 8, alignItems: 'center', marginBottom: 6 }}>
          {icon}
          <strong>{title}</strong>
        </div>
        <div className="text-muted" style={{ fontSize: 13 }}>{description}</div>
      </div>
      <div style={{ display: 'grid', gap: 10 }}>{children}</div>
      <div>
        <button className="toolbar-btn" disabled={disabled} onClick={onAction}>{icon} {actionLabel}</button>
      </div>
    </div>
  )
}
