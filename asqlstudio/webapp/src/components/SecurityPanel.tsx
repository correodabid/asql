import { type ReactNode, useEffect, useMemo, useState } from 'react'
import { api } from '../lib/api'
import {
  IconAlertTriangle,
  IconCheck,
  IconChevronDown,
  IconKey,
  IconPlus,
  IconRefresh,
  IconShield,
  IconToggleLeft,
  IconToggleRight,
  IconTrash,
  IconUsers,
  IconUserPlus,
  IconLock,
} from './Icons'

/* ── Types ────────────────────────────────────────────────── */

type PrincipalKind = 'USER' | 'ROLE'
type PrincipalPrivilege = 'ADMIN' | 'SELECT_HISTORY'

type PrincipalRecord = {
  name: string
  kind: PrincipalKind
  enabled: boolean
  roles?: string[]
  effective_roles?: string[]
  referenced_by?: string[]
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
const guidedGrantPrivilegeOptions: PrincipalPrivilege[] = ['ADMIN']

/* ── Helpers ──────────────────────────────────────────────── */

function sorted(values?: string[]) {
  return [...(values ?? [])].sort((a, b) => a.localeCompare(b))
}

function canDeletePrincipal(p: PrincipalRecord) {
  return (
    !p.enabled &&
    (p.roles?.length ?? 0) === 0 &&
    (p.privileges?.length ?? 0) === 0 &&
    (p.referenced_by?.length ?? 0) === 0
  )
}

function capabilitySourceLabel(capability: PrincipalPrivilege, directPrivileges: string[], effectivePrivileges: string[], effectiveRoles: string[]) {
  if (!effectivePrivileges.includes(capability)) {
    return 'Not granted'
  }
  if (directPrivileges.includes(capability)) {
    return 'Direct grant'
  }
  if (effectiveRoles.length > 0) {
    return `Inherited via ${effectiveRoles.length} role${effectiveRoles.length === 1 ? '' : 's'}`
  }
  return 'Effective grant'
}

/* ── Toast types ──────────────────────────────────────────── */

type Toast = { id: number; message: string; kind: 'success' | 'error' }
let toastSeq = 0

/* ── Action drawer IDs ────────────────────────────────────── */

type DrawerId =
  | 'create-user'
  | 'create-role'
  | 'grant-history-access'
  | 'grant-privilege'
  | 'revoke-privilege'
  | 'grant-role'
  | 'revoke-role'
  | 'set-password'
  | null

/* ── Main Panel ───────────────────────────────────────────── */

export function SecurityPanel() {
  const [principals, setPrincipals] = useState<PrincipalRecord[]>([])
  const [expanded, setExpanded] = useState<string | null>(null)
  const [activeDrawer, setActiveDrawer] = useState<DrawerId>(null)
  const [toasts, setToasts] = useState<Toast[]>([])
  const [busy, setBusy] = useState('')

  /* bootstrap form */
  const [bootstrapPrincipal, setBootstrapPrincipal] = useState('admin')
  const [bootstrapPassword, setBootstrapPassword] = useState('')

  /* create user form */
  const [newUser, setNewUser] = useState('')
  const [newUserPassword, setNewUserPassword] = useState('')

  /* create role form */
  const [newRole, setNewRole] = useState('')

  /* guided historical access grant */
  const [historyAccessPrincipal, setHistoryAccessPrincipal] = useState('')

  /* grant/revoke privilege */
  const [grantPrincipal, setGrantPrincipal] = useState('')
  const [grantPrivilege, setGrantPrivilege] = useState<PrincipalPrivilege>('ADMIN')
  const [revokePrincipal, setRevokePrincipal] = useState('')
  const [revokePrivilege, setRevokePrivilege] = useState<PrincipalPrivilege>('SELECT_HISTORY')

  /* grant/revoke role */
  const [grantRolePrincipal, setGrantRolePrincipal] = useState('')
  const [grantRole, setGrantRole] = useState('')
  const [revokeRolePrincipal, setRevokeRolePrincipal] = useState('')
  const [revokeRole, setRevokeRole] = useState('')

  /* set password */
  const [passwordPrincipal, setPasswordPrincipal] = useState('')
  const [passwordValue, setPasswordValue] = useState('')

  /* derived */
  const hasCatalog = principals.length > 0
  const users = useMemo(() => principals.filter((p) => p.kind === 'USER'), [principals])
  const roles = useMemo(() => principals.filter((p) => p.kind === 'ROLE'), [principals])
  const admins = useMemo(() => principals.filter((p) => (p.effective_privileges ?? []).includes('ADMIN')), [principals])
  const historyReaders = useMemo(
    () => principals.filter((p) => (p.effective_privileges ?? []).includes('SELECT_HISTORY')),
    [principals],
  )
  const disabledCount = useMemo(() => principals.filter((p) => !p.enabled).length, [principals])

  /* toast helper */
  const toast = (message: string, kind: 'success' | 'error') => {
    const id = ++toastSeq
    setToasts((t) => [...t, { id, message, kind }])
    setTimeout(() => setToasts((t) => t.filter((x) => x.id !== id)), 3200)
  }

  /* data fetch */
  const refresh = async () => {
    const resp = await api<ListPrincipalsResponse>('/api/security/principals', 'GET')
    setPrincipals((resp.principals ?? []).slice().sort((a, b) => a.name.localeCompare(b.name)))
  }

  useEffect(() => {
    void refresh().catch((err) => toast(err instanceof Error ? err.message : String(err), 'error'))
  }, [])

  /* mutation runner */
  const run = async (label: string, fn: () => Promise<string>) => {
    setBusy(label)
    try {
      const msg = await fn()
      await refresh()
      toast(msg, 'success')
    } catch (err) {
      toast(err instanceof Error ? err.message : String(err), 'error')
    } finally {
      setBusy('')
    }
  }

  /* mutations */
  const submitBootstrap = () =>
    run('bootstrap', async () => {
      const resp = await api<SecurityMutationResponse>('/api/security/bootstrap-admin', 'POST', {
        principal: bootstrapPrincipal,
        password: bootstrapPassword,
      })
      setBootstrapPassword('')
      return `Admin principal "${resp.principal?.name ?? bootstrapPrincipal}" bootstrapped.`
    })

  const submitCreateUser = () =>
    run('create-user', async () => {
      const resp = await api<SecurityMutationResponse>('/api/security/users', 'POST', {
        principal: newUser,
        password: newUserPassword,
      })
      setNewUser('')
      setNewUserPassword('')
      setActiveDrawer(null)
      return `User "${resp.principal?.name ?? newUser}" created.`
    })

  const submitCreateRole = () =>
    run('create-role', async () => {
      const resp = await api<SecurityMutationResponse>('/api/security/roles', 'POST', {
        principal: newRole,
      })
      setNewRole('')
      setActiveDrawer(null)
      return `Role "${resp.principal?.name ?? newRole}" created.`
    })

  const submitGrantHistoricalAccess = () =>
    run('grant-history-access', async () => {
      await api<SecurityMutationResponse>('/api/security/history-access/grant', 'POST', {
        principal: historyAccessPrincipal,
      })
      setHistoryAccessPrincipal('')
      setActiveDrawer(null)
      return `Granted historical access to ${historyAccessPrincipal}.`
    })

  const submitGrantPrivilege = () =>
    run('grant-privilege', async () => {
      await api<SecurityMutationResponse>('/api/security/privileges/grant', 'POST', {
        principal: grantPrincipal,
        privilege: grantPrivilege,
      })
      setActiveDrawer(null)
      return `Granted ${grantPrivilege} to ${grantPrincipal}.`
    })

  const submitRevokePrivilege = () =>
    run('revoke-privilege', async () => {
      await api<SecurityMutationResponse>('/api/security/privileges/revoke', 'POST', {
        principal: revokePrincipal,
        privilege: revokePrivilege,
      })
      setActiveDrawer(null)
      return `Revoked ${revokePrivilege} from ${revokePrincipal}.`
    })

  const submitGrantRole = () =>
    run('grant-role', async () => {
      await api<SecurityMutationResponse>('/api/security/roles/grant', 'POST', {
        principal: grantRolePrincipal,
        role: grantRole,
      })
      setActiveDrawer(null)
      return `Granted role ${grantRole} to ${grantRolePrincipal}.`
    })

  const submitRevokeRole = () =>
    run('revoke-role', async () => {
      await api<SecurityMutationResponse>('/api/security/roles/revoke', 'POST', {
        principal: revokeRolePrincipal,
        role: revokeRole,
      })
      setActiveDrawer(null)
      return `Revoked role ${revokeRole} from ${revokeRolePrincipal}.`
    })

  const submitSetPassword = () =>
    run('set-password', async () => {
      await api<SecurityMutationResponse>('/api/security/passwords/set', 'POST', {
        principal: passwordPrincipal,
        password: passwordValue,
      })
      setPasswordValue('')
      setActiveDrawer(null)
      return `Password updated for ${passwordPrincipal}.`
    })

  const submitToggle = (p: PrincipalRecord) =>
    run(`toggle-${p.name}`, async () => {
      const endpoint = p.enabled ? '/api/security/principals/disable' : '/api/security/principals/enable'
      await api<SecurityMutationResponse>(endpoint, 'POST', { principal: p.name })
      return `${p.name} ${p.enabled ? 'disabled' : 'enabled'}.`
    })

  const submitDelete = (name: string) =>
    run(`delete-${name}`, async () => {
      await api<SecurityMutationResponse>('/api/security/principals/delete', 'POST', { principal: name })
      setExpanded((prev) => (prev === name ? null : prev))
      return `Principal "${name}" deleted.`
    })

  const toggleDrawer = (id: DrawerId) => setActiveDrawer((prev) => (prev === id ? null : id))

  /* datalists for autocomplete */
  const principalNames = principals.map((p) => p.name)
  const roleNames = roles.map((p) => p.name)
  const userNames = users.map((p) => p.name)

  return (
    <div className="sec-page">
      {/* ── Header ──────────────────────────────────────── */}
      <div className="sec-header">
        <div className="sec-header-text">
          <h2 className="sec-title">Security</h2>
          <p className="sec-subtitle">
            Manage principals, privileges, and roles for pgwire authentication and temporal authorization.
          </p>
        </div>
        <button
          className="toolbar-btn"
          disabled={busy !== ''}
          onClick={() => void refresh().catch((err) => toast(err instanceof Error ? err.message : String(err), 'error'))}
        >
          <IconRefresh /> Refresh
        </button>
      </div>

      {/* ── KPI row ─────────────────────────────────────── */}
      <div className="sec-kpi-row">
        <SecKPI icon={<IconShield />} label="Principals" value={principals.length} color="var(--accent)" delay={0} />
        <SecKPI icon={<IconUsers />} label="Users" value={users.length} color="var(--text-safe)" delay={60} />
        <SecKPI icon={<IconKey />} label="Roles" value={roles.length} color="var(--text-warning)" delay={120} />
        <SecKPI icon={<IconLock />} label="Admins" value={admins.length} color="#ec4899" delay={180} />
        <SecKPI icon={<IconKey />} label="History readers" value={historyReaders.length} color="#06b6d4" delay={240} />
      </div>

      {/* ── Bootstrap (first-run) ───────────────────────── */}
      {!hasCatalog && (
        <div className="sec-bootstrap glass-section" style={{ animationDelay: '80ms' }}>
          <div className="sec-bootstrap-icon">
            <IconShield />
          </div>
          <div className="sec-bootstrap-body">
            <h3 className="sec-bootstrap-title">Initialize Security</h3>
            <p className="sec-bootstrap-desc">
              No durable principal catalog found. Create the first admin principal to enable pgwire authentication and protect your data.
            </p>
            <div className="sec-bootstrap-form">
              <label className="sec-field">
                <span className="sec-field-label">Admin name</span>
                <input
                  className="sec-input"
                  value={bootstrapPrincipal}
                  onChange={(e) => setBootstrapPrincipal(e.target.value)}
                  placeholder="admin"
                />
              </label>
              <label className="sec-field">
                <span className="sec-field-label">Password</span>
                <input
                  className="sec-input"
                  type="password"
                  value={bootstrapPassword}
                  onChange={(e) => setBootstrapPassword(e.target.value)}
                  placeholder="••••••••"
                />
              </label>
              <button
                className="toolbar-btn primary"
                disabled={busy !== '' || !bootstrapPrincipal.trim() || !bootstrapPassword.trim()}
                onClick={() => void submitBootstrap()}
              >
                <IconShield /> Bootstrap admin
              </button>
            </div>
          </div>
        </div>
      )}

      {/* ── Quick actions strip ─────────────────────────── */}
      {hasCatalog && (
        <div className="glass-section" style={{ animationDelay: '100ms' }}>
          <div className="glass-section-header">
            <span className="glass-section-title">Quick Actions</span>
          </div>
          <div className="glass-section-body">
            <div className="sec-actions-strip">
              <ActionChip
                icon={<IconUserPlus />}
                label="Create user"
                active={activeDrawer === 'create-user'}
                onClick={() => toggleDrawer('create-user')}
              />
              <ActionChip
                icon={<IconShield />}
                label="Create role"
                active={activeDrawer === 'create-role'}
                onClick={() => toggleDrawer('create-role')}
              />
              <ActionChip
                icon={<IconKey />}
                label="Grant historical access"
                active={activeDrawer === 'grant-history-access'}
                onClick={() => toggleDrawer('grant-history-access')}
              />
              <ActionChip
                icon={<IconKey />}
                label="Grant privilege"
                active={activeDrawer === 'grant-privilege'}
                onClick={() => toggleDrawer('grant-privilege')}
              />
              <ActionChip
                icon={<IconKey />}
                label="Revoke privilege"
                active={activeDrawer === 'revoke-privilege'}
                onClick={() => toggleDrawer('revoke-privilege')}
              />
              <ActionChip
                icon={<IconShield />}
                label="Grant role"
                active={activeDrawer === 'grant-role'}
                onClick={() => toggleDrawer('grant-role')}
              />
              <ActionChip
                icon={<IconShield />}
                label="Revoke role"
                active={activeDrawer === 'revoke-role'}
                onClick={() => toggleDrawer('revoke-role')}
              />
              <ActionChip
                icon={<IconLock />}
                label="Set password"
                active={activeDrawer === 'set-password'}
                onClick={() => toggleDrawer('set-password')}
              />
            </div>

            {/* ── Slide-down drawers ────────────────────── */}
            {activeDrawer === 'create-user' && (
              <DrawerForm
                title="Create user"
                description="Add a durable login principal for pgwire and authorization."
                actionLabel="Create user"
                disabled={busy !== '' || !newUser.trim() || !newUserPassword.trim()}
                onSubmit={() => void submitCreateUser()}
                onClose={() => setActiveDrawer(null)}
              >
                <label className="sec-field">
                  <span className="sec-field-label">User name</span>
                  <input className="sec-input" value={newUser} onChange={(e) => setNewUser(e.target.value)} placeholder="analyst" />
                </label>
                <label className="sec-field">
                  <span className="sec-field-label">Password</span>
                  <input className="sec-input" type="password" value={newUserPassword} onChange={(e) => setNewUserPassword(e.target.value)} placeholder="••••••••" />
                </label>
              </DrawerForm>
            )}

            {activeDrawer === 'create-role' && (
              <DrawerForm
                title="Create role"
                description="Create a reusable role for privilege bundles."
                actionLabel="Create role"
                disabled={busy !== '' || !newRole.trim()}
                onSubmit={() => void submitCreateRole()}
                onClose={() => setActiveDrawer(null)}
              >
                <label className="sec-field">
                  <span className="sec-field-label">Role name</span>
                  <input className="sec-input" value={newRole} onChange={(e) => setNewRole(e.target.value)} placeholder="history_readers" />
                </label>
              </DrawerForm>
            )}

            {activeDrawer === 'grant-history-access' && (
              <DrawerForm
                title="Grant historical access"
                description="Make temporal access an explicit choice. This grants SELECT_HISTORY so the principal can run AS OF LSN, AS OF TIMESTAMP, and FOR HISTORY queries based on current grant state."
                actionLabel="Grant historical access"
                disabled={busy !== '' || !historyAccessPrincipal.trim()}
                onSubmit={() => void submitGrantHistoricalAccess()}
                onClose={() => setActiveDrawer(null)}
              >
                <div className="sec-guide-card">
                  <div className="sec-guide-title">What this unlocks</div>
                  <ul className="sec-guide-list">
                    <li>Temporal reads against current or older snapshots.</li>
                    <li>Historical helper workflows in pgwire, gRPC, HTTP, and Studio.</li>
                    <li>Access is evaluated against the principal&apos;s current grants, not historical grant state.</li>
                  </ul>
                </div>
                <label className="sec-field">
                  <span className="sec-field-label">Principal</span>
                  <input className="sec-input" list="sec-dl-principals" value={historyAccessPrincipal} onChange={(e) => setHistoryAccessPrincipal(e.target.value)} placeholder="analyst or history_readers" />
                </label>
                <div className="sec-guide-card sec-guide-card-subtle">
                  <div className="sec-guide-title">Grant preview</div>
                  <div className="sec-guide-preview">
                    <span className="sec-chip sec-chip-privilege">SELECT_HISTORY</span>
                    <span className="sec-guide-preview-text">Direct grant to {historyAccessPrincipal.trim() || 'selected principal'}</span>
                  </div>
                </div>
              </DrawerForm>
            )}

            {activeDrawer === 'grant-privilege' && (
              <DrawerForm
                title="Grant privilege"
                description="Grant an explicit non-temporal privilege directly to a user or role. Use the dedicated historical-access flow for temporal reads."
                actionLabel="Grant"
                disabled={busy !== '' || !grantPrincipal.trim() || !grantPrivilege}
                onSubmit={() => void submitGrantPrivilege()}
                onClose={() => setActiveDrawer(null)}
              >
                <label className="sec-field">
                  <span className="sec-field-label">Principal</span>
                  <input className="sec-input" list="sec-dl-principals" value={grantPrincipal} onChange={(e) => setGrantPrincipal(e.target.value)} placeholder="analyst" />
                </label>
                <label className="sec-field">
                  <span className="sec-field-label">Privilege</span>
                  <select className="sec-input" value={grantPrivilege} onChange={(e) => setGrantPrivilege(e.target.value as PrincipalPrivilege)}>
                    {guidedGrantPrivilegeOptions.map((o) => <option key={o} value={o}>{o}</option>)}
                  </select>
                </label>
              </DrawerForm>
            )}

            {activeDrawer === 'revoke-privilege' && (
              <DrawerForm
                title="Revoke privilege"
                description="Remove a direct privilege grant from a principal."
                actionLabel="Revoke"
                disabled={busy !== '' || !revokePrincipal.trim()}
                onSubmit={() => void submitRevokePrivilege()}
                onClose={() => setActiveDrawer(null)}
              >
                <label className="sec-field">
                  <span className="sec-field-label">Principal</span>
                  <input className="sec-input" list="sec-dl-principals" value={revokePrincipal} onChange={(e) => setRevokePrincipal(e.target.value)} placeholder="analyst" />
                </label>
                <label className="sec-field">
                  <span className="sec-field-label">Privilege</span>
                  <select className="sec-input" value={revokePrivilege} onChange={(e) => setRevokePrivilege(e.target.value as PrincipalPrivilege)}>
                    {privilegeOptions.map((o) => <option key={o} value={o}>{o}</option>)}
                  </select>
                </label>
              </DrawerForm>
            )}

            {activeDrawer === 'grant-role' && (
              <DrawerForm
                title="Grant role"
                description="Grant an existing role to a user or another role."
                actionLabel="Grant"
                disabled={busy !== '' || !grantRolePrincipal.trim() || !grantRole.trim()}
                onSubmit={() => void submitGrantRole()}
                onClose={() => setActiveDrawer(null)}
              >
                <label className="sec-field">
                  <span className="sec-field-label">Principal</span>
                  <input className="sec-input" list="sec-dl-principals" value={grantRolePrincipal} onChange={(e) => setGrantRolePrincipal(e.target.value)} placeholder="analyst" />
                </label>
                <label className="sec-field">
                  <span className="sec-field-label">Role</span>
                  <input className="sec-input" list="sec-dl-roles" value={grantRole} onChange={(e) => setGrantRole(e.target.value)} placeholder="history_readers" />
                </label>
              </DrawerForm>
            )}

            {activeDrawer === 'revoke-role' && (
              <DrawerForm
                title="Revoke role"
                description="Remove a direct role grant from a principal."
                actionLabel="Revoke"
                disabled={busy !== '' || !revokeRolePrincipal.trim() || !revokeRole.trim()}
                onSubmit={() => void submitRevokeRole()}
                onClose={() => setActiveDrawer(null)}
              >
                <label className="sec-field">
                  <span className="sec-field-label">Principal</span>
                  <input className="sec-input" list="sec-dl-principals" value={revokeRolePrincipal} onChange={(e) => setRevokeRolePrincipal(e.target.value)} placeholder="analyst" />
                </label>
                <label className="sec-field">
                  <span className="sec-field-label">Role</span>
                  <input className="sec-input" list="sec-dl-roles" value={revokeRole} onChange={(e) => setRevokeRole(e.target.value)} placeholder="history_readers" />
                </label>
              </DrawerForm>
            )}

            {activeDrawer === 'set-password' && (
              <DrawerForm
                title="Set password"
                description="Rotate the stored password for a user principal."
                actionLabel="Update password"
                disabled={busy !== '' || !passwordPrincipal.trim() || !passwordValue.trim()}
                onSubmit={() => void submitSetPassword()}
                onClose={() => setActiveDrawer(null)}
              >
                <label className="sec-field">
                  <span className="sec-field-label">User</span>
                  <input className="sec-input" list="sec-dl-users" value={passwordPrincipal} onChange={(e) => setPasswordPrincipal(e.target.value)} placeholder="analyst" />
                </label>
                <label className="sec-field">
                  <span className="sec-field-label">New password</span>
                  <input className="sec-input" type="password" value={passwordValue} onChange={(e) => setPasswordValue(e.target.value)} placeholder="••••••••" />
                </label>
              </DrawerForm>
            )}
          </div>
        </div>
      )}

      {/* ── Principal catalog ───────────────────────────── */}
      {hasCatalog && (
        <div className="glass-section" style={{ animationDelay: '160ms' }}>
          <div className="glass-section-header">
            <span className="glass-section-title">Principal Catalog</span>
            <span className="sec-catalog-count">{principals.length} principal{principals.length !== 1 ? 's' : ''}{disabledCount > 0 ? ` · ${disabledCount} disabled` : ''}</span>
          </div>
          <div className="glass-section-body">
            <div className="sec-catalog">
              {principals.map((p, i) => (
                <PrincipalCard
                  key={p.name}
                  principal={p}
                  expanded={expanded === p.name}
                  onToggleExpand={() => setExpanded((prev) => (prev === p.name ? null : p.name))}
                  onToggleEnabled={() => void submitToggle(p)}
                  onDelete={() => void submitDelete(p.name)}
                  busy={busy}
                  delay={i * 40}
                />
              ))}
            </div>
          </div>
        </div>
      )}

      {/* ── Empty state ─────────────────────────────────── */}
      {!hasCatalog && (
        <div className="sec-empty">
          <div className="sec-empty-icon"><IconShield /></div>
          <p className="sec-empty-label">No principals registered yet</p>
          <p className="sec-empty-hint">Bootstrap an admin principal above to get started.</p>
        </div>
      )}

      {/* ── Datalists ───────────────────────────────────── */}
      <datalist id="sec-dl-principals">
        {principalNames.map((n) => <option key={n} value={n} />)}
      </datalist>
      <datalist id="sec-dl-users">
        {userNames.map((n) => <option key={n} value={n} />)}
      </datalist>
      <datalist id="sec-dl-roles">
        {roleNames.map((n) => <option key={n} value={n} />)}
      </datalist>

      {/* ── Toast rail ──────────────────────────────────── */}
      {toasts.length > 0 && (
        <div className="sec-toast-rail">
          {toasts.map((t) => (
            <div key={t.id} className={`sec-toast sec-toast-${t.kind}`}>
              {t.kind === 'success' ? <IconCheck /> : <IconAlertTriangle />}
              <span>{t.message}</span>
            </div>
          ))}
        </div>
      )}
    </div>
  )
}

/* ── Sub-components ───────────────────────────────────────── */

function SecKPI({
  icon,
  label,
  value,
  color,
  delay = 0,
}: {
  icon: ReactNode
  label: string
  value: number
  color: string
  delay?: number
}) {
  return (
    <div
      className="kpi-card"
      style={{ '--kpi-accent': color, animationDelay: `${delay}ms` } as React.CSSProperties}
    >
      <div className="kpi-card-glow" />
      <div className="kpi-icon-wrap" style={{ color }}>
        {icon}
      </div>
      <div className="kpi-content">
        <span className="kpi-label">{label}</span>
        <div className="kpi-value">{value}</div>
      </div>
    </div>
  )
}

function ActionChip({
  icon,
  label,
  active,
  onClick,
}: {
  icon: ReactNode
  label: string
  active: boolean
  onClick: () => void
}) {
  return (
    <button className={`sec-action-chip ${active ? 'active' : ''}`} onClick={onClick}>
      {icon}
      <span>{label}</span>
    </button>
  )
}

function DrawerForm({
  title,
  description,
  actionLabel,
  disabled,
  onSubmit,
  onClose,
  children,
}: {
  title: string
  description: string
  actionLabel: string
  disabled: boolean
  onSubmit: () => void
  onClose: () => void
  children: ReactNode
}) {
  return (
    <div className="sec-drawer">
      <div className="sec-drawer-header">
        <div>
          <div className="sec-drawer-title">{title}</div>
          <div className="sec-drawer-desc">{description}</div>
        </div>
        <button className="icon-btn" onClick={onClose} title="Close">
          <IconPlus /> {/* rotated 45° via CSS */}
        </button>
      </div>
      <div className="sec-drawer-fields">{children}</div>
      <div className="sec-drawer-footer">
        <button className="toolbar-btn primary" disabled={disabled} onClick={onSubmit}>
          {actionLabel}
        </button>
        <button className="toolbar-btn" onClick={onClose}>Cancel</button>
      </div>
    </div>
  )
}

function PrincipalCard({
  principal: p,
  expanded,
  onToggleExpand,
  onToggleEnabled,
  onDelete,
  busy,
  delay = 0,
}: {
  principal: PrincipalRecord
  expanded: boolean
  onToggleExpand: () => void
  onToggleEnabled: () => void
  onDelete: () => void
  busy: string
  delay?: number
}) {
  const directRoles = sorted(p.roles)
  const effectiveRoles = sorted(p.effective_roles)
  const inheritedRoles = effectiveRoles.filter((r) => !directRoles.includes(r))
  const directPrivileges = sorted(p.privileges as string[])
  const effectivePrivileges = sorted(p.effective_privileges as string[])
  const inheritedPrivileges = effectivePrivileges.filter((pr) => !directPrivileges.includes(pr))
  const refs = sorted(p.referenced_by)
  const canAuthenticate = p.kind === 'USER' && p.enabled
  const hasAdmin = effectivePrivileges.includes('ADMIN')
  const hasHistoricalAccess = effectivePrivileges.includes('SELECT_HISTORY')
  const adminSource = capabilitySourceLabel('ADMIN', directPrivileges, effectivePrivileges, effectiveRoles)
  const historicalSource = capabilitySourceLabel('SELECT_HISTORY', directPrivileges, effectivePrivileges, effectiveRoles)

  return (
    <div
      className={`sec-principal ${p.enabled ? '' : 'disabled'} ${expanded ? 'expanded' : ''}`}
      style={{ animationDelay: `${delay}ms` }}
    >
      {/* ── Summary row ─────────────────────────────────── */}
      <button className="sec-principal-header" onClick={onToggleExpand}>
        <div className="sec-principal-identity">
          <span className={`sec-status-dot ${p.enabled ? 'dot-ok' : 'dot-off'}`} />
          <span className="sec-principal-name">{p.name}</span>
          <span className={`sec-badge sec-badge-${p.kind.toLowerCase()}`}>{p.kind}</span>
          {!p.enabled && <span className="sec-badge sec-badge-disabled">DISABLED</span>}
          {(p.effective_privileges ?? []).includes('ADMIN') && (
            <span className="sec-badge sec-badge-admin">ADMIN</span>
          )}
        </div>
        <div className="sec-principal-meta">
          <span className="sec-meta-stat">{directRoles.length} role{directRoles.length !== 1 ? 's' : ''}</span>
          <span className="sec-meta-sep">&middot;</span>
          <span className="sec-meta-stat">{directPrivileges.length} privilege{directPrivileges.length !== 1 ? 's' : ''}</span>
          <span className={`sec-chevron ${expanded ? 'open' : ''}`}>
            <IconChevronDown />
          </span>
        </div>
      </button>

      {/* ── Expanded detail ─────────────────────────────── */}
      {expanded && (
        <div className="sec-principal-detail">
          {/* Actions row */}
          <div className="sec-principal-actions">
            <button
              className={`sec-toggle-btn ${p.enabled ? 'enabled' : 'off'}`}
              disabled={busy !== ''}
              onClick={onToggleEnabled}
              title={p.enabled ? 'Disable this principal' : 'Enable this principal'}
            >
              {p.enabled ? <IconToggleRight /> : <IconToggleLeft />}
              {p.enabled ? 'Enabled' : 'Disabled'}
            </button>
            {canDeletePrincipal(p) && (
              <button
                className="sec-delete-btn"
                disabled={busy !== ''}
                onClick={onDelete}
              >
                <IconTrash />
                Delete
              </button>
            )}
          </div>

          <div className="sec-inspector-grid">
            <InspectorCard
              title="Login posture"
              status={canAuthenticate ? 'Can authenticate' : p.kind === 'ROLE' ? 'Role only' : 'Blocked'}
              tone={canAuthenticate ? 'ok' : p.kind === 'ROLE' ? 'info' : 'muted'}
              detail={
                p.kind === 'ROLE'
                  ? 'Roles do not log in directly; they bundle permissions for users or other roles.'
                  : p.enabled
                    ? 'Enabled users can authenticate and perform baseline current reads.'
                    : 'Disabled users cannot authenticate until re-enabled.'
              }
            />
            <InspectorCard
              title="Historical access"
              status={hasHistoricalAccess ? 'SELECT_HISTORY active' : 'No historical access'}
              tone={hasHistoricalAccess ? 'history' : 'muted'}
              detail={
                hasHistoricalAccess
                  ? `${historicalSource}. Allows AS OF LSN, AS OF TIMESTAMP, and FOR HISTORY queries under current grant state.`
                  : 'Grant SELECT_HISTORY explicitly to allow temporal reads and history helpers.'
              }
              chips={hasHistoricalAccess ? ['SELECT_HISTORY'] : []}
            />
            <InspectorCard
              title="Admin/operator access"
              status={hasAdmin ? 'ADMIN active' : 'No admin access'}
              tone={hasAdmin ? 'admin' : 'muted'}
              detail={
                hasAdmin
                  ? `${adminSource}. Allows administrative mutations and operator-sensitive helpers.`
                  : 'Without ADMIN, current DDL/DML and operator helpers stay blocked.'
              }
              chips={hasAdmin ? ['ADMIN'] : []}
            />
            <InspectorCard
              title="Role inheritance"
              status={effectiveRoles.length > 0 ? `${effectiveRoles.length} effective role${effectiveRoles.length === 1 ? '' : 's'}` : 'No effective roles'}
              tone={effectiveRoles.length > 0 ? 'info' : 'muted'}
              detail={
                inheritedRoles.length > 0
                  ? `Includes inherited role chain beyond direct membership: ${inheritedRoles.join(', ')}.`
                  : directRoles.length > 0
                    ? 'All current effective roles come from direct membership.'
                    : 'No role inheritance currently contributes to effective permissions.'
              }
              chips={effectiveRoles}
            />
          </div>

          {/* Attributes grid */}
          <div className="sec-attr-grid">
            <AttrRow label="Direct roles" items={directRoles} variant="role" />
            {inheritedRoles.length > 0 && (
              <AttrRow label="Inherited roles" items={inheritedRoles} variant="inherited" />
            )}
            <AttrRow label="Direct privileges" items={directPrivileges} variant="privilege" />
            {inheritedPrivileges.length > 0 && (
              <AttrRow label="Inherited privileges" items={inheritedPrivileges} variant="inherited" />
            )}
            {refs.length > 0 && <AttrRow label="Referenced by" items={refs} variant="ref" />}
          </div>
        </div>
      )}
    </div>
  )
}

function InspectorCard({
  title,
  status,
  detail,
  tone,
  chips = [],
}: {
  title: string
  status: string
  detail: string
  tone: 'ok' | 'history' | 'admin' | 'info' | 'muted'
  chips?: string[]
}) {
  return (
    <div className={`sec-inspector-card sec-inspector-card-${tone}`}>
      <div className="sec-inspector-title">{title}</div>
      <div className="sec-inspector-status">{status}</div>
      <div className="sec-inspector-detail">{detail}</div>
      {chips.length > 0 && (
        <div className="sec-inspector-chips">
          {chips.map((chip) => (
            <span key={chip} className="sec-chip sec-chip-inherited">
              {chip}
            </span>
          ))}
        </div>
      )}
    </div>
  )
}

function AttrRow({
  label,
  items,
  variant,
}: {
  label: string
  items: string[]
  variant: 'role' | 'privilege' | 'inherited' | 'ref'
}) {
  return (
    <div className="sec-attr-row">
      <span className="sec-attr-label">{label}</span>
      <div className="sec-attr-values">
        {items.length > 0 ? (
          items.map((v) => (
            <span key={v} className={`sec-chip sec-chip-${variant}`}>
              {v}
            </span>
          ))
        ) : (
          <span className="sec-attr-empty">&mdash;</span>
        )}
      </div>
    </div>
  )
}
