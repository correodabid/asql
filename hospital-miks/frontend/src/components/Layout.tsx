import { Outlet, NavLink, useLocation } from 'react-router-dom'
import { useEffect, useState } from 'react'
import { motion, AnimatePresence } from 'framer-motion'
import { useAuth } from '../context/AuthContext'
import {
  ArrowRightOnRectIcon, ChevronLeftIcon, BellIcon, HospitalIcon,
  MagnifyingGlassIcon,
} from './icons'
import GlobalCommandPalette from './GlobalCommandPalette'
import { getCurrentPageTitle, navSections } from './navigation'

export default function Layout() {
  const { logout, role } = useAuth()
  const location = useLocation()
  const [collapsed, setCollapsed] = useState(false)
  const [commandPaletteOpen, setCommandPaletteOpen] = useState(false)

  useEffect(() => {
    function handleGlobalKeydown(event: KeyboardEvent) {
      const target = event.target as HTMLElement | null
      const isTypingTarget = target && (
        target.tagName === 'INPUT'
        || target.tagName === 'TEXTAREA'
        || target.tagName === 'SELECT'
        || target.isContentEditable
      )

      if ((event.metaKey || event.ctrlKey) && event.key.toLowerCase() === 'k') {
        event.preventDefault()
        setCommandPaletteOpen(true)
        return
      }

      if (!isTypingTarget && event.key === '/') {
        event.preventDefault()
        setCommandPaletteOpen(true)
      }
    }

    window.addEventListener('keydown', handleGlobalKeydown)
    return () => window.removeEventListener('keydown', handleGlobalKeydown)
  }, [])

  useEffect(() => {
    setCommandPaletteOpen(false)
  }, [location.pathname])

  return (
    <>
      <GlobalCommandPalette open={commandPaletteOpen} onClose={() => setCommandPaletteOpen(false)} onLogout={logout} />
      <div className="flex h-screen overflow-hidden bg-[radial-gradient(circle_at_top_left,rgba(16,185,129,0.08),transparent_24%),linear-gradient(180deg,#f8fbfd_0%,#eef4f8_100%)] p-3 gap-3">
      {/* Sidebar */}
      <motion.aside
        animate={{ width: collapsed ? 72 : 260 }}
        transition={{ duration: 0.3, ease: [0.4, 0, 0.2, 1] }}
        className="relative flex flex-col text-white shadow-sidebar z-20 overflow-hidden rounded-[28px] border border-white/10 bg-[linear-gradient(180deg,#0f172a_0%,#111827_45%,#0b1220_100%)] shadow-[0_24px_60px_rgba(15,23,42,0.28)]"
      >
        {/* Logo */}
        <div className={`flex items-center gap-3 px-4 py-5 border-b border-white/10 bg-white/[0.03] ${collapsed ? 'justify-center' : ''}`}>
          <div className="flex-shrink-0 w-9 h-9 rounded-xl bg-primary-400/15 ring-1 ring-primary-300/20 flex items-center justify-center shadow-lg shadow-primary-950/20">
            <HospitalIcon className="w-5 h-5 text-primary-300" />
          </div>
          <AnimatePresence>
            {!collapsed && (
              <motion.div
                initial={{ opacity: 0, width: 0 }}
                animate={{ opacity: 1, width: 'auto' }}
                exit={{ opacity: 0, width: 0 }}
                transition={{ duration: 0.2 }}
                className="overflow-hidden whitespace-nowrap"
              >
                <h1 className="text-base font-bold tracking-tight text-white">Hospital MiKS</h1>
                <p className="text-[10px] text-primary-300/80 tracking-[0.18em]">POWERED BY ASQL</p>
              </motion.div>
            )}
          </AnimatePresence>
        </div>

        {/* Navigation */}
        <nav className="flex-1 overflow-y-auto py-3 px-2 dark-scroll">
          {navSections.map((section) => (
            <div key={section.section} className="mb-4">
              <AnimatePresence>
                {!collapsed && (
                  <motion.p
                    initial={{ opacity: 0 }}
                    animate={{ opacity: 1 }}
                    exit={{ opacity: 0 }}
                    className="text-[10px] uppercase tracking-[0.15em] text-white/45 font-semibold px-3 mb-1.5"
                  >
                    {section.section}
                  </motion.p>
                )}
              </AnimatePresence>
              {section.items.map((item) => {
                const isActive = item.to === '/' ? location.pathname === '/' : location.pathname.startsWith(item.to)
                return (
                  <NavLink
                    key={item.to}
                    to={item.to}
                    end={item.to === '/'}
                    className="block"
                  >
                    <div
                      className={`group relative flex items-center gap-3 px-3 py-2 rounded-xl text-[13px] font-medium transition-all duration-200 ${
                        isActive
                          ? 'bg-white/[0.12] text-white shadow-[inset_0_1px_0_rgba(255,255,255,0.06),0_8px_20px_rgba(0,0,0,0.18)]'
                          : 'text-white/72 hover:bg-white/[0.08] hover:text-white'
                      } ${collapsed ? 'justify-center' : ''}`}
                    >
                      {isActive && (
                        <motion.div
                          layoutId="activeIndicator"
                          className="absolute left-0 top-1/2 -translate-y-1/2 w-[3px] h-5 bg-primary-300 rounded-r-full shadow-[0_0_12px_rgba(52,211,153,0.55)]"
                          transition={{ type: 'spring', stiffness: 500, damping: 35 }}
                        />
                      )}
                      <item.icon className={`flex-shrink-0 w-[18px] h-[18px] transition-colors ${isActive ? 'text-primary-300' : 'text-white/80 group-hover:text-white'}`} />
                      <AnimatePresence>
                        {!collapsed && (
                          <motion.span
                            initial={{ opacity: 0, width: 0 }}
                            animate={{ opacity: 1, width: 'auto' }}
                            exit={{ opacity: 0, width: 0 }}
                            transition={{ duration: 0.2 }}
                            className="overflow-hidden whitespace-nowrap"
                          >
                            {item.label}
                          </motion.span>
                        )}
                      </AnimatePresence>
                      {item.badge && !collapsed && (
                        <span className="ml-auto text-[10px] bg-rose-400/15 text-rose-200 px-1.5 py-0.5 rounded-full font-semibold border border-rose-300/10">
                          {item.badge}
                        </span>
                      )}
                      {collapsed && (
                        <div className="absolute left-full ml-2 px-2 py-1 bg-slate-950 text-white text-xs rounded-lg opacity-0 group-hover:opacity-100 pointer-events-none transition-opacity whitespace-nowrap z-50 shadow-lg border border-white/10">
                          {item.label}
                        </div>
                      )}
                    </div>
                  </NavLink>
                )
              })}
            </div>
          ))}
        </nav>

        {/* Footer */}
        <div className="border-t border-white/10 p-3">
          <AnimatePresence>
            {!collapsed && (
              <motion.div
                initial={{ opacity: 0 }}
                animate={{ opacity: 1 }}
                exit={{ opacity: 0 }}
                className="flex items-center gap-3 p-2 rounded-xl bg-white/[0.06] border border-white/10 mb-2"
              >
                <div className="w-8 h-8 rounded-full bg-gradient-to-br from-primary-400 to-primary-600 flex items-center justify-center text-xs font-bold text-white">
                  {(role ?? 'U')[0]}
                </div>
                <div className="flex-1 min-w-0">
                  <p className="text-xs font-medium text-white truncate">Administrador</p>
                  <p className="text-[10px] text-white/60">{role ?? 'user'}</p>
                </div>
                <button
                  onClick={logout}
                  className="p-1.5 rounded-lg hover:bg-white/10 text-white/60 hover:text-white transition-colors"
                  title="Cerrar sesión"
                >
                  <ArrowRightOnRectIcon className="w-4 h-4" />
                </button>
              </motion.div>
            )}
          </AnimatePresence>
          {collapsed && (
            <button
              onClick={logout}
              className="w-full flex items-center justify-center p-2 rounded-xl hover:bg-white/10 text-white/60 hover:text-white transition-colors"
              title="Cerrar sesión"
            >
              <ArrowRightOnRectIcon className="w-4 h-4" />
            </button>
          )}
        </div>

        {/* Collapse toggle */}
        <button
          onClick={() => setCollapsed(!collapsed)}
          className="absolute top-5 -right-3 w-6 h-6 bg-slate-900 border border-slate-700 rounded-full flex items-center justify-center text-white/80 hover:text-white hover:bg-slate-800 transition-all shadow-lg z-30"
        >
          <ChevronLeftIcon className={`w-3 h-3 transition-transform duration-300 ${collapsed ? 'rotate-180' : ''}`} />
        </button>
      </motion.aside>

      {/* Main content area */}
      <div className="flex-1 flex flex-col overflow-hidden rounded-[28px] border border-slate-200/80 bg-white/72 backdrop-blur-xl shadow-[0_20px_60px_rgba(15,23,42,0.08)]">
        {/* Top bar */}
        <header className="h-16 bg-white/78 backdrop-blur-xl border-b border-slate-200/80 flex items-center justify-between px-6 flex-shrink-0 z-10">
          <div className="flex items-center gap-3">
            <h2 className="text-sm font-semibold text-surface-800 capitalize">
              {getCurrentPageTitle(location.pathname)}
            </h2>
          </div>
          <div className="flex items-center gap-3">
            <button
              onClick={() => setCommandPaletteOpen(true)}
              className="hidden md:flex items-center gap-2 rounded-2xl border border-slate-200 bg-white/85 px-3.5 py-2 text-sm text-surface-500 shadow-sm transition hover:border-primary-200 hover:text-surface-800"
            >
              <MagnifyingGlassIcon className="w-4 h-4" />
              <span>Buscar o navegar…</span>
              <span className="rounded-lg bg-slate-100 px-2 py-0.5 text-[11px] font-medium text-surface-400">⌘K</span>
            </button>
            <button className="relative p-2 rounded-xl text-surface-400 hover:bg-slate-100 hover:text-surface-700 transition-colors border border-transparent hover:border-slate-200">
              <BellIcon className="w-5 h-5" />
              <span className="absolute top-1.5 right-1.5 w-2 h-2 bg-accent-rose rounded-full" />
            </button>
            <div className="w-px h-6 bg-slate-200" />
            <div className="flex items-center gap-2">
              <div className="w-8 h-8 rounded-full bg-gradient-to-br from-primary-400 to-primary-600 flex items-center justify-center text-xs font-bold text-white shadow-[0_8px_20px_rgba(16,185,129,0.22)]">
                {(role ?? 'U')[0]}
              </div>
            </div>
          </div>
        </header>

        {/* Page content */}
        <main className="flex-1 overflow-y-auto">
          <motion.div
            key={location.pathname}
            initial={{ opacity: 0, y: 8 }}
            animate={{ opacity: 1, y: 0 }}
            transition={{ duration: 0.3, ease: 'easeOut' }}
            className="p-6 max-w-[1600px] mx-auto w-full"
          >
            <Outlet />
          </motion.div>
        </main>
      </div>
      </div>
    </>
  )
}
