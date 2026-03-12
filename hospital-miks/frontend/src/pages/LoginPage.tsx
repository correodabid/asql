import { useState, FormEvent } from 'react'
import { motion } from 'framer-motion'
import { useAuth } from '../context/AuthContext'
import { HospitalIcon, BoltIcon } from '../components/icons'

export default function LoginPage() {
  const { login } = useAuth()
  const [username, setUsername] = useState('')
  const [password, setPassword] = useState('')
  const [error, setError] = useState('')
  const [loading, setLoading] = useState(false)

  async function handleSubmit(e: FormEvent) {
    e.preventDefault()
    setError('')
    setLoading(true)
    try {
      await login(username, password)
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Error de autenticación')
    } finally {
      setLoading(false)
    }
  }

  return (
    <div className="min-h-screen flex relative overflow-hidden">
      {/* Left panel — branding */}
      <div className="hidden lg:flex lg:w-[55%] bg-mesh relative flex-col justify-between p-12 overflow-hidden">
        {/* Animated background shapes */}
        <div className="absolute inset-0 overflow-hidden pointer-events-none">
          <motion.div
            animate={{ rotate: 360 }}
            transition={{ duration: 120, repeat: Infinity, ease: 'linear' }}
            className="absolute -top-40 -right-40 w-[600px] h-[600px] rounded-full border border-primary-500/10"
          />
          <motion.div
            animate={{ rotate: -360 }}
            transition={{ duration: 90, repeat: Infinity, ease: 'linear' }}
            className="absolute top-1/2 -left-32 w-[500px] h-[500px] rounded-full border border-primary-400/10"
          />
          <div className="absolute bottom-20 right-20 w-80 h-80 bg-primary-500/5 rounded-full blur-3xl" />
          <div className="absolute top-20 left-20 w-60 h-60 bg-primary-400/5 rounded-full blur-3xl" />
          {/* Grid overlay */}
          <div
            className="absolute inset-0 opacity-[0.03]"
            style={{
              backgroundImage: 'linear-gradient(rgba(255,255,255,0.1) 1px, transparent 1px), linear-gradient(90deg, rgba(255,255,255,0.1) 1px, transparent 1px)',
              backgroundSize: '60px 60px',
            }}
          />
        </div>

        {/* Logo */}
        <motion.div
          initial={{ opacity: 0, y: -20 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ delay: 0.2 }}
          className="relative z-10 flex items-center gap-3"
        >
          <div className="w-10 h-10 rounded-xl bg-primary-500/20 backdrop-blur-sm flex items-center justify-center border border-primary-500/20">
            <HospitalIcon className="w-5 h-5 text-primary-400" />
          </div>
          <span className="text-white text-lg font-bold tracking-tight">Hospital MiKS</span>
        </motion.div>

        {/* Center content */}
        <motion.div
          initial={{ opacity: 0, y: 30 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ delay: 0.4 }}
          className="relative z-10 max-w-md"
        >
          <h2 className="text-4xl font-extrabold text-white leading-tight mb-4">
            Sistema de Gestión
            <span className="block bg-gradient-to-r from-primary-300 to-primary-500 bg-clip-text text-transparent">
              Hospitalaria
            </span>
          </h2>
          <p className="text-white/50 text-base leading-relaxed">
            Plataforma integral para la gestión clínica, administrativa y operativa.
            Diseñada con tecnología determinista de nueva generación.
          </p>

          {/* Feature pills */}
          <div className="flex flex-wrap gap-2 mt-8">
            {['Auditoría completa', 'Time-Travel', 'Multi-dominio', 'Determinista'].map((feat, i) => (
              <motion.span
                key={feat}
                initial={{ opacity: 0, scale: 0.8 }}
                animate={{ opacity: 1, scale: 1 }}
                transition={{ delay: 0.6 + i * 0.1 }}
                className="px-3 py-1.5 rounded-full text-xs font-medium bg-white/5 text-white/60 border border-white/10 backdrop-blur-sm"
              >
                {feat}
              </motion.span>
            ))}
          </div>
        </motion.div>

        {/* Bottom */}
        <motion.div
          initial={{ opacity: 0 }}
          animate={{ opacity: 1 }}
          transition={{ delay: 1 }}
          className="relative z-10 flex items-center gap-2 text-white/30"
        >
          <BoltIcon className="w-3.5 h-3.5" />
          <span className="text-[11px] tracking-wider font-medium">POWERED BY ASQL — DETERMINISTIC SQL ENGINE</span>
        </motion.div>
      </div>

      {/* Right panel — login form */}
      <div className="flex-1 flex items-center justify-center p-8 bg-surface-50">
        <motion.div
          initial={{ opacity: 0, x: 20 }}
          animate={{ opacity: 1, x: 0 }}
          transition={{ delay: 0.3, duration: 0.5 }}
          className="w-full max-w-sm"
        >
          {/* Mobile logo */}
          <div className="lg:hidden flex items-center gap-3 mb-10">
            <div className="w-10 h-10 rounded-xl bg-primary-100 flex items-center justify-center">
              <HospitalIcon className="w-5 h-5 text-primary-600" />
            </div>
            <span className="text-surface-800 text-lg font-bold tracking-tight">Hospital MiKS</span>
          </div>

          <div className="mb-8">
            <h1 className="text-2xl font-bold text-surface-900 tracking-tight">Bienvenido</h1>
            <p className="text-sm text-surface-400 mt-1">Inicia sesión para acceder al sistema</p>
          </div>

          <form onSubmit={handleSubmit} className="space-y-5">
            {error && (
              <motion.div
                initial={{ opacity: 0, y: -8 }}
                animate={{ opacity: 1, y: 0 }}
                className="bg-red-50 border border-red-100 text-red-600 text-sm rounded-xl p-3.5 flex items-center gap-2"
              >
                <svg className="w-4 h-4 flex-shrink-0" fill="currentColor" viewBox="0 0 20 20">
                  <path fillRule="evenodd" d="M10 18a8 8 0 100-16 8 8 0 000 16zM8.28 7.22a.75.75 0 00-1.06 1.06L8.94 10l-1.72 1.72a.75.75 0 101.06 1.06L10 11.06l1.72 1.72a.75.75 0 101.06-1.06L11.06 10l1.72-1.72a.75.75 0 00-1.06-1.06L10 8.94 8.28 7.22z" clipRule="evenodd" />
                </svg>
                {error}
              </motion.div>
            )}

            <div>
              <label className="block text-xs font-semibold text-surface-500 uppercase tracking-wider mb-2">
                Usuario
              </label>
              <input
                type="text"
                value={username}
                onChange={e => setUsername(e.target.value)}
                className="input"
                placeholder="admin"
                required
                autoFocus
              />
            </div>

            <div>
              <label className="block text-xs font-semibold text-surface-500 uppercase tracking-wider mb-2">
                Contraseña
              </label>
              <input
                type="password"
                value={password}
                onChange={e => setPassword(e.target.value)}
                className="input"
                placeholder="••••••••"
                required
              />
            </div>

            <button
              type="submit"
              disabled={loading}
              className="btn-primary w-full py-3 text-sm relative overflow-hidden"
            >
              {loading ? (
                <span className="flex items-center justify-center gap-2">
                  <motion.div
                    animate={{ rotate: 360 }}
                    transition={{ duration: 1, repeat: Infinity, ease: 'linear' }}
                    className="w-4 h-4 border-2 border-white/30 border-t-white rounded-full"
                  />
                  Iniciando sesión…
                </span>
              ) : (
                'Iniciar Sesión'
              )}
            </button>
          </form>

          <div className="mt-8 pt-6 border-t border-surface-100">
            <p className="text-[11px] text-surface-300 text-center">
              Demo: <span className="font-mono text-surface-400">admin</span> / <span className="font-mono text-surface-400">admin123</span>
            </p>
          </div>
        </motion.div>
      </div>
    </div>
  )
}
