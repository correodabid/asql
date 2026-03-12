import { createContext, useContext, useState, useEffect, ReactNode } from 'react'
import { api } from '../services/api'

interface AuthState {
  token: string | null
  userId: string | null
  staffId: string | null
  role: string | null
}

interface AuthContextValue extends AuthState {
  login: (username: string, password: string) => Promise<void>
  logout: () => void
  isAuthenticated: boolean
}

const AuthContext = createContext<AuthContextValue | null>(null)

const STORAGE_KEY = 'hospital-miks-auth'

export function AuthProvider({ children }: { children: ReactNode }) {
  const [state, setState] = useState<AuthState>(() => {
    const saved = localStorage.getItem(STORAGE_KEY)
    if (saved) {
      try {
        const parsed = JSON.parse(saved) as AuthState
        if (parsed.token) {
          api.setToken(parsed.token)
        }
        return parsed
      } catch { /* ignore corrupt data */ }
    }
    return { token: null, userId: null, staffId: null, role: null }
  })

  useEffect(() => {
    if (state.token) {
      localStorage.setItem(STORAGE_KEY, JSON.stringify(state))
    } else {
      localStorage.removeItem(STORAGE_KEY)
    }
  }, [state])

  async function login(username: string, password: string) {
    const res = await api.post<{
      token: string; user_id: string; staff_id: string; role: string
    }>('/auth/login', { username, password })
    const { token, user_id, staff_id, role } = res.data
    api.setToken(token)
    setState({ token, userId: user_id, staffId: staff_id, role })
  }

  function logout() {
    api.setToken('')
    setState({ token: null, userId: null, staffId: null, role: null })
  }

  return (
    <AuthContext.Provider value={{ ...state, login, logout, isAuthenticated: !!state.token }}>
      {children}
    </AuthContext.Provider>
  )
}

export function useAuth() {
  const ctx = useContext(AuthContext)
  if (!ctx) throw new Error('useAuth must be used within AuthProvider')
  return ctx
}
