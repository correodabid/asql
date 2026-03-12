import type { ReactNode } from 'react'
import {
  DashboardIcon, UsersIcon, HeartIcon, CalendarIcon, BeakerIcon,
  ScissorsIcon, BedIcon, ClockIcon, ActivityIcon, CurrencyIcon,
  EnvelopeIcon, DocumentIcon, ShieldCheckIcon, BoltIcon,
} from './icons'

export interface NavItem {
  to: string
  label: string
  icon: (props: { className?: string }) => ReactNode
  badge?: string
}

export const navSections: { section: string; items: NavItem[] }[] = [
  {
    section: 'Principal',
    items: [
      { to: '/', label: 'Dashboard', icon: DashboardIcon },
    ],
  },
  {
    section: 'Gestión Clínica',
    items: [
      { to: '/staff', label: 'Personal', icon: UsersIcon },
      { to: '/patients', label: 'Pacientes', icon: HeartIcon },
      { to: '/appointments', label: 'Citas', icon: CalendarIcon },
      { to: '/surgery', label: 'Quirófanos', icon: ScissorsIcon },
      { to: '/admissions', label: 'Ingresos', icon: BedIcon },
      { to: '/rehab', label: 'Rehabilitación', icon: ActivityIcon },
    ],
  },
  {
    section: 'Servicios',
    items: [
      { to: '/pharmacy', label: 'Farmacia', icon: BeakerIcon },
      { to: '/guard-shifts', label: 'Guardias', icon: ClockIcon },
      { to: '/billing', label: 'Facturación', icon: CurrencyIcon },
      { to: '/messaging', label: 'Mensajería', icon: EnvelopeIcon },
      { to: '/documents', label: 'Documentos', icon: DocumentIcon },
    ],
  },
  {
    section: 'Sistema',
    items: [
      { to: '/audit', label: 'Auditoría', icon: ShieldCheckIcon },
      { to: '/explorer', label: 'ASQL Explorer', icon: BoltIcon },
    ],
  },
]

export const flatNavItems = navSections.flatMap((section) => section.items.map((item) => ({ ...item, section: section.section })))

export function getCurrentPageTitle(pathname: string): string {
  const titles: Record<string, string> = {
    '/': 'Dashboard',
    '/staff': 'Gestión de Personal',
    '/patients': 'Gestión de Pacientes',
    '/appointments': 'Gestión de Citas',
    '/pharmacy': 'Farmacia',
    '/surgery': 'Quirófanos',
    '/admissions': 'Ingresos',
    '/guard-shifts': 'Guardias',
    '/rehab': 'Rehabilitación',
    '/billing': 'Facturación',
    '/messaging': 'Mensajería',
    '/documents': 'Documentos',
    '/audit': 'Auditoría',
    '/explorer': 'ASQL Explorer',
  }
  return titles[pathname] ?? 'Hospital MiKS'
}
