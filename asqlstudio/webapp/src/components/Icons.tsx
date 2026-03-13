/* Inline SVG icons – zero dependencies, tree-shakeable */

const sz = { width: 16, height: 16, viewBox: '0 0 24 24', fill: 'none', stroke: 'currentColor', strokeWidth: 2, strokeLinecap: 'round' as const, strokeLinejoin: 'round' as const }

export const IconTable = () => (
  <svg {...sz}><rect x="3" y="3" width="18" height="18" rx="2" /><path d="M3 9h18M3 15h18M9 3v18" /></svg>
)

export const IconColumn = () => (
  <svg {...sz}><path d="M12 3v18M3 12h18" /><circle cx="12" cy="12" r="2" /></svg>
)

export const IconPlus = () => (
  <svg {...sz}><path d="M12 5v14M5 12h14" /></svg>
)

export const IconTrash = () => (
  <svg {...sz}><path d="M3 6h18M19 6v14a2 2 0 01-2 2H7a2 2 0 01-2-2V6M8 6V4a2 2 0 012-2h4a2 2 0 012 2v2" /></svg>
)

export const IconKey = () => (
  <svg {...sz}><path d="M21 2l-2 2m-7.61 7.61a5.5 5.5 0 11-7.778 7.778 5.5 5.5 0 017.777-7.777zm0 0L15.5 7.5m0 0l3 3L22 7l-3-3m-3.5 3.5L19 4" /></svg>
)

export const IconLink = () => (
  <svg {...sz}><path d="M10 13a5 5 0 007.54.54l3-3a5 5 0 00-7.07-7.07l-1.72 1.71M14 11a5 5 0 00-7.54-.54l-3 3a5 5 0 007.07 7.07l1.71-1.71" /></svg>
)

export const IconCode = () => (
  <svg {...sz}><polyline points="16 18 22 12 16 6" /><polyline points="8 6 2 12 8 18" /></svg>
)

export const IconDiff = () => (
  <svg {...sz}><path d="M12 3v18M3 12h6M15 12h6M3 6h18M3 18h18" /></svg>
)

export const IconPlay = () => (
  <svg {...sz}><polygon points="5 3 19 12 5 21 5 3" /></svg>
)

export const IconRefresh = () => (
  <svg {...sz}><polyline points="23 4 23 10 17 10" /><path d="M20.49 15a9 9 0 11-2.12-9.36L23 10" /></svg>
)

export const IconDownload = () => (
  <svg {...sz}><path d="M21 15v4a2 2 0 01-2 2H5a2 2 0 01-2-2v-4M7 10l5 5 5-5M12 15V3" /></svg>
)

export const IconCheck = () => (
  <svg {...sz}><polyline points="20 6 9 17 4 12" /></svg>
)

export const IconShield = () => (
  <svg {...sz}><path d="M12 22s8-4 8-10V5l-8-3-8 3v7c0 6 8 10 8 10z" /></svg>
)

export const IconActivity = () => (
  <svg {...sz}><polyline points="22 12 18 12 15 21 9 3 6 12 2 12" /></svg>
)

export const IconTimeline = () => (
  <svg {...sz}><circle cx="12" cy="12" r="9" /><polyline points="12 7 12 12 15 15" /><path d="M16.51 17.35l-.35 3.83a2 2 0 01-2 1.82H9.83a2 2 0 01-2-1.82l-.35-3.83m.01-10.7l.35-3.83A2 2 0 019.83 1h4.35a2 2 0 011.98 1.82l.35 3.83" strokeWidth="1.5" /></svg>
)

export const IconDatabase = () => (
  <svg {...sz}><ellipse cx="12" cy="5" rx="9" ry="3" /><path d="M21 12c0 1.66-4 3-9 3s-9-1.34-9-3" /><path d="M3 5v14c0 1.66 4 3 9 3s9-1.34 9-3V5" /></svg>
)

export const IconSchema = () => (
  <svg {...sz}><rect x="2" y="2" width="8" height="8" rx="2" /><rect x="14" y="2" width="8" height="8" rx="2" /><rect x="8" y="14" width="8" height="8" rx="2" /><path d="M6 10v2a2 2 0 002 2h0M18 10v2a2 2 0 01-2 2h0" /></svg>
)

export const IconCopy = () => (
  <svg {...sz}><rect x="9" y="9" width="13" height="13" rx="2" /><path d="M5 15H4a2 2 0 01-2-2V4a2 2 0 012-2h9a2 2 0 012 2v1" /></svg>
)

export const IconEye = () => (
  <svg {...sz}><path d="M1 12s4-7 11-7 11 7 11 7-4 7-11 7S1 12 1 12z" /><circle cx="12" cy="12" r="3" /></svg>
)

export const IconChevronDown = () => (
  <svg {...sz} width={12} height={12}><polyline points="6 9 12 15 18 9" /></svg>
)

export const IconDot = ({ color = 'currentColor' }: { color?: string }) => (
  <svg width={8} height={8} viewBox="0 0 8 8"><circle cx="4" cy="4" r="4" fill={color} /></svg>
)

export const IconUnique = () => (
  <svg {...sz}><path d="M12 2L2 7l10 5 10-5-10-5zM2 17l10 5 10-5M2 12l10 5 10-5" /></svg>
)

export const IconNullable = () => (
  <svg {...sz}><circle cx="12" cy="12" r="10" /><path d="M8 12h8" /></svg>
)

export const IconTerminal = () => (
  <svg {...sz}><polyline points="4 17 10 11 4 5" /><line x1="12" y1="19" x2="20" y2="19" /></svg>
)

export const IconGrid = () => (
  <svg {...sz}><rect x="3" y="3" width="7" height="7" /><rect x="14" y="3" width="7" height="7" /><rect x="14" y="14" width="7" height="7" /><rect x="3" y="14" width="7" height="7" /></svg>
)

export const IconUndo = () => (
  <svg {...sz} fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
    <path d="M3 7v6h6" />
    <path d="M21 17A9 9 0 0 0 6.3 7.3L3 10" />
  </svg>
)

export const IconRedo = () => (
  <svg {...sz} fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
    <path d="M21 7v6h-6" />
    <path d="M3 17A9 9 0 0 1 17.7 7.3L21 10" />
  </svg>
)

export const IconHistory = () => (
  <svg {...sz}><circle cx="12" cy="12" r="10" /><polyline points="12 6 12 12 16 14" /></svg>
)

export const IconClock = () => (
  <svg {...sz}><circle cx="12" cy="12" r="10" /><polyline points="12 6 12 12 16 14" /></svg>
)

export const IconMinus = () => (
  <svg {...sz}><path d="M5 12h14" /></svg>
)

export const IconMaximize = () => (
  <svg {...sz}>
    <path d="M8 3H5a2 2 0 00-2 2v3M21 8V5a2 2 0 00-2-2h-3M3 16v3a2 2 0 002 2h3M16 21h3a2 2 0 002-2v-3" />
  </svg>
)

export const IconStar = () => (
  <svg {...sz}><polygon points="12 2 15.09 8.26 22 9.27 17 14.14 18.18 21.02 12 17.77 5.82 21.02 7 14.14 2 9.27 8.91 8.26 12 2" /></svg>
)

export const IconStarFilled = () => (
  <svg {...sz} fill="currentColor"><polygon points="12 2 15.09 8.26 22 9.27 17 14.14 18.18 21.02 12 17.77 5.82 21.02 7 14.14 2 9.27 8.91 8.26 12 2" /></svg>
)

export const IconSearch = () => (
  <svg {...sz}><circle cx="11" cy="11" r="8" /><path d="M21 21l-4.35-4.35" /></svg>
)

export const IconX = () => (
  <svg {...sz}><path d="M18 6L6 18M6 6l12 12" /></svg>
)

export const IconGauge = () => (
  <svg {...sz}><path d="M12 12l3.5-3.5" strokeWidth="2.5" /><circle cx="12" cy="12" r="10" /><path d="M16.24 7.76a6 6 0 010 8.49M7.76 7.76a6 6 0 000 8.49" /></svg>
)

export const IconTrendingUp = () => (
  <svg {...sz}><polyline points="23 6 13.5 15.5 8.5 10.5 1 18" /><polyline points="17 6 23 6 23 12" /></svg>
)

export const IconZap = () => (
  <svg {...sz}><polygon points="13 2 3 14 12 14 11 22 21 10 12 10 13 2" /></svg>
)

export const IconFormat = () => (
  <svg {...sz}><path d="M4 7h16M4 12h10M4 17h12" /></svg>
)

export const IconSun = () => (
  <svg {...sz}><circle cx="12" cy="12" r="5" /><line x1="12" y1="1" x2="12" y2="3" /><line x1="12" y1="21" x2="12" y2="23" /><line x1="4.22" y1="4.22" x2="5.64" y2="5.64" /><line x1="18.36" y1="18.36" x2="19.78" y2="19.78" /><line x1="1" y1="12" x2="3" y2="12" /><line x1="21" y1="12" x2="23" y2="12" /><line x1="4.22" y1="19.78" x2="5.64" y2="18.36" /><line x1="18.36" y1="5.64" x2="19.78" y2="4.22" /></svg>
)

export const IconMoon = () => (
  <svg {...sz}><path d="M21 12.79A9 9 0 1111.21 3 7 7 0 0021 12.79z" /></svg>
)

export const IconExpand = () => (
  <svg {...sz}><polyline points="15 3 21 3 21 9" /><polyline points="9 21 3 21 3 15" /><line x1="21" y1="3" x2="14" y2="10" /><line x1="3" y1="21" x2="10" y2="14" /></svg>
)

export const IconUpload = () => (
  <svg {...sz}><path d="M21 15v4a2 2 0 01-2 2H5a2 2 0 01-2-2v-4M17 8l-5-5-5 5M12 3v12" /></svg>
)

export const IconImage = () => (
  <svg {...sz}><rect x="3" y="3" width="18" height="18" rx="2" /><circle cx="8.5" cy="8.5" r="1.5" /><polyline points="21 15 16 10 5 21" /></svg>
)

export const IconLayers = () => (
  <svg {...sz}><polygon points="12 2 2 7 12 12 22 7 12 2" /><polyline points="2 17 12 22 22 17" /><polyline points="2 12 12 17 22 12" /></svg>
)

export const IconArrowRight = () => (
  <svg {...sz}><line x1="5" y1="12" x2="19" y2="12" /><polyline points="12 5 19 12 12 19" /></svg>
)

export const IconFilter = () => (
  <svg {...sz}><polygon points="22 3 2 3 10 12.46 10 19 14 21 14 12.46 22 3" /></svg>
)

export const IconTrendingDown = () => (
  <svg {...sz}><polyline points="23 18 13.5 8.5 8.5 13.5 1 6" /><polyline points="17 18 23 18 23 12" /></svg>
)

export const IconAlertTriangle = () => (
  <svg {...sz}><path d="M10.29 3.86L1.82 18a2 2 0 001.71 3h16.94a2 2 0 001.71-3L13.71 3.86a2 2 0 00-3.42 0z" /><line x1="12" y1="9" x2="12" y2="13" /><line x1="12" y1="17" x2="12.01" y2="17" /></svg>
)

export const IconInfo = ({ className }: { className?: string }) => (
  <svg {...sz} className={className}><circle cx="12" cy="12" r="10" /><line x1="12" y1="16" x2="12" y2="12" /><line x1="12" y1="8" x2="12.01" y2="8" /></svg>
)

export const IconCpu = () => (
  <svg {...sz}><rect x="4" y="4" width="16" height="16" rx="2" /><rect x="9" y="9" width="6" height="6" /><path d="M9 1v3M15 1v3M9 20v3M15 20v3M20 9h3M20 14h3M1 9h3M1 14h3" /></svg>
)

export const IconServer = () => (
  <svg {...sz}><rect x="2" y="2" width="20" height="8" rx="2" /><rect x="2" y="14" width="20" height="8" rx="2" /><line x1="6" y1="6" x2="6.01" y2="6" /><line x1="6" y1="18" x2="6.01" y2="18" /></svg>
)

export const IconPause = () => (
  <svg {...sz}><rect x="6" y="4" width="4" height="16" /><rect x="14" y="4" width="4" height="16" /></svg>
)

export const IconSkipBack = () => (
  <svg {...sz}><polygon points="19 20 9 12 19 4 19 20" /><line x1="5" y1="19" x2="5" y2="5" /></svg>
)

export const IconSkipForward = () => (
  <svg {...sz}><polygon points="5 4 15 12 5 20 5 4" /><line x1="19" y1="5" x2="19" y2="19" /></svg>
)

export const IconChevronLeft = () => (
  <svg {...sz}><polyline points="15 18 9 12 15 6" /></svg>
)

export const IconChevronRight = () => (
  <svg {...sz}><polyline points="9 18 15 12 9 6" /></svg>
)

export const IconZoomIn = () => (
  <svg {...sz}><circle cx="11" cy="11" r="8" /><line x1="21" y1="21" x2="16.65" y2="16.65" /><line x1="11" y1="8" x2="11" y2="14" /><line x1="8" y1="11" x2="14" y2="11" /></svg>
)

export const IconZoomOut = () => (
  <svg {...sz}><circle cx="11" cy="11" r="8" /><line x1="21" y1="21" x2="16.65" y2="16.65" /><line x1="8" y1="11" x2="14" y2="11" /></svg>
)

export const IconChevronUp = () => (
  <svg {...sz}><polyline points="18 15 12 9 6 15" /></svg>
)

export const IconCheckCircle = () => (
  <svg {...sz}><path d="M22 11.08V12a10 10 0 11-5.93-9.14" /><polyline points="22 4 12 14.01 9 11.01" /></svg>
)

export const IconXCircle = () => (
  <svg {...sz}><circle cx="12" cy="12" r="10" /><line x1="15" y1="9" x2="9" y2="15" /><line x1="9" y1="9" x2="15" y2="15" /></svg>
)

export const IconToggleLeft = () => (
  <svg {...sz}><rect x="1" y="5" width="22" height="14" rx="7" /><circle cx="8" cy="12" r="3" /></svg>
)

export const IconToggleRight = () => (
  <svg {...sz}><rect x="1" y="5" width="22" height="14" rx="7" /><circle cx="16" cy="12" r="3" /></svg>
)

export const IconGitMerge = () => (
  <svg {...sz}><circle cx="18" cy="18" r="3" /><circle cx="6" cy="6" r="3" /><path d="M6 21V9a9 9 0 009 9" /></svg>
)

export const IconList = () => (
  <svg {...sz}><line x1="8" y1="6" x2="21" y2="6" /><line x1="8" y1="12" x2="21" y2="12" /><line x1="8" y1="18" x2="21" y2="18" /><line x1="3" y1="6" x2="3.01" y2="6" /><line x1="3" y1="12" x2="3.01" y2="12" /><line x1="3" y1="18" x2="3.01" y2="18" /></svg>
)

/** Two diverging arrows — row comparison / diff */
export const IconCompare = () => (
  <svg {...sz}>
    <circle cx="18" cy="18" r="3" />
    <circle cx="6" cy="6" r="3" />
    <path d="M13 6h3a2 2 0 012 2v7" />
    <path d="M11 18H8a2 2 0 01-2-2V9" />
    <polyline points="15 9 18 6 21 9" />
    <polyline points="9 15 6 18 3 15" />
  </svg>
)

