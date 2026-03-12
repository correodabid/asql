import { Routes, Route } from 'react-router-dom'
import { useAuth } from './context/AuthContext'
import Layout from './components/Layout'
import LoginPage from './pages/LoginPage'
import Dashboard from './pages/Dashboard'
import StaffPage from './pages/StaffPage'
import PatientsPage from './pages/PatientsPage'
import AppointmentsPage from './pages/AppointmentsPage'
import PharmacyPage from './pages/PharmacyPage'
import SurgeryPage from './pages/SurgeryPage'
import AdmissionsPage from './pages/AdmissionsPage'
import GuardShiftsPage from './pages/GuardShiftsPage'
import RehabPage from './pages/RehabPage'
import BillingPage from './pages/BillingPage'
import MessagingPage from './pages/MessagingPage'
import DocumentsPage from './pages/DocumentsPage'
import AuditPage from './pages/AuditPage'
import ASQLExplorerPage from './pages/ASQLExplorerPage'

export default function App() {
  const { isAuthenticated } = useAuth()

  if (!isAuthenticated) {
    return <LoginPage />
  }

  return (
    <Routes>
      <Route path="/" element={<Layout />}>
        <Route index element={<Dashboard />} />
        <Route path="staff" element={<StaffPage />} />
        <Route path="patients" element={<PatientsPage />} />
        <Route path="appointments" element={<AppointmentsPage />} />
        <Route path="pharmacy" element={<PharmacyPage />} />
        <Route path="surgery" element={<SurgeryPage />} />
        <Route path="admissions" element={<AdmissionsPage />} />
        <Route path="guard-shifts" element={<GuardShiftsPage />} />
        <Route path="rehab" element={<RehabPage />} />
        <Route path="billing" element={<BillingPage />} />
        <Route path="messaging" element={<MessagingPage />} />
        <Route path="documents" element={<DocumentsPage />} />
        <Route path="audit" element={<AuditPage />} />
        <Route path="explorer" element={<ASQLExplorerPage />} />
      </Route>
    </Routes>
  )
}
