# Hospital MiKS – ASQL Domain Migrations

Each file represents a separate ASQL **domain** (bounded context).
Migrations must be applied in order because some domains declare
**Versioned Foreign Keys (VFK)** that reference tables in other domains.

## Execution order

1. `001_identity.sql`   – Users (auth)
2. `002_staff.sql`      – Staff & departments
3. `003_patients.sql`   – Patient registry
4. `004_clinical.sql`   – Appointments, surgeries, admissions, wards, beds, rooms
5. `005_pharmacy.sql`   – Medications, prescriptions, dispenses
6. `006_billing.sql`    – Invoices & items
7. `007_scheduling.sql` – Guard shifts
8. `008_rehab.sql`      – Rehabilitation plans & sessions
9. `009_messaging.sql`  – Internal messages & patient communications
10. `010_documents.sql`  – Document management & access log

## ASQL features used

| Feature | Where |
|---------|-------|
| Domain isolation | Every migration runs inside `BEGIN DOMAIN <name>` |
| VFK (Versioned FK) | clinical→staff, clinical→patients, pharmacy→staff, pharmacy→patients, billing→patients, etc. |
| CREATE ENTITY | Patient aggregate, Admission aggregate, Invoice aggregate |
| FOR HISTORY | Patients, prescriptions, admissions for compliance audit |
| IMPORT | Cross-domain reads (e.g. billing reads patient name) |
| Time travel (AS OF LSN) | Historical queries on any domain |

## Running migrations

```bash
# Connect to ASQL via pgwire and run each file
for f in migrations/*.sql; do
  psql -h 127.0.0.1 -p 5432 -f "$f"
done
```
