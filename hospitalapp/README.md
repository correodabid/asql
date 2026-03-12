# Hospital App sobre ASQL

Aplicación demo orientada a adopción para gestión hospitalaria con el paciente como eje.

Nota de posicionamiento:
- este documento describe un ejemplo vertical de apoyo,
- no sustituye el onboarding principal de ASQL,
- para la ruta canónica de adopción usa [docs/getting-started/README.md](../docs/getting-started/README.md).

Objetivos:

- forzar modelado explícito por dominios,
- probar flujos asistenciales reales,
- hacer visible la trazabilidad y auditoría,
- detectar puntos de fricción en la adopción de ASQL.

## Qué incluye

- servicio HTTP en Go: [hospitalapp/main.go](main.go)
- esquema multi-dominio y flujos asistenciales centrados en paciente
- fixture reproducible: [hospitalapp/fixtures/hospital-careflow-demo-v1.json](fixtures/hospital-careflow-demo-v1.json)
- endpoints para snapshot, historia y auditoría
- documento de fricciones: [hospitalapp/FRICTION_LOG.md](FRICTION_LOG.md)

## Dominios usados

- `patients`: identidad clínica y contactos
- `careflow`: episodio y encuentro
- `clinical`: triaje
- `orders`: órdenes de laboratorio y medicación
- `pharmacy`: dispensación
- `operations`: cama y cirugía
- `billing`: cargos del episodio
- `compliance`: firmas lógicas y eventos auditables

## Flujos implementados

1. registro de paciente
2. admisión
3. orden de laboratorio
4. orden y dispensación de medicación
5. reserva quirúrgica
6. alta y cierre económico

Todos los flujos escriben eventos de auditoría en el mismo commit que la acción asistencial.

## Arranque

### 1. Levantar ASQL

```text
go run ./cmd/asqld -addr :5433 -data-dir .asql
```

### 2. Levantar la app hospitalaria

```text
go run ./hospitalapp -listen :8095 -pgwire 127.0.0.1:5433
```

### 3. Crear el esquema

```text
curl -X POST http://127.0.0.1:8095/bootstrap
```

### 4. Cargar escenario demo

```text
curl -X POST http://127.0.0.1:8095/seed/demo
```

## Endpoints principales

- `POST /bootstrap`
- `POST /seed/demo`
- `POST /patients/register`
- `POST /patients/{patientID}/admissions`
- `POST /patients/{patientID}/lab-orders`
- `POST /patients/{patientID}/medication-orders`
- `POST /patients/{patientID}/surgeries`
- `POST /patients/{patientID}/discharge`
- `GET /patients/{patientID}/snapshot`
- `GET /patients/{patientID}/snapshot?lsn=<n>`
- `GET /patients/{patientID}/history`
- `GET /patients/{patientID}/audit`

## Ejemplo mínimo

### Registrar paciente

```json
{
  "patient_id": "patient-demo-001",
  "medical_record_no": "MRN-DEMO-001",
  "full_name": "Lucia Martin",
  "date_of_birth": "1990-02-01",
  "sex": "F",
  "national_id": "ID-DEMO-001",
  "contact_name": "Alberto Martin",
  "contact_relation": "SPOUSE",
  "contact_phone": "+34-600-200-200",
  "actor_id": "adm-100",
  "reason": "Front-desk registration"
}
```

### Ver snapshot actual

```text
curl http://127.0.0.1:8095/patients/patient-demo-001/snapshot
```

### Ver auditoría

```text
curl http://127.0.0.1:8095/patients/patient-demo-001/audit
```

## Uso de fixture

Validación:

```text
go run ./cmd/asqlctl -command fixture-validate -fixture-file hospitalapp/fixtures/hospital-careflow-demo-v1.json
```

Carga:

```text
go run ./cmd/asqlctl -command fixture-load -pgwire 127.0.0.1:5433 -fixture-file hospitalapp/fixtures/hospital-careflow-demo-v1.json
```

## Qué se quiere observar

- cuántas transacciones cruzan demasiados dominios,
- qué columnas/versioned FK resultan verbosas,
- qué parte del compliance queda todavía en capa aplicación,
- qué consultas históricas son cómodas y cuáles no,
- qué modelo de IDs/timestamps necesita más ergonomía.
