# BankApp on ASQL

Aplicación de referencia para explorar la adopción de ASQL en un sistema de gestión bancaria.

Esta app no es una ruta paralela de onboarding.
Debe leerse como una extensión práctica de [docs/getting-started/README.md](../docs/getting-started/README.md), especialmente después de los capítulos 04–09.

El objetivo no es modelar un producto bancario completo, sino forzar un recorrido de adopción profundo sobre capacidades propias de ASQL:

- dominios explícitos,
- transacciones `DOMAIN` y `CROSS DOMAIN`,
- entidades y versionado,
- `VERSIONED FOREIGN KEY`,
- consultas `AS OF LSN`,
- `FOR HISTORY`,
- helpers temporales como `current_lsn()`, `row_lsn(...)`, `entity_version(...)` y `resolve_reference(...)`,
- fixtures deterministas.

## Responsibility boundary

- **Engine-owned concern**: dominios explícitos, referencias versionadas, historia, snapshots `AS OF LSN`, fixtures deterministas.
- **App-owned concern**: reglas de riesgo, semántica de aprobación, vocabulario regulatorio, significado de eventos y políticas de negocio.
- **Recommended integration pattern**: usar la muestra para aprender a componer primitivas de ASQL, no para convertir ASQL en un producto bancario vertical.

## Qué incluye

- [main.go](main.go): ejecutable Go que crea esquema, carga escenario y lanza inspección temporal.
- [scenario.go](scenario.go): definición determinista del escenario.
- [tx_helpers.go](tx_helpers.go): patrón Go de helper para `DOMAIN` y `CROSS DOMAIN` sin esconder la frontera transaccional.
- [fixtures/banking-core-demo-v1.json](fixtures/banking-core-demo-v1.json): fixture reproducible para validar y cargar con `asqlctl`.
- [FRICTION_LOG.md](FRICTION_LOG.md): documento de fricciones tecnológicas encontradas al adoptar ASQL.

## Cómo usar esta app dentro del onboarding

Orden recomendado:

1. recorrer [docs/getting-started/04-domains-and-transactions.md](../docs/getting-started/04-domains-and-transactions.md),
2. recorrer [docs/getting-started/05-time-travel-and-history.md](../docs/getting-started/05-time-travel-and-history.md),
3. recorrer [docs/getting-started/06-entities-and-versioned-references.md](../docs/getting-started/06-entities-and-versioned-references.md),
4. recorrer [docs/getting-started/07-fixtures-and-seeding.md](../docs/getting-started/07-fixtures-and-seeding.md),
5. revisar las convenciones compactas de adopción en [docs/getting-started/10-adoption-playbook.md](../docs/getting-started/10-adoption-playbook.md),
6. usar esta app como ejemplo profundo que combina todo lo anterior.

## Dominios usados

- `identity`: clientes y contactos.
- `ledger`: cuentas y apuntes contables.
- `payments`: solicitudes y eventos de transferencia.
- `risk`: revisiones de riesgo.

## Flujo que se ejercita

1. alta de dos clientes en `identity`,
2. apertura de cuentas en `ledger`,
3. creación de una transferencia en `payments` con referencias versionadas a cliente y cuentas,
4. aprobación en `risk`,
5. liquidación en `payments` + `ledger`,
6. actualización posterior del cliente para demostrar separación entre snapshot capturado y estado actual.

## Arranque local

### 1. Levantar ASQL

Desde la raíz del repositorio:

```bash
go run ./cmd/asqld -addr :5433 -data-dir .asql-bankapp
```

### 2. Ejecutar la aplicación

En otra terminal:

```bash
go run ./bankapp -pgwire 127.0.0.1:5433 -mode all
```

`-mode all` hace tres cosas:

- aplica el esquema,
- ejecuta el escenario,
- imprime lecturas actuales e históricas.

También puedes usar:

- `-mode schema`
- `-mode scenario`
- `-mode inspect`
- `-mode print-sql`

Importante: `schema` y `all` están pensados para un `data-dir` fresco.

## Flujo fixture-first

Validar fixture:

```bash
go run ./cmd/asqlctl -command fixture-validate -fixture-file bankapp/fixtures/banking-core-demo-v1.json
```

Cargar fixture:

```bash
go run ./cmd/asqlctl -command fixture-load -pgwire 127.0.0.1:5433 -fixture-file bankapp/fixtures/banking-core-demo-v1.json
```

## Qué observar

Tras ejecutar `-mode all`, conviene fijarse en:

- la obligación de declarar los dominios antes de cada unidad de trabajo,
- las columnas de captura temporal (`customer_version`, `source_account_version`, `destination_account_version`, `transfer_version`),
- la diferencia entre el estado actual y el estado `AS OF LSN`,
- el uso de `FOR HISTORY` para explicar una transición,
- cómo `resolve_reference(...)` devuelve el token temporal actual de una fila o entidad.

## Consultas manuales útiles

```sql
SELECT current_lsn();
SELECT row_lsn('payments.transfer_requests', 'tr-001');
SELECT entity_version('payments', 'transfer_entity', 'tr-001');
SELECT entity_head_lsn('payments', 'transfer_entity', 'tr-001');
SELECT entity_version_lsn('payments', 'transfer_entity', 'tr-001', 1);
SELECT resolve_reference('identity.customers', 'cust-001');
SELECT * FROM payments.transfer_requests FOR HISTORY WHERE id = 'tr-001';
```

## Lectura recomendada

- [docs/getting-started/04-domains-and-transactions.md](../docs/getting-started/04-domains-and-transactions.md)
- [docs/getting-started/05-time-travel-and-history.md](../docs/getting-started/05-time-travel-and-history.md)
- [docs/getting-started/06-entities-and-versioned-references.md](../docs/getting-started/06-entities-and-versioned-references.md)
- [docs/getting-started/07-fixtures-and-seeding.md](../docs/getting-started/07-fixtures-and-seeding.md)
- [docs/getting-started/09-go-sdk-and-integration.md](../docs/getting-started/09-go-sdk-and-integration.md)

Si el equipo duda sobre `ROOT` e `INCLUDES`, revisar el checklist en [docs/getting-started/06-entities-and-versioned-references.md](../docs/getting-started/06-entities-and-versioned-references.md).
