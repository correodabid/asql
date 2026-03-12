# Pharma Manufacturing App on ASQL

Aplicación de referencia para explorar la adopción de ASQL en un entorno de pharma manufacturing con trazabilidad y evidencias de cumplimiento.

Esta app no intenta convertir ASQL en un producto vertical.
Debe leerse como una extensión práctica de [docs/getting-started/README.md](../docs/getting-started/README.md), especialmente después de los capítulos 04–09.

El objetivo es forzar una adopción profunda de primitivas propias de ASQL en un caso donde la trazabilidad importa mucho:

- dominios explícitos,
- transacciones `DOMAIN` y `CROSS DOMAIN`,
- entidades y versionado,
- `VERSIONED FOREIGN KEY`,
- consultas `AS OF LSN`,
- `FOR HISTORY`,
- helpers temporales como `current_lsn()`, `row_lsn(...)`, `entity_version(...)`, `entity_version_lsn(...)` y `resolve_reference(...)`,
- fixtures deterministas,
- integración Go vía pgwire con `pgx`.

## Responsibility boundary

- **Engine-owned concern**: fronteras explícitas, referencias versionadas, historia, replay-safe snapshots, fixtures deterministas, observabilidad temporal.
- **App-owned concern**: significado regulatorio de firmas, clasificación de desviaciones, semántica de revisión QA, políticas GxP y vocabulario de cumplimiento.
- **Recommended integration pattern**: usar la muestra para aprender a componer primitivas de ASQL, no para empujar semántica farmacéutica al motor.

## Qué incluye

- [main.go](main.go): ejecutable Go que crea esquema, carga escenario y lanza inspección temporal.
- [scenario.go](scenario.go): definición determinista del escenario.
- [tx_helpers.go](tx_helpers.go): patrón Go de helper para `DOMAIN` y `CROSS DOMAIN` sin esconder la frontera transaccional.
- [fixtures/pharma-manufacturing-demo-v1.json](fixtures/pharma-manufacturing-demo-v1.json): fixture reproducible para validar y cargar con `asqlctl`.
- [FRICTION_LOG.md](FRICTION_LOG.md): documento de fricciones tecnológicas encontradas al adoptar ASQL.

## Cómo usar esta app dentro del onboarding

Orden recomendado:

1. recorrer [docs/getting-started/04-domains-and-transactions.md](../docs/getting-started/04-domains-and-transactions.md),
2. recorrer [docs/getting-started/05-time-travel-and-history.md](../docs/getting-started/05-time-travel-and-history.md),
3. recorrer [docs/getting-started/06-entities-and-versioned-references.md](../docs/getting-started/06-entities-and-versioned-references.md),
4. recorrer [docs/getting-started/07-fixtures-and-seeding.md](../docs/getting-started/07-fixtures-and-seeding.md),
5. recorrer [docs/getting-started/09-go-sdk-and-integration.md](../docs/getting-started/09-go-sdk-and-integration.md),
6. usar esta app como ejemplo profundo que combina todo lo anterior.

## Dominios usados

- `recipe`: master recipes, operaciones y parámetros de proceso.
- `inventory`: lotes de materiales y reservas.
- `execution`: process orders, pasos de batch y materiales consumidos.
- `quality`: desviaciones y revisiones QA.
- `compliance`: firmas y atestaciones asociadas a snapshots versionados.

## Flujo que se ejercita

1. alta de una master recipe versionada en `recipe`,
2. carga de lotes liberados en `inventory`,
3. creación de un batch order que captura la versión exacta de la recipe y de los lotes reservados,
4. arranque del batch con firma en `compliance`,
5. apertura de una desviación y puesta en hold del batch,
6. revisión posterior de la recipe para demostrar separación entre la versión capturada por el batch y la versión actual,
7. cierre de desviación y liberación del batch con nuevas atestaciones.

## Arranque local

### 1. Levantar ASQL

Desde la raíz del repositorio:

```bash
go run ./cmd/asqld -addr :5433 -data-dir .asql-pharmaapp
```

### 2. Ejecutar la aplicación

En otra terminal:

```bash
go run ./pharmaapp -pgwire 127.0.0.1:5433 -mode all
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
go run ./cmd/asqlctl -command fixture-validate -fixture-file pharmaapp/fixtures/pharma-manufacturing-demo-v1.json
```

Cargar fixture:

```bash
go run ./cmd/asqlctl -command fixture-load -pgwire 127.0.0.1:5433 -fixture-file pharmaapp/fixtures/pharma-manufacturing-demo-v1.json
```

## Qué observar

Tras ejecutar `-mode all`, conviene fijarse en:

- la obligación de declarar los dominios antes de cada unidad de trabajo,
- las columnas de captura temporal (`recipe_version`, `lot_version`, `batch_version`, `deviation_version`),
- la diferencia entre la recipe actual y la recipe capturada por el batch,
- el uso de `FOR HISTORY` para explicar estados de batch, recipe y deviation,
- cómo `resolve_reference(...)` devuelve el token temporal actual de una fila o entidad,
- cómo la app necesita helpers propios para convertir primitivas temporales en explicaciones de negocio.

## Consultas manuales útiles

```sql
SELECT current_lsn();
SELECT row_lsn('execution.batch_orders', 'batch-001');
SELECT entity_version('execution', 'batch_record_entity', 'batch-001');
SELECT entity_version_lsn('execution', 'batch_record_entity', 'batch-001', 3);
SELECT entity_version('recipe', 'master_recipe_entity', 'recipe-001');
SELECT resolve_reference('recipe.master_recipes', 'recipe-001');
SELECT * FROM execution.batch_orders FOR HISTORY WHERE id = 'batch-001';
SELECT * FROM recipe.master_recipes AS OF LSN 8 WHERE id = 'recipe-001';
```

## Lectura recomendada

- [docs/getting-started/04-domains-and-transactions.md](../docs/getting-started/04-domains-and-transactions.md)
- [docs/getting-started/05-time-travel-and-history.md](../docs/getting-started/05-time-travel-and-history.md)
- [docs/getting-started/06-entities-and-versioned-references.md](../docs/getting-started/06-entities-and-versioned-references.md)
- [docs/getting-started/07-fixtures-and-seeding.md](../docs/getting-started/07-fixtures-and-seeding.md)
- [docs/getting-started/09-go-sdk-and-integration.md](../docs/getting-started/09-go-sdk-and-integration.md)

Si el equipo duda sobre `ROOT` e `INCLUDES`, revisar el checklist en [docs/getting-started/06-entities-and-versioned-references.md](../docs/getting-started/06-entities-and-versioned-references.md).
