# BankApp – ASQL Friction Log

## Objetivo

Este documento recoge fricciones detectadas al construir una aplicación de referencia que usa ASQL de forma intensa.

El foco es exclusivamente tecnológico:

- fricciones del motor,
- de su modelo mental,
- de su superficie SQL/pgwire,
- de su ergonomía de integración,
- y de su workflow operativo.

No se consideran fricciones propias del dominio bancario.

## Qué se forzó en la muestra

La muestra usa deliberadamente:

- 4 dominios explícitos,
- transacciones `BEGIN DOMAIN ...` y `BEGIN CROSS DOMAIN ...`,
- entidades (`CREATE ENTITY`),
- `VERSIONED FOREIGN KEY`,
- helpers temporales,
- `FOR HISTORY`,
- `AS OF LSN`,
- fixture determinista,
- integración Go vía pgwire con `pgx`.

Eso hace visible dónde un equipo sufrirá durante las primeras semanas de adopción.

## Fricciones observadas

### 1. La selección de dominio entra en el código de aplicación muy pronto

**Dónde aparece**

En cuanto se implementa el primer caso con más de una frontera (`identity`, `ledger`, `payments`, `risk`), la aplicación tiene que decidir en cada operación si usa `DOMAIN` o `CROSS DOMAIN`.

**Por qué es fricción de ASQL**

No es una molestia del ejemplo. Es parte del contrato central de ASQL: las fronteras no son implícitas.

**Impacto en adopción**

- obliga a crear helpers de transacción desde el inicio,
- rompe repositorios o servicios que asumían transacciones invisibles,
- empuja a rediseñar la capa de aplicación antes de que el equipo domine el resto de capacidades.

**Oportunidad de producto**

- patrones oficiales para helpers de transacción por dominio,
- diagnóstico de sobreuso de `CROSS DOMAIN`,
- guía corta para descubrir fronteras correctas.

**Prioridad**: P0

---

### 2. El modelo temporal exige conceptos nuevos en el esquema, no solo en las consultas

**Dónde aparece**

Para usar referencias versionadas hay que añadir columnas explícitas como `customer_version`, `source_account_version` o `transfer_version`.

**Por qué es fricción de ASQL**

La capacidad temporal no vive solo en runtime. Afecta al diseño físico de tablas y a las migraciones.

**Impacto en adopción**

- el equipo debe aprender cuándo almacenar `LSN` y cuándo versión de entidad,
- aparecen columnas técnicas que no existen en esquemas SQL convencionales,
- cuesta explicar a equipos de producto por qué hay campos “extra” que el motor gestiona indirectamente.

**Oportunidad de producto**

- plantillas de esquema para referencias versionadas,
- guía comparativa: fila normal vs entidad vs referencia versionada,
- validaciones o sugerencias al definir `VERSIONED FOREIGN KEY`.

**Prioridad**: P0

---

### 3. `CREATE ENTITY` aporta mucho valor, pero exige modelado previo muy claro

**Dónde aparece**

La aplicación necesita decidir qué tabla es `ROOT` y qué tablas entran en `INCLUDES` antes de capturar versiones de forma útil.

**Por qué es fricción de ASQL**

ASQL ofrece una capa de agregados potente, pero no hay un camino trivial para equipos que vienen de modelado puramente relacional.

**Impacto en adopción**

- puede retrasar la adopción de entidades hasta demasiado tarde,
- o provocar entidades mal definidas que después contaminan la semántica temporal,
- introduce una decisión arquitectónica fuerte en una fase temprana.

**Oportunidad de producto**

- guía de modelado de entidades con ejemplos multi-industria,
- checklist para decidir cuándo una tabla debe ser `ROOT`,
- tooling o validaciones para entidades sospechosamente grandes o vacías.

**Prioridad**: P1

---

### 4. La depuración temporal es muy potente, pero todavía demasiado artesanal

**Dónde aparece**

La muestra necesita combinar manualmente:

- `current_lsn()`,
- `row_lsn(...)`,
- `entity_version(...)`,
- `entity_version_lsn(...)`,
- `AS OF LSN`,
- `FOR HISTORY`.

**Por qué es fricción de ASQL**

Todas estas piezas pertenecen al valor central del producto, pero hoy la composición recae casi por completo en quien integra.

**Impacto en adopción**

- aumenta el tiempo hasta que los equipos obtienen el beneficio real de replay/historia,
- dificulta estandarizar playbooks de incidentes,
- hace que equipos nuevos perciban la capacidad temporal como “experta” en vez de “normal”.

**Oportunidad de producto**

- cookbook con patrones de “estado actual + explicación histórica”,
- helpers SDK para snapshot, history y resolución de versiones,
- ejemplos más prescriptivos en el onboarding.

**Prioridad**: P1

---

### 5. El flujo fixture-first es correcto, pero su autoría sigue siendo costosa

**Dónde aparece**

La fixture de la muestra es explícita, ordenada y útil, pero escribirla a mano requiere mucho detalle.

**Por qué es fricción de ASQL**

ASQL exige determinismo real. Eso endurece el formato y elimina atajos comunes de seeds ad hoc.

**Impacto en adopción**

- mantener fixtures grandes puede sentirse caro,
- la transición desde scripts SQL o seeds ORM es brusca,
- el equipo necesita aprender una disciplina adicional para tests y demos.

**Oportunidad de producto**

- mejores herramientas para derivar fixtures desde entornos locales controlados,
- mensajes de validación más orientados a aprendizaje,
- starter packs fixture-first para nuevos proyectos.

**Prioridad**: P1

---

### 6. La compatibilidad pgwire es útil, pero la superficie soportada todavía requiere vigilancia activa

**Dónde aparece**

La integración Go funciona bien con `pgx`, pero obliga a pensar en la compatibilidad real del subconjunto PostgreSQL y en detalles como `simple_protocol`.

**Por qué es fricción de ASQL**

La expectativa natural de un equipo será “si habla pgwire, mi stack PostgreSQL debería funcionar”. En la práctica, el ajuste fino depende del subconjunto soportado.

**Impacto en adopción**

- dudas tempranas sobre qué cliente, ORM o patrón SQL será seguro,
- necesidad de descubrir sorpresas por ensayo y error,
- mayor coste de integración para stacks no Go.

**Oportunidad de producto**

- matriz de compatibilidad más accionable,
- guía por tipo de cliente,
- guardrails y errores con recomendaciones concretas.

**Prioridad**: P0

---

### 7. El valor de ASQL aparece cuando la app asume responsabilidad explícita, y eso eleva la barra inicial

**Dónde aparece**

La aplicación tiene que decidir:

- qué dominios participan,
- qué mutaciones van juntas,
- qué snapshots conservar mentalmente,
- qué tablas merecen entidad,
- qué datos se observan con historia y cuáles con auditoría propia.

**Por qué es fricción de ASQL**

No es un fallo del ejemplo. Es la consecuencia directa de un motor que hace visibles fronteras, determinismo e historia.

**Impacto en adopción**

- onboarding más lento que en una base de datos relacional tradicional,
- mayor necesidad de coaching arquitectónico,
- riesgo de rechazo temprano si el equipo esperaba “Postgres con extras”.

**Oportunidad de producto**

- mejor narrativa de responsabilidad motor vs aplicación,
- material de training para mental model,
- ejemplo app de referencia subordinada al getting-started.

**Prioridad**: P0

---

### 8. Falta una capa intermedia de ergonomía entre SQL crudo y capacidades avanzadas

**Dónde aparece**

Para explotar bien ASQL, la aplicación termina teniendo utilidades propias para:

- iniciar transacciones correctas,
- registrar checkpoints de `LSN`,
- lanzar consultas temporales,
- convertir historia en explicaciones legibles.

**Por qué es fricción de ASQL**

El motor expone bien los primitives, pero todavía deja mucho ensamblaje repetitivo a cada equipo.

**Impacto en adopción**

- duplicación de helpers entre proyectos,
- inconsistencia entre equipos,
- más tiempo hasta llegar a una integración “idiomática”.

**Oportunidad de producto**

- helpers SDK genéricos,
- paquetes de referencia no verticales,
- convenios recomendados para IDs, timestamps, auditoría y lectura temporal.

**Prioridad**: P1

## Lo que no debe resolverse metiendo lógica bancaria en ASQL

Estas fricciones no justifican mover al motor:

- reglas de scoring de riesgo,
- semántica de cumplimiento normativo,
- workflow de aprobación bancaria,
- catálogos de eventos del negocio,
- modelos de actor, rol o expediente.

La mejora debe ir a:

- ergonomía general,
- documentación,
- validación,
- observabilidad,
- tooling,
- SDKs y patrones de integración.

## Conclusión

La muestra confirma que ASQL sí ofrece un valor diferencial real cuando una aplicación necesita:

- fronteras explícitas,
- snapshots reproducibles,
- historia consultable,
- referencias temporales,
- y debugging determinista.

La fricción principal no está en el caso bancario. Está en el salto de modelo mental y en la ergonomía necesaria para que un equipo llegue rápido a usar esas capacidades sin tener que reinventar patrones en cada proyecto.

La prioridad debería ser reducir fricción de adopción sin debilitar las propiedades centrales del motor.
