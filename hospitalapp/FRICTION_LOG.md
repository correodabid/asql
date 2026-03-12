# Fricciones de adopción de ASQL

Este documento recoge fricciones de adopción de ASQL en general.
La app hospitalaria sirve como vehículo para hacerlas visibles, pero las observaciones aplican también a otros dominios con trazabilidad fuerte, workflows multi-módulo y necesidad de reproducibilidad.

## 1. El modelado por dominios exige diseño temprano

Valor:

- hace visibles los límites reales del sistema,
- mejora determinismo, replay y explicación operativa,
- evita que las fronteras entre módulos queden implícitas.

Fricción de adopción:

- obliga a nombrar y estabilizar dominios antes de lo que muchos equipos están acostumbrados,
- destapa desacuerdos organizativos, no solo técnicos,
- hace evidente cuando el modelo real del producto todavía no está bien separado.

Patrón general:

- equipos que vienen de una sola base de datos “plana” suelen querer usar dominios como simple prefijo de esquema,
- eso lleva rápido a demasiados `BEGIN CROSS DOMAIN ...` y reduce la claridad del modelo.

## 2. La orquestación transaccional explícita cambia hábitos de desarrollo

Valor:

- deja claro qué cambia junto,
- mejora auditabilidad y depuración,
- elimina parte de la ambigüedad típica de capas ORM o servicios con efectos laterales ocultos.

Fricción de adopción:

- muchos equipos esperan que la transacción sea implícita o la delegan al framework,
- con ASQL hay que pensar y escribir el alcance transaccional de forma explícita,
- esto exige disciplina en la capa aplicación y helpers propios.

Patrón general:

- la adopción es más fácil cuando el equipo introduce wrappers por flujo de negocio y no expone SQL/tx raw a toda la base de código.

## 3. `VERSIONED FOREIGN KEY` aporta mucho valor, pero sube la complejidad conceptual

Valor:

- preserva el contexto exacto de la referencia,
- permite explicar qué versión de una entidad quedó capturada en un proceso,
- encaja muy bien con auditoría, debugging y replay.

Fricción de adopción:

- añade columnas auxiliares como `*_version` o `*_lsn`,
- alarga el DDL,
- obliga a aprender una semántica distinta de la FK tradicional.

Patrón general:

- para equipos que solo quieren integridad referencial clásica, la propuesta inicial puede parecer demasiado sofisticada,
- para equipos con requisitos temporales fuertes, el valor aparece rápido pero requiere curva de aprendizaje.

## 4. La capa temporal es potente, pero todavía de bajo nivel para muchos casos de producto

Valor:

- `FOR HISTORY`, `AS OF LSN`, `row_lsn`, `entity_version` y `entity_version_lsn` resuelven muy bien el plano temporal,
- permiten depuración reproducible y análisis forense muy difícil de conseguir en otros stacks.

Fricción de adopción:

- el lenguaje temporal es técnico y exige entender `LSN`, versiones y snapshots,
- muchas vistas funcionales requieren componer varias consultas,
- faltan abstracciones de más alto nivel para timelines de caso, incidentes o expedientes.

Patrón general:

- el motor da primitives sólidas,
- la aplicación todavía necesita construir vistas de negocio sobre esas primitives.

## 5. El compliance no desaparece: solo se vuelve más estructurable

Valor:

- ASQL da una base muy fuerte para trazabilidad y reproducibilidad,
- facilita justificar qué sabía el sistema en un instante concreto.

Fricción de adopción:

- conceptos como `actor`, `reason`, `meaning`, firma lógica, evidencia y clasificación del artefacto siguen siendo decisiones de la app,
- no todos los equipos sabrán modelarlos igual,
- sin librerías compartidas aparecen convenciones inconsistentes entre proyectos.

Patrón general:

- ASQL ayuda mucho con la base temporal y determinista,
- pero la semántica regulatoria sigue necesitando una capa de producto bien definida.

## 6. IDs, timestamps y metadatos siguen necesitando opinión de aplicación

Valor:

- el núcleo se mantiene compacto y no se acopla a un modelo de negocio concreto.

Fricción de adopción:

- el equipo tiene que decidir formatos de IDs, timestamps y payloads auditables,
- malas decisiones aquí degradan legibilidad, reproducibilidad o consistencia,
- para pilotos rápidos se echan en falta plantillas más opinionadas.

Patrón general:

- la adopción mejora cuando existe un starter kit con convenciones claras para naming, tiempo, actores y eventos.

## 7. Fixture-first ayuda mucho más que el enfoque API-first

Valor:

- encaja con la naturaleza determinista del motor,
- facilita demos reproducibles, integración y debugging,
- acelera conversaciones de producto porque fija escenarios reales.

Fricción de adopción:

- muchos equipos empiezan antes por endpoints o UI que por escenarios reproducibles,
- eso retrasa el aprendizaje de las ventajas reales de ASQL,
- sin fixtures cuesta demostrar replay, historia y equivalencia de estado.

Patrón general:

- la mejor puerta de entrada suele ser un fixture pequeño pero representativo,
- después conviene colgar sobre él el servicio, la UI y los tests.

## 8. La ergonomía actual favorece a equipos cómodos con SQL y Go

Valor:

- la superficie es clara, directa y cercana al runtime real,
- reduce magia y hace más visible el comportamiento del sistema.

Fricción de adopción:

- equipos muy dependientes de ORMs, frameworks opinionados o modelos CRUD tradicionales pueden sentir más fricción inicial,
- parte de la integración útil hoy pasa por componer helpers propios,
- la experiencia es más natural para equipos con cultura backend/infra que para equipos puramente app/framework.

Patrón general:

- ASQL premia equipos que aceptan explicitar límites, historia y transacciones,
- y penaliza mentalidades que esperan ocultar toda la complejidad detrás del framework.

## Señales positivas observadas

- una vez entendido el modelo, la historia temporal resulta muy valiosa,
- fixtures validables mejoran onboarding y discusión funcional,
- las entidades y referencias versionadas explican mejor ciertos workflows que una base relacional tradicional,
- el motor fuerza conversaciones arquitectónicas que normalmente se posponen demasiado.

## Recomendaciones de adopción

1. empezar con un solo nodo y pocos dominios,
2. elegir un escenario reproducible y modelarlo primero como fixture,
3. encapsular `BEGIN DOMAIN` y `BEGIN CROSS DOMAIN` en helpers de aplicación,
4. introducir entidades y referencias versionadas solo donde el valor temporal sea claro,
5. definir desde el principio convenciones de auditoría: `actor`, `reason`, `meaning`, timestamps e IDs,
6. construir una capa de consultas de negocio sobre las primitives temporales,
7. documentar explícitamente cuándo una operación debe ser single-domain y cuándo cross-domain.

## Documentos relacionados

- [docs/adr/0002-generalist-engine-boundary-and-adoption-surface.md](../docs/adr/0002-generalist-engine-boundary-and-adoption-surface.md)
- [docs/product/asql-adoption-friction-prioritized-backlog-v1.md](../docs/product/asql-adoption-friction-prioritized-backlog-v1.md)
