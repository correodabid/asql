# Pharma Manufacturing App – ASQL Friction Log

## Objetivo

Este documento recoge fricciones detectadas al construir una aplicación de referencia que usa ASQL de forma intensa en un contexto de trazabilidad fuerte.

El foco es exclusivamente tecnológico:

- fricciones del motor,
- de su modelo mental,
- de su superficie SQL/pgwire,
- de su ergonomía de integración,
- y de su workflow operativo.

No se consideran fricciones propias del dominio pharma.
Tampoco se usan estas observaciones para pedir que ASQL absorba semántica regulatoria o vocabulario GxP.

## Qué se forzó en la muestra

La muestra usa deliberadamente:

- 5 dominios explícitos,
- transacciones `BEGIN DOMAIN ...` y `BEGIN CROSS DOMAIN ...`,
- entidades (`CREATE ENTITY`),
- `VERSIONED FOREIGN KEY`,
- helpers temporales,
- `FOR HISTORY`,
- `AS OF LSN`,
- fixture determinista,
- integración Go vía pgwire con `pgx`,
- un caso donde la aplicación necesita explicar qué recipe, qué lotes y qué versión de batch quedaron realmente capturados.

Eso hace visible dónde un equipo sufrirá durante las primeras semanas de adopción.

## Cómo leer este log

Este log no intenta demostrar que ASQL sea inadecuado para el caso.
Intenta identificar qué partes del coste de adopción pertenecen realmente al motor y, por tanto, deberían responderse con una de estas salidas de producto:

- mejores primitivas,
- mejores diagnósticos,
- mejor documentación,
- mejor tooling,
- mejores SDKs o patrones de integración,
- mejor observabilidad orientada a adopción.

La regla de lectura es simple:

- si la fricción exige más claridad o ergonomía sobre fronteras, historia, determinismo o referencias temporales, probablemente es fricción de ASQL,
- si la fricción exige vocabulario regulatorio, workflow sectorial o semántica de negocio, no pertenece al motor.

## Fricciones observadas

### 1. La frontera transaccional deja de ser una decisión invisible

**Dónde aparece**

La app necesita decidir en cada operación si el trabajo vive en un solo dominio o si debe usar `CROSS DOMAIN` entre `recipe`, `inventory`, `execution`, `quality` y `compliance`.

**Por qué es fricción de ASQL**

Es una propiedad central del producto: ASQL exige fronteras explícitas.
No es un detalle del ejemplo.

**Impacto en adopción**

- obliga a rediseñar pronto la capa de servicios,
- hace visibles decisiones que muchos equipos hoy esconden dentro de repositorios u ORMs,
- aumenta la necesidad de helpers de transacción desde el primer sprint.

**Oportunidad de producto**

- patrones oficiales para helpers de scope transaccional,
- heurísticas y métricas para detectar sobreuso de `CROSS DOMAIN`,
- guía breve para descubrir primeras fronteras útiles.

**Prioridad**: P0

---

### 2. El versionado temporal invade el esquema físico desde el día uno

**Dónde aparece**

Aparecen columnas explícitas como `recipe_version`, `lot_version`, `batch_version` y `deviation_version` en tablas de trabajo normales.

**Por qué es fricción de ASQL**

La semántica temporal no es un añadido solo de consulta.
Modifica cómo se modelan tablas, migraciones y referencias.

**Impacto en adopción**

- el equipo debe decidir cuándo capturar versión de entidad y cuándo `LSN` de fila,
- el esquema incorpora columnas técnicas poco familiares,
- sube el coste de revisión de diseño porque la historia futura depende del shape actual.

**Oportunidad de producto**

- plantillas de esquema para referencias versionadas,
- linters o sugerencias para `VERSIONED FOREIGN KEY`,
- guía comparativa entre referencia normal, referencia row-based y referencia entity-based.

**Prioridad**: P0

---

### 3. `CREATE ENTITY` aporta claridad, pero obliga a un modelado más maduro del que muchos equipos tienen al inicio

**Dónde aparece**

Hay que decidir pronto que `master_recipes`, `material_lots`, `batch_orders` y `deviations` son raíces válidas, y qué tablas deben entrar en `INCLUDES`.

**Por qué es fricción de ASQL**

ASQL ofrece una capa agregada potente, pero no existe una traducción automática desde un esquema relacional convencional.

**Impacto en adopción**

- retrasa a equipos que aún no dominan sus agregados,
- puede fijar entidades malas demasiado pronto,
- mezcla diseño lógico y estrategia de depuración histórica en la misma conversación.

**Oportunidad de producto**

- checklist más operativo para elegir `ROOT` e `INCLUDES`,
- validaciones de entidades sospechosamente grandes o ambiguas,
- ejemplos de modelado en varias industrias sin empujar verticalización del motor.

**Prioridad**: P1

---

### 4. La explicación histórica real sigue siendo demasiado manual para el valor que promete ASQL

**Dónde aparece**

Para explicar un batch hay que combinar manualmente:

- `current_lsn()`,
- `row_lsn(...)`,
- `entity_version(...)`,
- `entity_version_lsn(...)`,
- `resolve_reference(...)`,
- `FOR HISTORY`,
- `AS OF LSN`.

**Por qué es fricción de ASQL**

Estas capacidades son parte del corazón del producto, pero hoy la composición recae en quien integra.

**Impacto en adopción**

- cuesta convertir primitivas potentes en playbooks diarios,
- la depuración histórica se siente experta en vez de normal,
- cada equipo acaba inventando su propio lenguaje de inspección temporal.

**Oportunidad de producto**

- cookbook más prescriptivo,
- helpers SDK para snapshot + history + explanation,
- recorridos de Studio más guiados para análisis temporal multi-tabla.

**Prioridad**: P1

---

### 5. La promesa fixture-first es correcta, pero el coste de autoría todavía es alto

**Dónde aparece**

El fixture de la muestra es claro y determinista, pero escribirlo exige ordenar dominios, dependencias, referencias versionadas y pasos de transacción con mucho detalle.

**Por qué es fricción de ASQL**

El determinismo estricto es una propiedad del producto.
Eso endurece el formato y elimina atajos comunes de seeds informales.

**Impacto en adopción**

- curva de aprendizaje mayor para equipos que vienen de SQL scripts u ORMs,
- dificultad para mantener fixtures grandes durante cambios de esquema,
- más coste inicial antes de que la recompensa de reproducibilidad se vuelva evidente.

**Oportunidad de producto**

- tooling para derivar fixtures desde escenarios locales controlados,
- validaciones con feedback más pedagógico,
- starter packs fixture-first por tipo de integración.

**Prioridad**: P1

---

### 6. La compatibilidad pgwire ayuda, pero todavía obliga a pensar activamente en el subconjunto soportado

**Dónde aparece**

La integración Go con `pgx` funciona, pero la app sigue necesitando asumir un subconjunto PostgreSQL concreto y detalles como `simple_protocol`.

**Por qué es fricción de ASQL**

La expectativa natural es “si habla pgwire, mi stack PostgreSQL debería entrar casi sin fricción”.
La realidad es más matizada.

**Impacto en adopción**

- dudas tempranas sobre qué cliente o capa de acceso será segura,
- más necesidad de pruebas exploratorias,
- fricción adicional para stacks que no controlan bien el SQL emitido.

**Oportunidad de producto**

- matriz de compatibilidad más accionable,
- guías por tipo de cliente,
- errores con recomendaciones concretas cuando se sale del subset soportado.

**Prioridad**: P0

---

### 7. Falta una capa intermedia de ergonomía entre primitivas del motor y patrones de integración reutilizables

**Dónde aparece**

La aplicación termina creando helpers propios para:

- iniciar scopes transaccionales correctos,
- registrar checkpoints de `LSN`,
- resolver snapshots históricos,
- traducir historia bruta a explicaciones legibles.

**Por qué es fricción de ASQL**

El motor expone bien las primitivas, pero deja demasiado ensamblaje repetitivo a cada equipo.
No es un problema de modelado conceptual puro, sino de falta de una capa de integración reusable entre SQL crudo y uso cotidiano.

**Impacto en adopción**

- duplicación de utilidades entre proyectos,
- integración menos idiomática de la necesaria,
- tiempo más largo hasta que el equipo siente que “ya sabe usar ASQL bien”.

**Oportunidad de producto**

- helpers SDK genéricos,
- paquetes de referencia no verticales,
- convenciones recomendadas para transaction scopes, temporal inspection y explicaciones históricas.

**Prioridad**: P1

---

### 8. El modelo mental de ASQL exige más responsabilidad explícita y eleva la barra inicial de adopción

**Dónde aparece**

La app tiene que decidir:

- qué dominios participan,
- qué tablas deben ser entidades,
- qué snapshot quiere capturar una referencia,
- qué estado se explica con historia de fila y cuál con versión de entidad,
- qué inspecciones temporales deben entrar en los workflows operativos.

**Por qué es fricción de ASQL**

Es el coste natural de un motor que prioriza determinismo, historia y fronteras visibles.
No es lo mismo que la fricción 1, que trata la frontera transaccional concreta, ni que la 7, que trata la falta de helpers y patrones reutilizables.
Aquí el problema es el salto completo de modelo mental que el equipo debe interiorizar.

**Impacto en adopción**

- onboarding más lento que en una base de datos relacional tradicional,
- mayor necesidad de coaching arquitectónico,
- riesgo de decepción si el equipo esperaba solo “Postgres con extras”.

**Oportunidad de producto**

- narrativa más clara sobre el salto de modelo mental,
- training específico de adopción,
- apps de referencia profundas como ésta subordinadas al getting-started.

**Prioridad**: P0

---

### 9. La observabilidad de adopción todavía necesita cerrar mejor el bucle entre modelado y runtime

**Dónde aparece**

La muestra deja preguntas útiles que hoy requieren investigación manual, por ejemplo:

- cuántas transacciones cruzan demasiados dominios,
- qué referencias versionadas capturan más cambios de los esperados,
- qué entidades generan más versiones por unidad de trabajo,
- dónde se concentran las consultas `AS OF LSN` y `FOR HISTORY`.

**Por qué es fricción de ASQL**

Son señales clave para saber si un equipo está usando bien el modelo del motor, no solo para operar el runtime.

**Impacto en adopción**

- feedback lento sobre malas decisiones de modelado,
- dificultad para mejorar integraciones de forma iterativa,
- más dependencia de revisión manual de código y fixtures.

**Oportunidad de producto**

- métricas de adopción temporal y de cross-domain usage,
- vistas admin orientadas a modelado,
- diagnósticos que unan schema shape con patrones runtime.

**Prioridad**: P2

---

### 10. La evolución de esquema se vuelve más delicada cuando el diseño ya incorpora historia, entidades y referencias versionadas

**Dónde aparece**

En cuanto el modelo incorpora `CREATE ENTITY` y `VERSIONED FOREIGN KEY`, cualquier cambio en tablas, relaciones o fronteras agregadas deja de sentirse como una migración SQL ordinaria.

**Por qué es fricción de ASQL**

En ASQL, el shape del esquema no afecta solo a escrituras y lecturas actuales.
También afecta a cómo se capturan referencias, cómo se reconstruye historia y cómo se interpretan snapshots futuros.

**Impacto en adopción**

- más cautela al evolucionar el modelo,
- mayor coste de revisar migraciones porque la semántica temporal puede cambiar sin que el SQL parezca dramáticamente distinto,
- dificultad para responder con confianza qué cambios son seguros para historia, replay y explicabilidad.

**Oportunidad de producto**

- guardrails específicos para evolución de entidades y referencias versionadas,
- checklist de migración con impacto temporal explícito,
- validaciones que alerten cuando una migración cambia de facto la semántica histórica observable.

**Prioridad**: P1

---

### 11. Los errores de modelado todavía no siempre se convierten en diagnósticos de adopción suficientemente guiados

**Dónde aparece**

Cuando una referencia versionada no resuelve como se esperaba, cuando un `CROSS DOMAIN` parece excesivo, o cuando una entidad está mal delimitada, el equipo suele necesitar bastante interpretación manual para entender el problema real.

**Por qué es fricción de ASQL**

El onboarding de ASQL depende mucho de aprender su modelo mental.
Si los errores se expresan solo como fallos técnicos puntuales y no como feedback de modelado, el aprendizaje se ralentiza mucho.

**Impacto en adopción**

- más ensayo y error del necesario,
- mayor dependencia de expertos internos o revisión manual,
- más riesgo de que el equipo atribuya el problema a “comportamiento raro del motor” en lugar de a una decisión de modelado corregible.

**Oportunidad de producto**

- errores con contexto más pedagógico,
- diagnósticos que sugieran revisar `ROOT`, `INCLUDES`, alcance transaccional o tipo de referencia temporal,
- validaciones de schema y fixture pensadas explícitamente para onboarding.

**Prioridad**: P1

## Lo que no debe resolverse metiendo lógica pharma en ASQL

Estas fricciones no justifican mover al motor:

- significado regulatorio de una firma,
- taxonomías de desviaciones o CAPA,
- políticas GMP/GAMP específicas,
- semántica de Qualified Person,
- workflows de aprobación o evidencia propios de una organización.

La mejora debe ir a:

- ergonomía general,
- documentación,
- validación,
- observabilidad,
- tooling,
- SDKs y patrones de integración.

## Conclusión

La muestra confirma que ASQL aporta un valor diferencial fuerte cuando una aplicación necesita:

- fronteras explícitas,
- snapshots reproducibles,
- referencias temporales estables,
- historia consultable,
- y una explicación determinista de cómo un estado llegó a existir.

La fricción principal no está en pharma manufacturing.
Está en el salto de modelo mental y en la ergonomía necesaria para que un equipo use rápido las primitivas del motor sin tener que inventar demasiada infraestructura de integración.

La prioridad debería ser reducir fricción de adopción sin debilitar las propiedades centrales de ASQL.

## Resumen ejecutivo

Si hubiera que agrupar este log en pocas líneas para priorización de producto, la lectura sería:

- **P0**: hacer más adoptables las fronteras explícitas, el esquema temporal y la compatibilidad pgwire real.
- **P1**: reducir el coste de modelar entidades, explicar historia, evolucionar schema temporal y construir capas de integración repetibles.
- **P2**: cerrar el bucle de observabilidad de adopción para que el runtime también ayude a mejorar el modelado.

La tesis general se mantiene:

- ASQL ya tiene primitivas potentes,
- el mayor trabajo pendiente está en ergonomía, diagnósticos, patrones y narrativa de adopción,
- y ese trabajo debe seguir siendo generalista, no sectorial.
