# TaskManager: revisión y propuesta de mejora

> Actualización: la implementación posterior y su estado de comprobación están en [TaskManager implementado](TASKMANAGER_IMPLEMENTACION.md). Este documento conserva el análisis previo.


Fecha: 2026-10-02. Destinatario: agente que evalúe e implemente una evolución del coordinador legacy del Harvester.

La mejora recomendada es un coordinador en memoria que garantice orden por contexto, admisión verificable, cancelación hasta la salida real del worker y despacho al completar tareas. Puede conservar los workers, acciones y lanes existentes. La revisión encontró problemas de corrección que conviene resolver antes de aumentar concurrencia o capacidad de cola.

Este documento contiene una propuesta; no modifica código ejecutable. La evidencia procede de inspección estática del checkout. Los escenarios de intercalado descritos son deducciones del código, no fallos reproducidos en una instalación. No se consultaron valores de base de datos, entorno de producción ni métricas, ni se ejecutaron pruebas.

## 1. Fuentes y alcance

Referencias del core relativas a `src/main/java/org/lareferencia/core/`, salvo indicación distinta. App = `../lareferencia-lrharvester-app`; dark = `../lareferencia-dark-lib`; entity = `../lareferencia-entity-lib`.

Revisiones inspeccionadas:

| Módulo | HEAD |
|---|---|
| core-lib | `c769020fbfc18e781a1b8e48b67b632166a9546c` |
| lrharvester-app | `452c96eebe190c00744b6071f27a8e97b37b2540` |
| dark-lib | `83c3ee6e86b5cf85e9f62ad71ad967bb03c22d91` |
| entity-lib | `f0c38785687dc8c6f54bd3af4372b5001d78d353` |

Se revisaron TaskManager, configuración del scheduler y del motor, executor y manager de acciones, aplicación de configuración de workers, cron, consumidores de resultados en la API general y dARK, contratos de worker, cancelación de workers base y harvesting, y la coordinación de Flowable como alternativa. El inventario inicial está en [`../../docs/ANALISIS_TASKMANAGER_ENCOLAMIENTO_CORE_LIB.md`](../../docs/ANALISIS_TASKMANAGER_ENCOLAMIENTO_CORE_LIB.md); esta revisión amplía y precisa sus conclusiones.

## 2. Comportamiento y configuración efectivos

Legacy se activa con `workflow.engine=legacy` o con la propiedad ausente (`task/TaskManagerConfig.java:40-43`; `task/NetworkActionExecutorConfig.java:65-76`). TaskManager acepta cada worker individualmente; no modela dependencias entre acciones.

| Control | Valor por defecto | Evidencia y alcance |
|---|---|---|
| `taskmanager.concurrent.tasks` | 4 | `task/TaskManager.java:114-118,296-298`: total registrado como activo, global por instancia de aplicación. |
| `taskmanager.max_queuded.tasks` | 32 | `task/TaskManager.java:117-118,305-307,379-386`: pendientes agregados globalmente; el typo forma parte de la clave efectiva. |
| `scheduler.pool.size` | 10 | `task/TaskManagerConfig.java:52-59`: scheduler compartido para workers y cron; la tarea periódica usa scheduling de Spring. |
| Exclusión por contexto | Un worker no terminado/no cancelado | `task/TaskManager.java:316-324,350-351`; el drenaje exige además lista de activos vacía (`451-452`). |
| Exclusión por lane | Un registrado por lane no negativo | `task/TaskManager.java:350-365,426-429`. Un lane negativo desactiva esta exclusión. Lane 0 funciona igual que cualquier otro ID no negativo: no bloquea lanes distintos. |
| Limpieza | 2 s fijos | `task/TaskManager.java:414-415`; `taskmanager.clean.interval` no interviene. |
| `workflow.max-queued-processes` | 32 | Flowable: `flowable/config/WorkflowProperties.java:42-55`, `flowable/WorkflowService.java:291-319`; no controla TaskManager. |

El archivo versionado del app `config/application.properties.d/05-harvester.properties:6-13` contiene 10/4/32. Son defaults/configuración del repositorio, no valores constatados en ejecución. Los límites son locales a una JVM; dos instancias del Harvester no comparten exclusión ni contadores de TaskManager.

La configuración de lanes tiene más de una fuente. `worker/BaseWorker.java:60-82` da `-1`; los XML pueden fijarlo; `task/WorkerConfigurationIntrospector.java:18,33-36,52-53` descubre propiedades escalares editables y no excluye `serialLaneId`. `task/ApplicationWorkerConfigurationService.java:35-60,76-101` guarda defaults/configuración, y `task/WorkerConfigurationApplier.java:16-47` aplica los valores de BD a las nuevas instancias en ejecución normal. Por ello el lane efectivo puede prevalecer sobre el XML. El lanzador manual dARK crea el bean y no llama a ese applier (`app/.../api/v5/DarkManualCommandLauncher.java:38-46`), introduciendo una diferencia entre rutas.

Los XML existentes no son todos activos: `app/config/beans/actions.xml:38-54` importa harvesting, dark, cleaning, validation, frontend, semantic y xoai; network, historic, project y otros están comentados. El catálogo persistido ordena acciones (`task/ApplicationActionCatalogService.java:178-186`), pero el orden del catálogo no está protegido por el algoritmo de despacho.

Los archivos `.properties` se añaden con `addLast` en orden alfabético (`util/PropertiesDirectoryListener.java:75-85`). Para una clave duplicada entre estas fuentes, la primera fuente tiene precedencia; no debe suponerse que un archivo `99-...` siempre sobreescribe a `05-...`. La verificación de límites efectivos debe consultar el Environment y la fuente ganadora. La ruta se obtiene de `app.config.dir`, default `config` (`util/ConfigPathResolver.java:49-75`).

## 3. Hallazgos y consecuencias

### F1. El despacho puede invertir el orden por red — prioridad alta

`task/TaskManager.java:449-457` hace `poll()` antes de saber si puede iniciar el worker. Si falta capacidad global o el lane está ocupado, `launchWorkerWithResult` lo agrega al final (`379-380`).

Ejemplo deducido: una red tiene `[cosecha, validación, indexación]` y las cuatro plazas globales están ocupadas por otras redes. Una pasada de limpieza transforma la cola en `[validación, indexación, cosecha]`. Si queda una plaza en la siguiente pasada, validación puede arrancar antes de cosecha. Si el bloqueo persiste, el orden sigue rotando. Una etapa puede consumir un snapshot anterior o encontrar estados que no esperaba.

Además, una solicitud nueva puede adelantarse a pendientes anteriores: `launchWorkerWithResult` no comprueba si la cola de su contexto ya contiene workers. También permite iniciar si el future anterior terminó, mientras el drenaje espera a la siguiente limpieza para quitarlo de `runningWorkers`. No existe FIFO estricto por red ni prioridad por antigüedad global.

### F2. La cancelación libera exclusión antes de demostrar salida — prioridad alta

`worker/BaseWorker.java:94-99` llama a `future.cancel(true)`. `task/TaskManager.java:419-435` elimina el registro al ver el future cancelado, y `316-324` deja de considerar ocupado ese contexto. La cancelación solicita interrupción; no demuestra que `worker.run()` y su cleanup hayan acabado. La exclusión del lane/contexto puede liberarse mientras el worker aún accede al almacenamiento.

La posibilidad de parada tardía está respaldada por los workers: `worker/BaseIteratorWorker.java:80-100` revisa `wasStopped` sólo al completar una página y ejecuta `postRun()` después; `worker/BaseBatchWorker.java:83,120-181,198-200` y `worker/solr/BaseBatchSolrWorker.java:77,133-175,182-190` usan flags sin `volatile`. `worker/harvesting/OCLCBasedHarvesterImpl.java:88-99,192-196` tiene otro flag sin `volatile` y descarta `InterruptedException`. Son debilidades de cooperación/visibilidad; no prueban que toda cancelación falle.

Hay una carrera adicional durante preparación: `HarvestingWorker.stop()` señala al harvester (`170-175`), pero `run()` llama más tarde `harvester.reset()` (`327-329`), que borra esa señal. El coordinador necesita un token de cancelación válido durante toda la ejecución.

### F3. Se informa aceptación aunque haya rechazos o preparación incompleta — prioridad alta

`task/LegacyNetworkActionExecutor.java:139-157,170-185` prepara y somete un worker cada vez, descarta el resultado vía `launchWorker()` y captura excepciones de creación/configuración sin propagarlas. `task/TaskManager.java:410-412` descarta `WorkerLaunchResult`. Una acción con varios pasos puede quedar parcialmente admitida.

La API general devuelve `ACCEPTED` si no recibió una excepción (`app/.../api/v5/ApiV5ManagementService.java:539-554`). El controller legacy también devuelve `DONE` tras ejecutar acciones (`app/.../controllers/ActionsController.java:286-310`). En cambio, la ruta manual dARK sí consume el resultado batch y traduce rechazo a `503 DARK_COMMAND_REJECTED` (`DarkManualCommandLauncher.java:46-49`; `ApiV5DarkService.java:79-87`). La semántica difiere según la entrada.

`launchWorkersWithResult` evita aceptación parcial por capacidad usando una reserva conservadora (`TaskManager.java:397-404`), pero puede rechazar incluso cuando todos los workers caben en plazas de ejecución libres: siempre cuenta todo el grupo contra pendientes. Tampoco revierte un grupo si ocurre una excepción después de someter el primer worker; la atomicidad es de comprobación de capacidad, no una transacción completa de preparación/despacho.

### F4. Las lecturas pueden competir con la creación de colas — prioridad alta

`QueueMap.getQueue()` hace `get`, crea y `put` separados (`task/TaskManager.java:70-76`). Las lecturas de monitorización también lo llaman y no están sincronizadas (`178-205,267-268`), por lo que una lectura crea estado mutable.

Intercalado posible para un contexto nuevo: una consulta ve `null`; admisión crea y publica una cola y encola un worker; la consulta publica después su propia cola vacía sobre la anterior. El worker admitido deja de estar en el mapa. El uso de `ConcurrentHashMap` no convierte esa secuencia en atómica. Solución mínima: creación atómica para escritores y consultas sin creación; el diseño propuesto usa un único propietario de estado y snapshots para lectura.

### F5. Cron reutiliza una instancia mutable y puede acumular disparos

`task/TaskManager.java:518-525` conserva un worker y lanza la misma instancia en cada disparo. En la ruta habitual es un `AllActionsWorker` (`task/LegacyNetworkActionExecutor.java:203-223,314-333`). Si el procesamiento demora más que el periodo, se pueden encolar varias referencias al mismo objeto, compartiendo `scheduledFuture` y contexto. No hay política de duplicados ni de disparos perdidos.

Ese `AllActionsWorker` ocupa una plaza global y la red sólo para expandir acciones. Al someter pasos desde dentro de su `run()`, normalmente los mismos pasos quedan bloqueados por el launcher que los está generando. Consume capacidad temporal y depende de la limpieza para habilitarlos; su expansión tampoco reserva el plan completo.

`clearScheduleByRunningContextID()` detiene el worker además de cancelar el cron (`task/TaskManager.java:492-497`); reprogramar puede por ello interrumpir un launcher vigente. La limpieza del mapa de programaciones está dentro del bucle de cancelación: conviene tomar snapshot, cancelar cada handle y vaciar después para conservar todas las referencias. No se afirma que el iterador actual pierda siempre elementos.

### F6. Cancelación y retorno de `run()` no equivalen a resultado de negocio

TaskManager sólo retira futures; no distingue éxito, error, cancelación o resultado parcial, ni conserva causa (`task/TaskManager.java:419-435`). Algunos workers capturan errores y llaman a su propio `stop()` o marcan snapshot fallido sin lanzar la excepción al scheduler (`BaseBatchWorker.java:161-175`; `HarvestingWorker.java:319-322,331-358`). Un wrapper debe registrar excepciones y salida real, pero para declarar éxito funcional también necesita un resultado del worker o del snapshot.

Actualmente la siguiente etapa se intenta independientemente del resultado de la anterior. Cambiar a “parar toda la cadena ante cualquier error” requiere definir dependencias y política de cada acción; serialización sola no establece esa semántica.

### F7. ID de contexto, red y comando están mezclados

`NetworkRunningContext.getId()` devuelve `NETWORK::<id>` (`worker/NetworkRunningContext.java:48-49,88-91`), pero el contexto manual dARK devuelve `DARK_MANUAL::<commandId>` (`dark/src/main/java/org/lareferencia/contrib/dark/worker/DarkManualRunningContext.java:46-49`). Un comando manual puede tener varios workers procedentes de varias redes bajo un solo contexto (`DarkManualCommandLauncher.java:38-44`).

La garantía es por ID de contexto: no cubre automáticamente todos los workers que tocan una misma red. Dos comandos o un comando y una acción automática pueden referirse a la misma red con IDs distintos; los lanes pueden restringirlos parcialmente. `killAndUnqueueActions(network)` se dirige a `NETWORK::<id>` (`LegacyNetworkActionExecutor.java:196-200`), por lo que no abarca automáticamente comandos dARK. Mantener explícita esa semántica al migrar; no ampliar cancelación por red sin decidir qué ejecuciones debe incluir.

### F8. El control global es costoso y no protege entre instancias

`totalSize()` recorre todas las colas (`TaskManager.java:96-100`); las colas vacías siguen en los mapas después de clear/remove. Consultas para contextos inexistentes también generan colas. Puede crecer el coste de cada comprobación con la cantidad histórica de contextos.

`killAllTaskByRunningContextID` llama `worker.stop()` bajo el monitor global (`277-285`). El stop de Solr hace rollback remoto (`BaseBatchSolrWorker.java:182-190`): una parada lenta puede bloquear admisión, limpieza y despacho de otras redes. El estado interno debería actualizarse en una sección breve y ejecutar I/O fuera de ella.

TaskManager no tiene protocolo propio de shutdown; sólo el scheduler está configurado para esperar tareas (`TaskManagerConfig.java:57`). Quedan sin contrato explícito el rechazo de nuevas solicitudes, qué pasa con pendientes y cuánto tiempo se espera a workers que no responden. Todas sus colas/lanes viven en memoria local.

### F9. Aumentar activos no limita el consumo total de recursos

Un worker puede abrir pools propios. Por ejemplo el indexador Elastic usa `availableProcessors()`, un pool fijo y un semáforo (`entity/src/main/java/org/lareferencia/core/entity/indexing/elastic/JSONElasticEntityIndexerThreadedImpl.java:136,154,215-229`). El límite de TaskManager regula wrappers de workers, no todos los hilos, conexiones HTTP o escrituras internas. Para elevar 4/32 hace falta medir presión real y revisar qué recursos representan los lanes.

## 4. Diseño recomendado

### 4.1. Un registro de ejecución independiente del worker

Introducir dentro de `org.lareferencia.core.task` un registro con ID único de ejecución, ID de grupo/plan opcional, secuencia de admisión, contexto de serialización, datos de red/comando, worker/fábrica, requisitos de lane, timestamps, estado, causa de espera y resultado. El future pertenece a esa ejecución; no es la identidad ni la fuente única de verdad de estado.

Estados mínimos: `QUEUED`, `DISPATCHED`, `RUNNING`, `CANCEL_REQUESTED`, `COMPLETED`, `FAILED`, `CANCELLED`. `DISPATCHED` ya reserva capacidad y exclusiones aunque aún no haya entrado en `run()`. `CANCEL_REQUESTED` conserva dichas reservas hasta la salida confirmada. `COMPLETED` significa retorno del código; el resultado funcional puede ser `SUCCESS`, `PARTIAL`, `FAILURE` o `UNKNOWN` hasta disponer de un contrato de worker.

Separar identidad de ejecución de exclusión: conservar inicialmente el contexto actual como clave de serialización y el lane actual como recurso de capacidad 1. Permitir después declarar recursos adicionales, como una red o destino de escritura, donde la revisión de workers lo justifique. Un comando multirred no debe bloquear redes completas durante toda su vida por accidente: los requisitos se declaran por paso. Si una tarea requiere varios recursos, comprobar y reservar todos juntos; nunca conservar una reserva parcial mientras espera otra.

### 4.2. Despacho justo y ordenado

Mantener una deque por contexto y un conjunto/cola de contextos candidatos. Sólo inspeccionar la cabeza con `peek`; retirar el elemento cuando capacidad y recursos estén reservados. Si está bloqueado, conservarlo en posición y visitar otro contexto. Para los candidatos usar turno circular, evitando depender del orden de `ConcurrentHashMap`.

Toda solicitud nueva entra por el mismo camino de admisión y despacho; no puede adelantarse a una cola previa del mismo contexto. Un contexto vuelve a competir al finalizar su worker. Si sólo ese contexto está habilitado, puede ocupar la siguiente plaza inmediatamente. No dejar plazas sin usar sólo porque otro contexto está bloqueado por lane.

Admisión, reserva y transición de estado necesitan una autoridad única: un lock breve sobre estructuras ordinarias es suficiente; no es necesario crear un thread coordinador ni introducir otra cola de solicitudes. Las llamadas a worker, BD, HTTP y listeners se realizan fuera de ese lock. Las consultas obtienen snapshots inmutables que no crean colas.

### 4.3. Separar cron de ejecución pesada

Usar un scheduler pequeño para cron y reconciliación, y un executor dedicado para workers. El callback cron sólo construye/admite un plan; no lo representar como worker `AllActions` que ocupa la misma plaza/red que sus pasos.

El executor tendrá máximo de hilos alineado con `maxConcurrentWorkers` y un buffer interno acotado de transferencia. Todas las tareas transferidas cuentan como `DISPATCHED`; no puede existir una segunda cola ilimitada de trabajos aceptados fuera del coordinador. Un rechazo del executor revierte reservas y conserva la tarea admitida en cola, o la termina explícitamente por shutdown. No usar `CallerRunsPolicy`: podría ejecutar un worker en el hilo HTTP/cron/coordinador y romper la separación de capacidad.

Un wrapper de ejecución notifica finalización en `finally` después de que el cuerpo haya salido; esa transición libera recursos y solicita nuevo despacho. El sondeo queda como reconciliación auxiliar configurable, sin ser la vía normal de progreso. Las señales de finalización no deben perderse si un listener falla.

El contrato actual `IWorker` requiere `ScheduledFuture<?>` (`worker/IWorker.java:48-59`), mientras un executor ordinario entrega un `Future<?>`. La implementación necesita un adapter de compatibilidad o migrar explícitamente ese contrato y sus consumidores; no basta con intercambiar los beans. El registro nuevo debe seguir siendo la autoridad de estado incluso si el adapter expone cancelación anticipada. Cuando un worker genera tareas internas, su contrato de finalización debe incluir espera de sus escrituras/flush pendientes: salir del wrapper sólo es suficiente si el worker ya se hizo cargo de esos hijos.

### 4.4. Admitir un plan completo, ejecutar pasos en orden

Antes de admitir una acción o el conjunto de acciones programadas, resolver catálogo, orden, configuración y todos los pasos. Si la preparación falla, no someter los primeros pasos. Congelar el orden y valores efectivos para esa admisión.

Mantener inicialmente los límites expresados en workers para comparar 4/32 con el comportamiento anterior. Bajo el lock, simular las reservas inmediatas válidas y contar sólo los pasos que quedarán esperando; aceptar el grupo entero si cabe o rechazarlo entero. Publicar toda la admisión antes de despachar. Esto corrige el falso rechazo del batch actual con plazas libres sin prometer atomicidad de efectos de negocio una vez comenzó `run()`.

Contar todos los pasos futuros contra la capacidad pendiente. No almacenar planes ilimitados escondidos detrás de una única entrada de cola. Si un plan supera la capacidad total, devolver un motivo específico en lugar de aceptación parcial.

La cola conserva los pasos del plan en su orden por contexto. Una política explícita puede parar pasos dependientes tras fallo confirmado, o continuar pasos independientes. Para una primera corrección, preservar la política funcional existente y registrar resultados; cambiar dependencias después de revisar los contratos de snapshots, validación e indexación. Las acciones funcionales que notifican un fallo y retornan requieren adaptar el contrato de resultado para que el coordinador lo conozca.

### 4.5. Cancelación con confirmación real

Cancelar un pendiente lo retira y devuelve su capacidad. Cancelar un `DISPATCHED` requiere resolver atómicamente si empezó: si se impide de forma demostrable la entrada al cuerpo, liberar sus reservas; si empezó, pasar a `CANCEL_REQUESTED` y esperar la salida del wrapper. Un `finally` aislado no basta cuando `FutureTask.cancel()` impide ejecutar el wrapper antes de empezar.

Para un worker activo, señalar un token visible (`AtomicBoolean`/`volatile`), solicitar interrupción y efectuar su stop fuera del lock global. Mantener recursos hasta confirmar salida. Uniformar chequeos durante preparación, iteración, espera de retries y operaciones bloqueantes; cerrar recursos en el hilo ejecutor y en `finally`. El token no se reinicia al resetear componentes internos.

Si el worker no responde en el tiempo esperado, mostrar `CANCEL_REQUESTED` prolongado y la causa. Un timeout no autoriza a liberar la red/lane mientras sigue escribiendo. La salida segura puede requerir intervención operativa o reinicio del proceso; Java no ofrece terminación segura arbitraria del código.

### 4.6. Cron y duplicados con semántica definida

Guardar una definición/fábrica y crear una ejecución nueva por disparo admitido. Asociar la programación a una clave única y hacer alta/reemplazo idempotente por esa clave; reprogramar cancela futuros disparos y no cancela por defecto una ejecución ya iniciada.

Propuesta inicial: si un plan cron de la misma programación sigue admitido, omitir el nuevo disparo y registrar `SKIPPED_ALREADY_PENDING`. Evita acumular cosechas atrasadas. La opción de conservar un único disparo pendiente consolidado puede añadirse si se necesita ejecutar inmediatamente al terminar; constituye una política diferente y debe declararse. Los comandos manuales siguen devolviendo aceptación/rechazo explícitos y no se deduplican silenciosamente.

### 4.7. Resultado de admisión y observabilidad

Devolver un recibo con ID, estado de admisión, contexto, tamaño admitido y motivo: capacidad global, límite por contexto, configuración inválida, shutdown o duplicado programado. `ACCEPTED` confirma que todo el grupo quedó admitido; no significa finalización. Las APIs v5/legacy y dARK deben consumir ese resultado de forma coherente.

Publicar counters de pendientes, despachados, corriendo, cancelación en curso, rechazo y cron omitido; duración de espera y ejecución; y causa de bloqueo por tarea. El estado por worker y snapshots se conserva como información de progreso. La retención de resultados finales en memoria debe tener tope y TTL; retirar contextos vacíos y referencias a workers terminados. La monitorización consulta metadata del coordinador sin cambiar colas ni llamar a I/O de workers dentro del lock.

## 5. Configuración y compatibilidad

Recomiendo conservar durante la transición `taskmanager.concurrent.tasks` y el alias histórico `taskmanager.max_queuded.tasks`. Una clave canónica `taskmanager.max-queued-tasks` puede añadirse; si ambas se definen y discrepan, fallar el arranque con un mensaje claro. Mantener `workflow.max-queued-processes` dentro del motor Flowable hasta decidir una configuración común con unidades idénticas.

| Configuración propuesta | Criterio inicial |
|---|---|
| Máximo de activos/reservados | 4, conservar default; executor alineado. |
| Máximo de pendientes globales | 32, conservar default y alias. |
| Pendientes por contexto | Opcional, desactivado inicialmente; definir tras medir tamaño máximo de planes normales para no bloquear una cadena habitual. |
| Capacidad por lane | 1 para IDs existentes; admitir capacidades mayores sólo al confirmar seguridad del recurso. |
| Intervalo de reconciliación | Propiedad realmente consumida, complementaria a notificación de finalización. |
| Política cron | Omitir disparo duplicado de la misma programación con un plan admitido. |
| Shutdown | Cerrar admisión y cron; marcar/remover pendientes; solicitar parada y esperar un plazo configurado para activos. |
| Retención de finalizados | TTL y número máximo de registros en memoria. |

Validar al arranque activos > 0, pendientes >= 0, tamaños de pool y periodos válidos. Que pendientes sea 0 debe permitir ejecución inmediata si hay plazas libres, también en batch. Cambios por entorno/archivo se aplican al arranque; un cambio dinámico de límites requiere una política explícita para reservas ya admitidas.

## 6. Alternativas

| Alternativa | Qué resuelve y qué exige |
|---|---|
| Ajustar 4/32 y pool | Cambia capacidad; no corrige orden, cancelación temprana, rechazo oculto ni creación concurrente de colas. Necesita métricas para dimensionar. |
| Correcciones puntuales de legacy | Puede arreglar `peek`, creación de colas, resultados y cancelación con menor alcance inicial. Conviene como primera etapa, pero requiere registros independientes para resolver correctamente la vida de las ejecuciones. |
| Coordinador legacy propuesto | Recomendado para este problema: conserva workers/acciones y añade garantías claras con estructuras en memoria. Implica adaptar contratos del core y consumidores del app. |
| Migrar a Flowable | Opción si se requieren BPMN, recuperación e historia de procesos. No es un cambio de configuración equivalente: su cola de procesos esperando en `WorkflowService.queuedByLane` es en memoria (`110-119,343-350`) y se limpia en shutdown (`227-228`). Además su shutdown elimina procesos activos (`258-275`). Revisar persistencia real, parada y todos los adapters antes de prometer recuperación. |
| Cola durable y coordinación compartida | Tiene sentido para reinicios sin pérdida o múltiples instancias coordinadas. Exige contratos persistentes, exclusión entre instancias, resultados y política de reintentos/idempotencia; es un objetivo adicional a la mejora local. |

Con los requisitos conocidos, la opción recomendada es evolucionar legacy. El diseño continúa siendo local y en memoria: recuperación de pendientes tras caída y coordinación entre JVM requieren otra fase si se solicitan.

## 7. Secuencia de implementación sugerida

1. Añadir escenarios deterministas de regresión para F1–F4, conservar los defaults y exponer configuración efectiva. Corregir lecturas que crean colas y el adelantamiento/rotación por contexto.
2. Introducir registro de ejecución, exclusiones reservadas y confirmación de salida, con cancelación antes/durante ejecución y reversión ante rechazo del executor. Alinear la cooperación de workers base y harvesting.
3. Separar scheduler/executor y activar despacho por finalización, incluyendo reconciliación y shutdown. Mantener el contrato de consultas legacy mediante adapters.
4. Preparar/admitir planes completos y propagar resultados a executor, manager, API general y dARK. Retirar el launcher `AllActions` del pool de workers y crear workers nuevos por disparo.
5. Añadir turnos entre contextos, política cron, métricas y límites opcionales medidos. Adoptar resultado funcional/dependencias por etapa tras decidir su semántica.

Cambios principales en core: `TaskManager`, `TaskManagerConfig`, `LegacyNetworkActionExecutor`, `INetworkActionExecutor`, `NetworkActionkManager`, contratos/base de worker y aplicación de configuración. Integraciones indispensables: recibos de `ApiV5ManagementService`, controller legacy y `DarkManualCommandLauncher`/registry; estos consumidores no están en core-lib. El adapter Flowable debe conservar su selección condicional y semántica, aunque un recibo común puede requerir adaptar su interfaz.

Para congelar valores y crear workers al comenzar, se debe decidir si las factories usan la configuración capturada al admitir o la vigente al iniciar. Recomiendo capturar valores al admitir y materializar la instancia al ejecutar; para compatibilidad inicial pueden prepararse instancias antes de admisión, siempre sin iniciar efectos de negocio. Las factories no se ejecutan bajo el lock global.

## 8. Criterios de aceptación para el agente implementador

Estos escenarios se proponen para la futura implementación; no se ejecutaron en esta revisión. Usar latches y reloj controlado, evitando pruebas basadas sólo en sleeps.

- Saturar los activos y comprobar que `[cosecha, validación, indexación]` conserva orden tras cualquier cantidad de reconciliaciones; una solicitud nueva tampoco adelanta la cola previa.
- Bloquear un lane y comprobar que las tareas de otros contextos elegibles usan plazas libres; dos tareas del mismo contexto o lane no se solapan.
- Cancelar un worker que sigue vivo hasta recibir una señal de prueba: ninguna tarea incompatible empieza hasta la salida real. Cubrir también cancelación entre preparación y ejecución, y antes de entrada al wrapper.
- Consultar un contexto inexistente mientras se admite su primera tarea: no perder registros ni crear estado desde la lectura.
- Admitir batches con capacidad exacta, pendientes 0 y plazas libres; rechazar un grupo sobredimensionado antes de ejecutar cualquier paso. Inyectar fallo de preparación y rechazo del executor sin dejar reservas huérfanas.
- Disparar varias veces el mismo cron bajo carga: aplicar la política declarada, crear instancias independientes, reemplazar programación sin detener la ejecución admitida y cancelar todos sus handles al eliminarla.
- Verificar admisión frente a resultado funcional: API general y dARK informan rechazo real; retorno normal con snapshot fallido no se etiqueta automáticamente como éxito de negocio.
- Verificar lanes efectivos desde XML/BD, diferencias con comandos manuales y exclusiones declaradas por red/recurso; CANCEL_ALL conserva el alcance decidido.
- Cerrar el contexto Spring durante carga: no aceptar después del cierre, resolver pendientes, conservar reservas de workers aún vivos y respetar el plazo operativo de shutdown.
- Verificar counters/snapshots bajo cambios simultáneos y liberar contextos/registros finalizados según TTL/tope; el consumo no debe crecer por consultas a IDs inexistentes.

No se localizaron referencias a `TaskManager` o `WorkerLaunchResult` en los tests Java inspeccionados de core-lib y lrharvester-app. Sí hay tests de configuración de workers (`src/test/java/org/lareferencia/core/task/WorkerConfigurationApplierTest.java`); no sustituyen regresiones del coordinador. Antes de aumentar capacidad, medir espera, ejecución, rechazos, presión de BD/SQLite/Solr/Elastic y hilos internos con una carga representativa.
