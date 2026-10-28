# Explicación para defender el TP

## Qué resuelve

«Simulamos la contratación de un plan. En lugar de que la API llame uno por uno a los demás servicios, publica un evento en Kafka. Facturación y aprovisionamiento trabajan en paralelo. Cuando ambos terminan bien se activa el plan; si alguno falla, la activación falla y se anula la cuenta de facturación.»

## Dónde mirar y qué explicar

| Componente / archivo | Explicación sencilla |
| --- | --- |
| ActivationController | Recibe POST y GET. HTTP 202 significa que aceptamos el pedido, todavía no que el plan esté activo. |
| ActivationService.create | Valida datos y guarda activación, historial y evento pendiente en una misma transacción. Devuelve PENDING. |
| ActivationService.receive | Bloquea la fila, registra el resultado, junta ambos pasos y emite un único resultado final. |
| aggregate en contracts | Es la regla de estados. Ambos OK dan ACTIVE; cualquier fallo da FAILED; los estados finales no se reabren. |
| BillingService | Crea una cuenta simulada. Si llega ActivationFailed cancela; recuerda el fallo si todavía no había cuenta. |
| ProvisioningService | Simula entre 1 y 3 segundos de trabajo. Publica éxito o un fallo de negocio solicitado desde la UI. |
| NotificationService | Envía email a Mailhog. Evita repetir con eventId, clave de negocio y Message-ID determinista. |
| CrmService | Escucha los cuatro topics y registra una copia independiente de cada evento. No modifica a los demás. |
| LoyaltyService | Se agrega después; lee el historial retenido con un nuevo grupo y asigna puntos simulados a los éxitos. |
| EventsGateway | El navegador se suscribe a un activationId. Envía snapshots actualizados por Socket.IO y recupera el historial al reconectar. |
| demo-ui/src/main.jsx | Formulario, consulta por ID, historial reciente local y timeline de eventos recibidos por socket. |
| Database.transaction | BEGIN, efecto, COMMIT; si hay error hace ROLLBACK. Libera la conexión siempre. |
| once | Marca un eventId dentro de la misma transacción del efecto. Si ya existe, no vuelve a ejecutar ese efecto. |
| outbox y relay | Guarda primero el evento pendiente junto al estado. Otro proceso lo publica y marca SENT. Si cae, lo reintenta. |
| KafkaToolkit.consume | Valida contrato/key, reintenta errores técnicos y confirma el offset después; usa DLQ si agota los intentos. |
| schema.ts | DDL: tablas, primary keys, foreign key, índices, estados válidos y restricciones únicas. |
| init-postgres.sh | Crea una base y un usuario por servicio; cada app entra solo a su base. |
| create-topics.sh | Crea cuatro topics y sus DLQ, con tres particiones y retención de siete días. |
| docker-compose.yml | Conecta infraestructura y aplicaciones; gestiona red, puertos, volúmenes, dependencias y healthchecks. |

## Preguntas probables

**¿Por qué Kafka?** Guarda eventos y permite grupos independientes. Billing y provisioning reciben el mismo pedido sin llamadas directas; un consumidor nuevo puede leer eventos anteriores retenidos.

**¿Qué es un topic y qué es una partición?** Un topic agrupa eventos de un dominio. Se divide en particiones para repartir trabajo. Kafka ordena mensajes dentro de cada partición de un topic.

**¿Por qué customerId como key?** Todos los eventos de ese cliente dentro de un topic van a la misma partición. No existe un orden total entre topics; por eso toleramos que el fallo llegue antes de crear la cuenta.

**¿Por qué grupos distintos para billing y provisioning?** Cada grupo recibe todos los eventos que le interesan. Si estuvieran en el mismo grupo repartirían los pedidos entre ambos, cuando necesitamos que ambos reaccionen a cada pedido.

**¿Qué pasa con dos instancias de billing?** Comparten billing-svc. Kafka reparte las particiones entre ellas. No necesitan otro puerto público y la base protege efectos duplicados.

**¿Qué es una saga?** Una operación repartida en pasos locales. Si falla, hacemos una compensación de negocio; no un rollback SQL entre bases diferentes.

**¿Qué significa idempotencia?** Recibir dos veces el mismo evento no crea dos cuentas, dos órdenes ni dos emails. La marca de eventId y el efecto SQL se confirman juntos.

**¿Por qué outbox?** Guardar y luego publicar por separado deja una ventana de pérdida si la app cae. Con outbox se guardan juntos el cambio y el evento pendiente. Puede publicarse más de una vez, por eso seguimos necesitando idempotencia.

**¿Qué es un offset?** La posición del mensaje en una partición. Confirmamos la posición siguiente después de procesar. Si el servicio cae antes, recibe el evento otra vez.

**¿Por qué hay un BigInt?** Kafka entrega offsets como strings que pueden superar la precisión segura de Number. BigInt permite sumar uno exactamente.

**¿Fallo de negocio o técnico?** Un fallo simulado de provisioning es un resultado válido y genera ProvisioningFailed. Un contrato inválido o una excepción técnica se reintenta; tras tres fallos va a DLQ.

**¿Qué es DLQ?** Un topic para mensajes que no se pudieron procesar. Conservamos el original y el motivo en headers; después continuamos con el siguiente mensaje.

**¿Por qué PostgreSQL?** Es la adaptación indicada por el profesor. Las entidades pasan a tablas, con claves únicas, índices y transacciones SQL. Cada servicio conserva su propia base y usuario.

**¿Por qué guardar un evento como JSONB si la base es relacional?** Las entidades y restricciones son relacionales; conservamos el sobre variable del evento en JSONB para no perder el payload ni su versión al guardar historial y outbox.

**¿Cómo reemplazaste el TTL?** Se borran marcas de processed_events de más de siete días con una consulta periódica indexada. Las cuentas y claves de negocio no se borran.

**¿Hay exactly-once global?** No. Kafka y PostgreSQL no comparten una transacción distribuida. La entrega es al menos una vez y deduplicamos efectos. Para Mailhog hacemos reconciliación por Message-ID; un proveedor SMTP real necesitaría otro contrato de idempotencia.

**¿Qué probaste realmente?** Compilación y tests de contratos, estados, retries, DLQ con contexto simulado y consultas PostgreSQL embebidas. El arranque de Docker, el broker real, el navegador y los cinco escenarios se validan en la computadora local, según validation.md.

## Resumen para decir en un minuto

«La UI crea una activación y recibe 202 con PENDING. La API guarda estado y un evento en outbox dentro de PostgreSQL. El relay publica ActivationRequested en Kafka con customerId como key. Billing y provisioning lo consumen en grupos distintos. La API junta sus resultados: activa si ambos están bien y falla si alguno está mal. Ante fallo, billing compensa incluso si todavía no había creado la cuenta. Notification envía el resultado por Mailhog y CRM guarda todos los eventos. Los consumidores son idempotentes, confirman offsets después de procesar y envían errores técnicos persistentes a DLQ. Docker Compose define toda la solución.»
