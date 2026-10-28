# Arquitectura y decisiones

## Comunicación y datos

activation-api es el único punto REST. Demo UI habla con ella por HTTP y Socket.IO. El gateway entrega snapshots por WebSocket al cambiar el historial; consulta la base cada 500 ms. Al reconectar, la UI vuelve a suscribirse y recibe un snapshot completo. Esto evita perder eventos en la carrera entre POST y la suscripción o en una caída entre commit y emisión. La entrega a la UI puede agrupar varios eventos en un snapshot; cada uno aparece individualmente en la timeline.

Los cinco servicios de negocio solo se comunican por Kafka. Mailhog representa un sistema externo simulado: SMTP para envío y API HTTP para reconciliar envíos tras una caída. No es una llamada entre microservicios de negocio.

| Servicio | Base / rol PostgreSQL | Tablas de dominio |
| --- | --- | --- |
| activation-api | activation | activations, activation_history |
| billing-service | billing | billing_accounts, compensation_requests |
| provisioning-service | provisioning | provisioning_orders |
| notification-service | notification | notifications |
| crm-analytics-service | crm | event_log |
| loyalty-service | loyalty | loyalty_activations |

Cada base tiene processed_events y outbox (infraestructura común; en consumidores puros outbox queda vacío). La configuración revoca CONNECT de PUBLIC y concede el acceso al propietario. Ninguna aplicación usa la cuenta administradora. Se comparten instancia y volumen, no tablas ni credenciales de rol.

La identidad, estados y relaciones se guardan en columnas relacionales, con PRIMARY KEY, UNIQUE, CHECK y foreign key de historial a activaciones. El sobre de evento se guarda en JSONB porque su payload depende de su tipo y debe conservarse para replay. JSONB no reemplaza las tablas de dominio.

## Topics, grupos y contratos

| Topic | Eventos | Grupos consumidores |
| --- | --- | --- |
| activation.requested | ActivationRequested | billing-svc, provisioning-svc, crm-analytics |
| billing.events | BillingAccountCreated, BillingFailed, BillingAccountCancelled | activation-api, crm-analytics |
| provisioning.events | ProvisioningCompleted, ProvisioningFailed | activation-api, crm-analytics |
| activation.events | ActivationCompleted, ActivationFailed | notification-svc, billing-svc, crm-analytics; loyalty-svc-v1 con perfil |

Cada topic tiene su `<topic>.dlq`, 3 particiones, factor de replicación 1 y retención configurada de 604800000 ms (7 días). La clave de todos los eventos de negocio es customerId. Los DLQ conservan la clave original (un mensaje malformado podría tener una clave incorrecta o vacía).

El sobre contiene eventId UUID, eventType, version=1, occurredAt ISO UTC, correlationId=activationId, customerId, source y payload. `libs/contracts/index.ts` define y valida el contrato en runtime. Contratos incompatibles requieren otra versión; los consumidores actuales rechazan versiones desconocidas y las envían a DLQ. No se usa Schema Registry.

Se utiliza ClientKafka / @EventPattern de Nest sobre KafkaJS. `postfixId: ''` conserva exactamente los nombres de grupo de la consigna. `fromBeginning: true` permite a grupos nuevos leer desde el primer offset retenido; un grupo existente continúa donde confirmó. El parser preserva value binario hasta el toolkit para que el DLQ conserve el mensaje original.

## Estado, concurrencia y compensación

PENDING → IN_PROGRESS al recibir el primer resultado; ambos OK → ACTIVE, primer fallo → FAILED. Un fallo como primer resultado termina directamente en FAILED. ACTIVE y FAILED son terminales. BillingAccountCancelled y cualquier resultado posterior se registran en historial, sin reabrir el estado.

activation-api bloquea la fila con SELECT FOR UPDATE durante la agregación, evitando dos cierres de saga al llegar resultados simultáneos. El evento terminal se inserta en outbox dentro de esa misma transacción.

Billing serializa por activationId con advisory lock transaccional. ActivationFailed guarda una intención de compensación permanente. Si ya había cuenta, la cancela; si todavía no había cuenta, el alta posterior se crea CANCELLED y publica BillingAccountCreated seguido de BillingAccountCancelled. Este caso evita una cuenta activa huérfana al cruzarse eventos de topics diferentes.

El orden de Kafka está garantizado **por partición dentro de un topic**, no entre topics. Billing y provisioning tienen consumer groups diferentes y corren en paralelo. Cada grupo puede procesar tres particiones en paralelo manteniendo secuencia dentro de cada una. El agregador y la compensación toleran los órdenes de llegada entre topics.

## Idempotencia, outbox y offsets

1. El consumidor comienza una transacción SQL e inserta eventId en processed_events con ON CONFLICT DO NOTHING.
2. Solo si insertó aplica el efecto de dominio y el outbox. Un rollback también revierte la marca de procesado.
3. Después de completar el efecto se confirma manualmente offset+1, usando BigInt para no perder precisión.
4. Si cae entre commit SQL y commit Kafka, la reentrega encuentra eventId y no repite el efecto. También hay claves de negocio únicas para cuentas, órdenes, notificaciones y log.

El relay corre cada 500 ms. Toma un advisory lock por base para que las réplicas no publiquen concurrentemente la misma cola. Publica secuencialmente, con productor idempotente, y marca SENT. La inserción de outbox también se serializa para asignar secuencias coherentes con commits concurrentes. No se usa SKIP LOCKED porque varios publicadores podrían invertir eventos del mismo cliente.

Una caída después de publicar y antes del commit de SENT puede volver a publicar con el mismo eventId. La entrega es **al menos una vez**, con efectos de negocio idempotentes; no se promete una transacción distribuida PostgreSQL-Kafka ni exactly-once global.

## Reintentos y DLQ

Un error técnico tiene **tres intentos totales**. Cada fallo espera 1 s, 2 s y 4 s respectivamente; tras el tercero se publica el value original en `<topic>.dlq`, con error, grupo, topic, partición, offset y attempts=3 en headers. Se confirma el offset original solamente después de que Kafka acepte el DLQ. Si publicar DLQ o confirmar offset falla, KafkaRetriableException permite reentrega y no se confirma anticipadamente. Se envían heartbeats durante esperas para evitar perder la membresía del grupo.

Un fallo de negocio simulado no lanza error técnico: se guarda como resultado FAILED y se publica BillingFailed o ProvisioningFailed. No pasa por DLQ.

Un servicio detenido conserva su offset; al volver procesa lo retenido por Kafka. La recuperación exige conservar volúmenes y reiniciar antes de que venza la retención. No hay garantía de recuperación ilimitada.

## Limpieza y notificaciones

El relay elimina processed_events con processed_at anterior a 7 días mediante DELETE indexado. Es el equivalente al TTL solicitado; se ejecuta cada ciclo mientras Kafka y PostgreSQL permiten trabajar. No necesita cron instalado en el host. Las intenciones de compensación y las claves únicas de negocio no expiran.

La retención de Kafka elimina segmentos, no mensajes exactamente a los 7 días. Se configura segment.ms de una hora; no se debe interpretar el TTL como garantía absoluta contra replay muy antiguo. Las restricciones de negocio y event_log/history conservan sus identidades para limitar efectos repetidos incluso fuera de esa ventana.

Notifications tiene UNIQUE(activation_id,event_type), event_id único y Message-ID determinista. Durante la transacción mantiene un advisory lock para serializar réplicas. Antes de enviar consulta los mensajes de Mailhog: si SMTP aceptó y el proceso cayó antes del commit, el siguiente intento detecta ese Message-ID y registra el envío sin reenviar. Esto es una reconciliación específica de la POC, probada con un doble; debe verificarse contra Mailhog real. Requiere conservar la bandeja de Mailhog durante la recuperación. La imagen Mailhog usa memoria: su reinicio pierde los emails simulados. No hay promesa exactly-once para un proveedor SMTP real, cuyo envío no participa de la transacción SQL.

## Variables

| Variable | Uso |
| --- | --- |
| DATABASE_URL | Conexión PostgreSQL de la base propia |
| KAFKA_BROKERS | kafka:29092 dentro de Compose; localhost:9092 desde host |
| KAFKA_CLIENT_ID | Identidad del cliente; por defecto nombre del servicio |
| KAFKA_GROUP_ID | Consumer group; Compose fija los de la consigna |
| SERVICE_NAME | Aplicación que inicia Dockerfile.backend |
| SMTP_HOST, SMTP_PORT | Mailhog SMTP (mailhog:1025) |
| MAILHOG_API_URL | Mailhog HTTP para reconciliación (mailhog:8025) |
| CORS_ORIGIN | Origen del frontend permitido por API y gateway |
| PROVISIONING_DELAY_MS | Override para pruebas; por defecto aleatorio 1000–3000 ms |
| CRM_GROUP_ID, LOYALTY_GROUP_ID | Overrides Compose para replay con grupos nuevos |

Solo API, UI, Kafka, Kafka UI y Mailhog exponen puertos al host; se enlazan a 127.0.0.1. Las apps internas pueden escalar sin conflictos de puertos o container_name. El proyecto es una POC local sin autenticación ni TLS.

## Referencias técnicas

- Consigna adjunta: POC Activación de Servicios con Kafka, secciones 01–11 y sus diagramas.
- [Transporte Kafka de NestJS](https://docs.nestjs.com/microservices/kafka): eventos, parser binario, offsets manuales y postfixId.
- [Docker de Apache Kafka](https://kafka.apache.org/39/getting-started/docker/).
- [Imagen oficial Kafka 3.9.1](https://github.com/apache/kafka/blob/3.9.1/docker/jvm/Dockerfile).
- [Transacciones PostgreSQL](https://www.postgresql.org/docs/16/tutorial-transactions.html).
