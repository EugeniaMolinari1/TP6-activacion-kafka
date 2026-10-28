# Guion de los cinco escenarios

Objetivo de exposición: aproximadamente 15 minutos una vez descargadas las imágenes. Este tiempo es una meta de la consigna, no una medición realizada en el entorno remoto.

Preparación: `docker compose up --build -d`, `docker compose ps -a`. Abrir UI http://localhost:5173 a la izquierda y Kafka UI http://localhost:8080 a la derecha. Mailhog http://localhost:8025 en otra pestaña. Usar un customerId diferente por escenario para identificar evidencias. Esperar que los cinco grupos principales aparezcan en Kafka UI; el de notificación debe haber iniciado al menos una vez antes del escenario 3.

## 1. Camino feliz · 2 minutos

- Crear cliente DEMO-01, Plan Digital, fallo Ninguno.
- Mostrar respuesta POST 202/PENDING en DevTools → Network o PowerShell del README.
- Mostrar timeline: ActivationRequested, BillingAccountCreated y ProvisioningCompleted (estos dos pueden intercambiarse), ActivationCompleted; estado ACTIVE.
- En Mailhog ver un email de bienvenida. Comparar su activationId con el de la UI.
- En Kafka UI mostrar que el mismo evento requested fue consumido por billing-svc y provisioning-svc en grupos independientes.

Explicación: un evento dispara varias reacciones (fan-out); la API no espera los resultados para aceptar el pedido.

## 2. Fallo y compensación · 3 minutos

- Crear DEMO-02 con fallo Provisioning.
- Esperar ProvisioningFailed, ActivationFailed y BillingAccountCancelled. La cuenta se anula aunque el resultado de billing llegue tarde.
- Mostrar estado FAILED, motivo del fallo y email de error.
- Confirmar cuenta CANCELLED desde la base de billing:

```powershell
docker compose exec postgres psql -U postgres -d billing -c "SELECT activation_id,customer_id,status FROM billing_accounts ORDER BY created_at DESC;"
```

- En Kafka UI filtrar por correlationId. No debe haber este fallo simulado en DLQ: es un resultado de negocio.
- Si sobra tiempo, repetir con fallo Billing: termina FAILED sin crear cuenta; provisioning puede terminar después y queda solo en historial.

Explicación: saga por coreografía; ante fallo se compensa un efecto ya creado. La intención de compensación también cubre el orden inverso. Para la carrera «fallo antes del alta», ver test automatizado de BillingService.

## 3. Servicio caído y recuperación · 3 minutos

Primero haber completado el escenario 1 asegura que notification-svc tiene offsets confirmados.

```powershell
docker compose stop notification-service
```

Crear tres activaciones **sin fallo**: DEMO-03A, DEMO-03B y DEMO-03C. Esperar ACTIVE en cada una. En Kafka UI → Consumers → notification-svc ver lag total 3 (se suma entre las tres particiones). Mailhog no debe recibir todavía sus emails.

```powershell
docker compose start notification-service
docker compose logs --tail 40 notification-service
```

Esperar lag 0 y los tres emails. Confirmar que los eventos de otras activaciones no se perdieron. No reiniciar Mailhog en este escenario.

Explicación: Kafka conserva los mensajes mientras el consumidor no está; el grupo continúa a partir de sus offsets al volver, dentro de la ventana de retención.

## 4. Escalar billing · 3 minutos

```powershell
docker compose up -d --scale billing-service=2
docker compose ps billing-service
```

En Kafka UI → Consumers → billing-svc, mostrar dos miembros y las tres particiones repartidas entre ellos, para cada topic suscrito. La distribución exacta puede variar. Crear pedidos con varios customerId; un solo cliente usa una sola partición por topic, por lo que no demuestra reparto de carga entre particiones.

No hay container_name ni puerto fijo en billing, así que no se produce colisión. Ambos miembros comparten grupo y base de billing. La idempotencia y los locks protegen los efectos.

Para volver al estado inicial:

```powershell
docker compose up -d --scale billing-service=1
```

Explicación: dentro del mismo grupo una partición se asigna a un solo consumidor; dos instancias reparten trabajo, no duplican todos los mensajes. Con tres particiones, más de tres miembros no da más paralelismo para ese topic.

## 5. Consumidor nuevo y replay · 2 minutos

Crear activaciones antes de iniciar loyalty. Luego:

```powershell
docker compose --profile loyalty up -d loyalty-service
docker compose logs -f loyalty-service
```

Mostrar mensajes «Replay ActivationCompleted/ActivationFailed». Loyalty escucha activation.events desde el primer offset retenido con un grupo independiente. Asigna 100 puntos simulados solo a activaciones exitosas, una vez por activationId:

```powershell
docker compose exec postgres psql -U postgres -d loyalty -c "SELECT activation_id,customer_id,points FROM loyalty_activations ORDER BY created_at;"
```

Los otros servicios no se modificaron. Para repetir desde un grupo nuevo en PowerShell:

```powershell
$env:LOYALTY_GROUP_ID='loyalty-demo-v2'
docker compose --profile loyalty up -d --force-recreate loyalty-service
```

La tabla permanece sin duplicados. Un grupo nuevo vuelve a leer el historial **retenido**, no eventos ya eliminados por retención.

CRM también puede demostrarlo con un grupo nuevo:

```powershell
$env:CRM_GROUP_ID='crm-replay-v2'
docker compose up -d --force-recreate crm-analytics-service
docker compose logs --tail 50 crm-analytics-service
```

CRM registra todos los topics, y su event_log conserva un único registro por eventId. El replay vuelve a consumir y los duplicados no generan filas extra.

## Pruebas de aceptación adicionales

Con Node.js 22.19+ y `npm ci`, ejecutar `npm run demo:verify`. El script genera datos de prueba, verifica los tres modos de fallo, timeline Socket.IO, emails, duplicados e inválido a DLQ seguido de otro pedido válido. No lo ejecutar simultáneamente con acciones manuales sobre sus activaciones.

Para inspección manual de duplicados: en Kafka UI copiar value **y key** de un ActivationRequested y volver a publicarlos al mismo topic sin cambiar eventId. Hacer lo mismo con un evento terminal de activation.events. Consultar la cuenta, el historial y Mailhog: no debe aparecer otro efecto. Para DLQ enviar un JSON incompatible (p. ej. `{"version":999}`) al topic activation.requested; tras los tres fallos se ve en activation.requested.dlq con motivo/offset/grupo. Después publicar una activación válida.

## Recuperación del outbox

Después de crear datos, reiniciar una aplicación con `docker compose restart activation-api`. Para una prueba más exigente, detener Kafka y solicitar una activación mientras la API siga disponible: el POST guarda PENDING y outbox; al iniciar Kafka, el relay reintenta. El tiempo de reconexión puede variar y debe verificarse localmente. Consultar el outbox sin modificarlo:

```powershell
docker compose exec postgres psql -U postgres -d activation -c "SELECT seq,topic,status,created_at,sent_at FROM outbox ORDER BY seq DESC LIMIT 15;"
```

No atribuir una garantía de exactly-once a este mecanismo: puede repetir publicaciones con el mismo eventId y los consumidores las deduplican.
