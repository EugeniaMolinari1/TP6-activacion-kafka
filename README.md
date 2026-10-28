# TP6 · Activación de servicios con Kafka

POC de Programación Avanzada de EugeniaMolinari1. Implementa la consigna «POC Activación de Servicios con Kafka», con **PostgreSQL dockerizado** por indicación del profesor. Proyecto independiente, con cinco aplicaciones NestJS, UI React/Vite, Kafka KRaft, Socket.IO, Kafka UI y Mailhog. El consumidor adicional loyalty permite demostrar replay.

## Ejecutar en Windows / Docker Desktop

Requisitos: Git y Docker Desktop iniciado, con contenedores Linux y Docker Compose v2 o superior. Se recomienda disponer de 6 GB de RAM para Docker. Node.js no es necesario para levantar el sistema completo.

```powershell
git clone https://github.com/EugeniaMolinari1/TP6-activacion-kafka.git
cd TP6-activacion-kafka
docker compose up --build -d
docker compose ps -a
```

La primera ejecución descarga imágenes e instala dependencias dentro de los builds. Esperar a que activation-api y demo-ui estén healthy. `kafka-init` debe terminar con código 0 (Exited es normal para este servicio).

| Componente | URL local |
| --- | --- |
| Demo UI | http://localhost:5173 |
| API / salud | http://localhost:3000/health |
| Kafka UI | http://localhost:8080 |
| Mailhog | http://localhost:8025 |
| Kafka para herramientas locales | localhost:9092 |

Compose usa valores predeterminados de desarrollo; no es necesario crear `.env`. Para cambiar puertos o contraseña, copiar `.env.example` a `.env` **antes de la primera ejecución**. Mantener una contraseña URI-safe o codificarla si contiene caracteres reservados. Cambiarla después no actualiza usuarios de un volumen PostgreSQL existente.

```powershell
docker compose logs -f activation-api billing-service provisioning-service notification-service
docker compose stop
docker compose start
```

`docker compose down` elimina contenedores y red, pero conserva los volúmenes de Kafka y PostgreSQL. Solo para reiniciar toda la demo desde cero (borra los datos): `docker compose down -v`.

Si recibiste el proyecto como ZIP mientras se habilita GitHub, extraelo, abrí una terminal en la carpeta que contiene `docker-compose.yml` y ejecutá `docker compose up --build -d`.

## Uso

En la UI ingresar cliente y plan. Elegir ninguno, billing o provisioning para simular un fallo de negocio. La API responde inmediatamente `202` y `PENDING`; la timeline se actualiza por Socket.IO. Las activaciones recientes quedan guardadas en el navegador y también pueden consultarse por ID.

Ejemplo PowerShell:

```powershell
$body = @{ customerId = 'C-1234'; planId = 'Plan Digital'; simulateFailure = 'none' } | ConvertTo-Json
$r = Invoke-WebRequest -Method Post -Uri http://localhost:3000/activations -ContentType 'application/json' -Body $body
$r.StatusCode
$activation = $r.Content | ConvertFrom-Json
Invoke-RestMethod "http://localhost:3000/activations/$($activation.activationId)"
```

Campos de entrada: `customerId`, `planId`, `channel` opcional (web) y `simulateFailure` opcional (`none`, `billing`, `provisioning`). Datos vacíos o modos inválidos devuelven 400; un ID inexistente devuelve 404.

## Flujo y estructura

1. activation-api guarda activación, historial y ActivationRequested en outbox en una transacción SQL.
2. El relay publica en `activation.requested` con key customerId.
3. billing y provisioning reciben en grupos distintos, trabajan en paralelo y guardan efecto + resultado en outbox.
4. activation-api agrega resultados: ambos OK → ACTIVE / ActivationCompleted; primer fallo → FAILED / ActivationFailed. Los resultados tardíos quedan en historial y no reabren la saga.
5. billing compensa incluso si el fallo llega antes del alta. notification envía un email simulado a Mailhog. CRM registra todos los eventos.

```text
apps/activation-api/          REST, agregador, historial y gateway Socket.IO
apps/billing-service/         Alta y compensación de cuentas
apps/provisioning-service/    Aprovisionamiento simulado de 1 a 3 segundos
apps/notification-service/    SMTP a Mailhog
apps/crm-analytics-service/   Log independiente de todos los eventos
apps/loyalty-service/         Replay de activaciones exitosas (perfil loyalty)
apps/demo-ui/                 React y Vite; Nginx en Docker
libs/contracts/               Tipos, validación, versión y nombres de topics
libs/kafka-toolkit/           Nest Kafka, transacciones, idempotencia, retries, DLQ y outbox
docker/                      Inicialización de bases y topics
tests/                       Pruebas de contratos, saga, toolkit y SQL
scripts/verify-demo.mjs       Verificación contra el entorno Docker levantado
docs/                        Arquitectura, matriz, demo, evidencias y defensa
```

No hay llamadas directas entre microservicios ni acceso a la base de otro servicio. PostgreSQL tiene seis bases y seis roles separados, incluida la del consumidor opcional. Los DDL de tablas e índices viven en `libs/kafka-toolkit/schema.ts` y se aplican al iniciar cada aplicación en su propia base. `docker/init-postgres.sh` crea bases, usuarios y permisos.

## Pruebas

Para ejecutar las pruebas sin Kafka ni PostgreSQL instalados, se necesita Node.js 22.19+ y npm:

```powershell
npm ci
npm run build
npm test
npm run test:integration
npm ci --prefix apps/demo-ui
npm run build:ui
```

Las pruebas SQL utilizan PostgreSQL embebido mediante PGlite. Ejecutan las consultas reales, transacciones y restricciones; simulan los advisory locks y SMTP. Las pruebas del toolkit simulan el contexto Kafka. **No equivalen a una prueba integral con contenedores.**

Con Docker levantado y dependencias npm instaladas:

```powershell
npm run demo:verify
```

Ese script comprueba los modos sin fallo / provisioning / billing, estados, WebSocket, email único, reentrega de eventos, envío de un contrato inválido a DLQ y continuidad posterior. **Genera activaciones y un mensaje inválido de prueba.** Los escenarios de caída, escalado y nuevo consumidor se ejecutan según [guion de demo](docs/demo-script.md).

## Estado de validación

Se verificaron las compilaciones backend y frontend, las pruebas automatizadas y la sintaxis de los scripts. Ver detalle y límites en [validación](docs/validation.md). El entorno de desarrollo remoto no tiene Docker; quedan pendientes el build de imágenes, el arranque real de Compose, Kafka KRaft, SMTP/Mailhog, Socket.IO en navegador y los cinco escenarios completos. No se afirma que esas comprobaciones hayan pasado.

## Documentación de entrega

- [Arquitectura y decisiones técnicas](docs/architecture.md)
- [Plan de implementación](docs/implementation-plan.md)
- [Matriz de los 17 requerimientos](docs/requirements.md)
- [Cinco escenarios de demo](docs/demo-script.md)
- [Evidencias y capturas](docs/evidence.md)
- [Defensa oral del código](docs/oral-defense.md)
- [Pruebas realizadas y pendientes](docs/validation.md)

La POC incluye fases 1 a 3 y outbox transaccional de fase 4 para cerrar la ventana entre guardar y publicar. No incluye Schema Registry, OpenTelemetry, autenticación, proveedores reales ni alta disponibilidad, que quedan fuera del alcance solicitado.
