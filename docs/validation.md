# Registro de validación y límites

Fecha de preparación: 15 de octubre de 2026. Entorno remoto: Node.js 24.19.0, npm 11.9.0. Imágenes de aplicaciones configuradas con Node 22.19.0. La diferencia de versión del runtime se deberá cubrir con el build Docker local.

## Comprobado satisfactoriamente

| Comprobación | Resultado |
| --- | --- |
| Instalación npm raíz y UI, con lockfiles | Completada |
| npm run build | TypeScript backend compilado |
| npm run build:ui | Bundle Vite de producción generado |
| npm test | 8 pruebas, todas aprobadas |
| npm run test:integration | 6 pruebas, todas aprobadas |
| sh -n de los dos scripts docker/ | Sintaxis válida |
| node --check scripts/verify-demo.mjs | Sintaxis válida |
| Compose mediante parser YAML | Estructura, servicios, rutas, perfil loyalty y posibilidad de escala comprobados por test |
| npm audit --omit=dev raíz | 0 vulnerabilidades informadas en la consulta realizada |

Pruebas principales: contratos y clave Kafka; ambos órdenes de éxito; estado FAILED terminal; tres intentos/backoff; offset posterior al efecto y precisión BigInt; DLQ con original y headers; DLQ caída sin commit; datos SQL, restricciones e idempotencia; compensación antes y después del alta; rollback; CRM y loyalty; notificación simulada tras caída entre SMTP y commit.

**Alcance de estas pruebas:** PGlite ejecuta PostgreSQL en WASM usando el DDL y las consultas del proyecto. El adaptador del test simula advisory locks y conexiones; no valida aislamiento entre procesos ni permisos de roles. KafkaContext, transporte SMTP y consulta previa a Mailhog se sustituyen por dobles donde corresponde. No se probó un broker Kafka ni un servidor SMTP real.

## Pendiente de comprobar en tu computadora

El ejecutable Docker no está instalado en el entorno remoto. No se ejecutó Docker Compose ni se descargaron sus imágenes; no se presenta una captura ficticia del sistema levantado.

1. `docker compose config` y `docker compose --profile loyalty config`: interpretación real de Compose, variables y rutas Windows.
2. `docker compose up --build -d`: disponibilidad de tags/imágenes, permisos de volúmenes Kafka/PostgreSQL y build de cada imagen.
3. `docker compose ps -a`: healthchecks y kafka-init Exited (0). Arranque y conexión real de las cinco apps.
4. `npm run demo:verify` (requiere Node y npm ci): HTTP 202/GET, key/topic reales, Socket.IO, Mailhog, duplicados, inválidos a DLQ y continuidad.
5. Cinco escenarios de docs/demo-script.md: camino feliz, compensación, caída/lag/recuperación, escala con rebalanceo y loyalty replay.
6. Uso visual de la UI, reconexión del navegador y misma timeline tras recargar/consultar ID.
7. Inspección de permisos: cada aplicación usa rol propio y no puede conectarse a otra base.
8. Reinicio durante un outbox pendiente y recuperación sin pérdida de efectos. Los advisory locks entre réplicas requieren PostgreSQL real.
9. Reconciliación real de Message-ID en Mailhog tras fallo entre SMTP y commit. No reiniciar ni borrar Mailhog durante esta prueba.
10. Duración de la demo inferior a 15 minutos y captura de evidencias reales.

No se añaden resultados aprobados a estos pendientes hasta ejecutar los comandos y observarlos.

## Diagnóstico

- Kafka tarda: revisar `docker compose logs kafka kafka-init`, esperar su healthcheck y el init de topics; no cambiar a publicación directa para evitar la espera.
- Un worker reinicia: `docker compose logs --tail 100 <servicio>`; confirmar DATABASE_URL, topics y broker interno `kafka:29092`.
- Error de contraseña después de cambiar .env: un volumen existente conserva roles anteriores. Restaurar la contraseña anterior o reinicializar explícitamente la demo con pérdida de datos.
- UI sin conexión: API healthy, puerto correcto y origen `http://localhost:5173`; si se usa 127.0.0.1 como origen, ajustar CORS_ORIGIN.
- Emails no llegan: revisar SMTP y Mailhog HTTP. Los errores técnicos persistentes terminan en activation.events.dlq y requieren inspección/reenvío manual tras resolver la causa.
- Durante pausa de notification, lag distinto de 3: esperar estados finales y comprobar que no hay otras activaciones en curso; sumar lag de todas las particiones.
- Replay no muestra eventos nuevos: utilizar un groupId que nunca existió y conservar el volumen Kafka; solo se recupera el historial retenido.

## Guardado en GitHub

Se utilizó exclusivamente la conexión de EugeniaMolinari1. La consulta del repositorio devolvió push=true, pero el intento de crear README.md devolvió HTTP 403: Resource not accessible by integration. La consulta de instalaciones habilitadas para esa conexión devolvió una lista vacía. No se escribió código en GitHub ni se utilizó otra cuenta; queda pendiente habilitar la autorización efectiva de escritura de contenido de la integración y publicar este mismo proyecto.
