# Lista de evidencias para la entrega

Obtener capturas **de la ejecución real en tu computadora**, no de estas instrucciones. No se adjuntan capturas de contenedores que no se pudieron ejecutar en el entorno remoto. Guardarlas con títulos claros; si el profesor pide una memoria, seguir el orden siguiente.

| Archivo sugerido | Qué mostrar | Qué demuestra |
| --- | --- | --- |
| 01-entorno.png | docker compose ps -a; kafka-init exit 0 y apps healthy | Arranque de Compose |
| 02-topics.png | Cuatro topics + cuatro DLQ, 3 particiones | Configuración Kafka |
| 03-post-202.png | POST, status 202 y JSON con activationId/PENDING | Respuesta asíncrona |
| 04-camino-feliz.png | UI con ACTIVE y los cuatro eventos | RF-01 a RF-05, RF-08 |
| 05-email-exito.png | Mailhog con bienvenida y mismo ID | Notificación |
| 06-compensacion.png | FAILED, fallo y BillingAccountCancelled | Saga y compensación |
| 07-cuenta-cancelada.png | SQL billing_accounts con CANCELLED | Persistencia de compensación |
| 08-email-error.png | Email de error del mismo ID | Notificación de fallo |
| 09-lag-3.png | notification detenido y lag 3 | Recuperación pendiente |
| 10-lag-0-emails.png | Servicio levantado, lag 0 y 3 emails | RNF-05 |
| 11-escala.png | Dos billing y particiones asignadas en Kafka UI | Consumer groups |
| 12-loyalty-replay.png | Logs de eventos previos y tabla loyalty | Nuevo consumidor y replay |
| 13-idempotencia.png | Misma cuenta/email después de reenvío | RNF-03 |
| 14-dlq.png | Mensaje original y headers attempts=3, error | RNF-04 |
| 15-get-historial.png | GET con estado, steps e historial | RF-09 |
| 16-crm.png | event_log con correlationId y eventos de los 4 topics | RF-10 |
| 17-pruebas.png | npm test y npm run test:integration | Pruebas automatizadas |
| 18-outbox.png | PENDING/SENT y mismo eventId | Outbox PostgreSQL |

Guardar también un video corto de los cinco escenarios como respaldo. Registrar fecha, comando, resultado esperado, resultado real e ID usado. No marcar un escenario como aprobado antes de observar su resultado.

Consultas de apoyo (PowerShell):

```powershell
docker compose exec postgres psql -U postgres -d crm -c "SELECT event_id,correlation_id,event->>'eventType' AS tipo FROM event_log ORDER BY received_at;"
docker compose exec postgres psql -U postgres -d notification -c "SELECT activation_id,event_type,message_id,sent_at FROM notifications;"
docker compose exec postgres psql -U postgres -d activation -c "SELECT activation_id,status,billing_status,provisioning_status FROM activations;"
```

La cuenta administradora se usa aquí para inspección docente; las aplicaciones usan los roles separados definidos en Compose.
