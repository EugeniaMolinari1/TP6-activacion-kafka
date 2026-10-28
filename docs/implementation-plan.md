# Plan y alcance de implementación

Se leyó el PDF completo de 13 páginas, incluidas las tablas y los diagramas de arquitectura, secuencia y fases. Se verificó la cuenta EugeniaMolinari1 con permisos push sobre el repositorio independiente solicitado.

| Etapa | Archivos / resultado | Criterio de revisión |
| --- | --- | --- |
| 1. Base | Contratos, seis apps, inicialización PostgreSQL/Kafka, Compose | Topics y roles correctos; compilación |
| 2. Flujo | POST/GET, billing/provisioning, agregación de saga | Camino feliz y fallos; transacciones SQL |
| 3. Demo visible | UI, gateway Socket.IO, Mailhog, historial | Timeline y emails reales al ejecutar Compose |
| 4. Resiliencia | Idempotencia, retry, DLQ, CRM y loyalty | Tests y escenarios 3–5 |
| 5. Outbox | Relay y transacciones PostgreSQL | No separar guardar/publicar; reentrega tolerada |
| 6. Entrega | README, matriz, demo, evidencias y defensa | GitHub y verificación manual documentada |

Fases 1–3 de la consigna implementadas; de fase 4 se incorpora solamente outbox. No se agregan autenticación, pagos reales, métricas de carga ni integraciones de producción. El directorio apps contiene varias aplicaciones del mismo TP dentro de su único repositorio, no otros trabajos prácticos anidados.

Los criterios que dependen de Docker permanecen pendientes hasta ejecutarlos localmente. Consultar validation.md y requirements.md para distinguir implementación de prueba.
