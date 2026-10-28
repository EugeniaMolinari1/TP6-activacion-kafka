# Matriz de cumplimiento de los 17 requerimientos

**Estado de implementación: los 17 tienen código/configuración.** El cumplimiento integral no se da por verificado: los checks de la última columna requieren ejecución local con Docker. “Test” se refiere a las pruebas que se describen en validation.md.

| ID | Requisito | Implementación | Comprobación automatizada local | Pendiente con Docker |
| --- | --- | --- | --- | --- |
| RF-01 | Formulario cliente, plan y fallo opcional | apps/demo-ui/src/main.jsx; createActivationSchema | Contrato válido/inválido (test) | Uso visual del formulario |
| RF-02 | POST devuelve 202/PENDING sin esperar workers | ActivationController.create; ActivationService.create | Retorno PENDING y compilación | HTTP 202 real: demo:verify |
| RF-03 | Publicar ActivationRequested | ActivationService.create; outbox; KafkaToolkit | Outbox y sobre SQL | Registro en topic real |
| RF-04 | Fan-out billing/provisioning paralelo | Controllers con grupos billing-svc / provisioning-svc | Procesamiento de ambos y resultados SQL | Grupos y paralelismo Kafka real |
| RF-05 | Ambos OK → ACTIVE y ActivationCompleted | aggregate; ActivationService.receive | Ambos órdenes de llegada; cierre único | Demo 1 |
| RF-06 | Fallo → FAILED, ActivationFailed y compensación | BillingService; compensation_requests; aggregate | Compensación previa/posterior; resultado tardío | Demo 2 y cuenta CANCELLED |
| RF-07 | Un email por evento terminal | NotificationService; UNIQUE y Message-ID | Doble SMTP y recuperación antes de commit | Mailhog real y demo:verify |
| RF-08 | Timeline en vivo por WebSocket | EventsGateway; demo-ui | Compilación y revisión de suscripción | Navegador, reconexión y demo:verify |
| RF-09 | GET estado e historial | ActivationService.get; activation_history | Lectura SQL y 4 eventos sin duplicados | GET HTTP; demo:verify |
| RF-10 | CRM guarda todos y puede hacer replay | CrmService; fromBeginning; CRM_GROUP_ID | event_log sin duplicados al reentregar | Grupo nuevo leyendo historial |
| RNF-01 | Todo levanta con Docker Compose | docker-compose.yml; Dockerfiles; docker/ | Parse YAML, rutas, sh -n | docker compose config / up; imágenes |
| RNF-02 | Orden por cliente con key customerId | KafkaToolkit.publish; relay secuencial; 3 particiones | Validación de key y revisión de outbox | Inspección de key/partición y escalado |
| RNF-03 | Consumidores idempotentes | once; processed_events; restricciones únicas | Reentregas SQL; rollback; SMTP simulado | Reenvío en Kafka y demo:verify |
| RNF-04 | Tres fallos → DLQ sin bloquear | retryThree; KafkaToolkit.consume | Backoff, original/headers, commit y DLQ caída | Inválido seguido de válido: demo:verify |
| RNF-05 | Recuperación de pendientes tras caída | Commit manual; volúmenes; outbox | Offset posterior al efecto; SQL idempotente | Demo 3, restart y lag |
| RNF-06 | CorrelationId en todos los eventos | makeEvent; parseEvent | Contrato y propagación en flujo SQL | Filtrar una saga en UI/Kafka/CRM |
| RNF-07 | Contratos compartidos versionados | libs/contracts/index.ts | Version=1; rechazo de versión incompatible | Inspección del mensaje real |
