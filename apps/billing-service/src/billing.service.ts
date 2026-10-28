import { Injectable } from "@nestjs/common";
import { Database, once, outbox } from "../../../libs/kafka-toolkit/database";
import { BusinessEvent, makeEvent } from "../../../libs/contracts";
@Injectable()
export class BillingService {
  constructor(private readonly db: Database) {}
  async receive(e: BusinessEvent) {
    await this.db.transaction(async (tx) => {
      // Serialize request and compensation across topics and replicas for this activation.
      await tx.query("SELECT pg_advisory_xact_lock(hashtextextended($1,0))", [
        e.correlationId,
      ]);
      await once(tx, e, async () => {
        if (e.eventType === "ActivationFailed") {
          await tx.query(
            "INSERT INTO compensation_requests(activation_id) VALUES($1) ON CONFLICT DO NOTHING",
            [e.correlationId],
          );
          const changed = await tx.query(
            "UPDATE billing_accounts SET status='CANCELLED',updated_at=now() WHERE activation_id=$1 AND status='CREATED' RETURNING activation_id",
            [e.correlationId],
          );
          if (changed.rowCount)
            await outbox(
              tx,
              makeEvent(
                "BillingAccountCancelled",
                "billing-service",
                e.correlationId,
                e.customerId,
                { reason: e.payload.reason },
              ),
            );
        } else if (e.eventType === "ActivationRequested") {
          const existing = await tx.query(
            "SELECT 1 FROM billing_accounts WHERE activation_id=$1",
            [e.correlationId],
          );
          if (existing.rowCount) return;
          if (e.payload.simulateFailure === "billing") {
            await outbox(
              tx,
              makeEvent(
                "BillingFailed",
                "billing-service",
                e.correlationId,
                e.customerId,
                { reason: "Fallo de facturación simulado" },
              ),
            );
            return;
          }
          const cancelled = await tx.query(
            "SELECT 1 FROM compensation_requests WHERE activation_id=$1",
            [e.correlationId],
          );
          await tx.query(
            "INSERT INTO billing_accounts(activation_id,customer_id,status) VALUES($1,$2,$3)",
            [
              e.correlationId,
              e.customerId,
              cancelled.rowCount ? "CANCELLED" : "CREATED",
            ],
          );
          await outbox(
            tx,
            makeEvent(
              "BillingAccountCreated",
              "billing-service",
              e.correlationId,
              e.customerId,
              {},
            ),
          );
          if (cancelled.rowCount)
            await outbox(
              tx,
              makeEvent(
                "BillingAccountCancelled",
                "billing-service",
                e.correlationId,
                e.customerId,
                { reason: "Compensación recibida antes del alta" },
              ),
            );
        }
      });
    });
  }
}
