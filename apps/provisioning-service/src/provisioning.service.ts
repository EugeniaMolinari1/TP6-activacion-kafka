import { Injectable } from "@nestjs/common";
import { Database, once, outbox } from "../../../libs/kafka-toolkit/database";
import { BusinessEvent, makeEvent } from "../../../libs/contracts";
import { wait } from "../../../libs/kafka-toolkit/retry";
@Injectable()
export class ProvisioningService {
  constructor(private readonly db: Database) {}
  async receive(e: BusinessEvent) {
    await wait(
      Number(
        process.env.PROVISIONING_DELAY_MS ||
          1000 + Math.floor(Math.random() * 2001),
      ),
    );
    await this.db.transaction(async (tx) => {
      await once(tx, e, async () => {
        const failed = e.payload.simulateFailure === "provisioning";
        const inserted = await tx.query(
          "INSERT INTO provisioning_orders(activation_id,customer_id,status) VALUES($1,$2,$3) ON CONFLICT DO NOTHING RETURNING activation_id",
          [e.correlationId, e.customerId, failed ? "FAILED" : "COMPLETED"],
        );
        if (inserted.rowCount)
          await outbox(
            tx,
            makeEvent(
              failed ? "ProvisioningFailed" : "ProvisioningCompleted",
              "provisioning-service",
              e.correlationId,
              e.customerId,
              failed ? { reason: "Fallo de aprovisionamiento simulado" } : {},
            ),
          );
      });
    });
  }
}
