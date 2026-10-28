import { Injectable, Logger } from "@nestjs/common";
import { Database, once } from "../../../libs/kafka-toolkit/database";
import { BusinessEvent } from "../../../libs/contracts";
@Injectable()
export class LoyaltyService {
  constructor(private readonly db: Database) {}
  async receive(e: BusinessEvent) {
    await this.db.transaction(async (tx) => {
      await once(tx, e, async () => {
        if (e.eventType === "ActivationCompleted")
          await tx.query(
            "INSERT INTO loyalty_activations(activation_id,customer_id,points,event_id) VALUES($1,$2,100,$3) ON CONFLICT DO NOTHING",
            [e.correlationId, e.customerId, e.eventId],
          );
      });
    });
    Logger.log(
      `Replay ${e.eventType} correlationId=${e.correlationId}`,
      "Loyalty",
    );
  }
}
