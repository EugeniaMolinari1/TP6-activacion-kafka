import { Injectable, Logger } from "@nestjs/common";
import { Database, once } from "../../../libs/kafka-toolkit/database";
import { BusinessEvent } from "../../../libs/contracts";
@Injectable()
export class CrmService {
  constructor(private readonly db: Database) {}
  async receive(e: BusinessEvent) {
    await this.db.transaction(async (tx) => {
      await once(tx, e, async () => {
        await tx.query(
          "INSERT INTO event_log(event_id,correlation_id,event) VALUES($1,$2,$3) ON CONFLICT DO NOTHING",
          [e.eventId, e.correlationId, e],
        );
      });
    });
    Logger.log(`${e.eventType} correlationId=${e.correlationId}`, "CRM");
  }
}
