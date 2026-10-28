import { Injectable, OnModuleDestroy } from "@nestjs/common";
import { Pool, PoolClient } from "pg";
import { commonSchema, schemas } from "./schema";
import { BusinessEvent, eventTopics } from "../contracts";
@Injectable()
export class Database implements OnModuleDestroy {
  readonly pool = new Pool({
    connectionString: process.env.DATABASE_URL,
    max: 10,
  });
  async init(service: string) {
    await this.transaction(async (tx) => {
      await tx.query("SELECT pg_advisory_xact_lock(62000)");
      await tx.query(commonSchema + schemas[service]);
    });
  }
  async transaction<T>(fn: (tx: PoolClient) => Promise<T>): Promise<T> {
    const tx = await this.pool.connect();
    try {
      await tx.query("BEGIN");
      const result = await fn(tx);
      await tx.query("COMMIT");
      return result;
    } catch (e) {
      await tx.query("ROLLBACK");
      throw e;
    } finally {
      tx.release();
    }
  }
  async onModuleDestroy() {
    await this.pool.end();
  }
}
export async function outbox(tx: PoolClient, e: BusinessEvent) {
  await tx.query("SELECT pg_advisory_xact_lock(62007)");
  await tx.query(
    "INSERT INTO outbox(event_id,topic,customer_id,event) VALUES($1,$2,$3,$4) ON CONFLICT DO NOTHING",
    [e.eventId, eventTopics[e.eventType], e.customerId, e],
  );
}
export async function history(tx: PoolClient, e: BusinessEvent) {
  await tx.query(
    "INSERT INTO activation_history(activation_id,event_id,event) VALUES($1,$2,$3) ON CONFLICT DO NOTHING",
    [e.correlationId, e.eventId, e],
  );
}
export async function once(
  tx: PoolClient,
  e: BusinessEvent,
  fn: () => Promise<void>,
) {
  const inserted = await tx.query(
    "INSERT INTO processed_events(event_id) VALUES($1) ON CONFLICT DO NOTHING RETURNING event_id",
    [e.eventId],
  );
  if (inserted.rowCount) await fn();
  return Boolean(inserted.rowCount);
}
