import { Injectable, Logger, OnModuleDestroy } from "@nestjs/common";
import {
  ClientKafka,
  KafkaContext,
  KafkaRetriableException,
} from "@nestjs/microservices";
import { firstValueFrom } from "rxjs";
import { BusinessEvent, parseEvent } from "../contracts";
import { Database } from "./database";
import { retryThree, wait } from "./retry";
@Injectable()
export class KafkaToolkit implements OnModuleDestroy {
  private readonly logger = new Logger(KafkaToolkit.name);
  private readonly client = new ClientKafka({
    client: {
      brokers: (process.env.KAFKA_BROKERS || "localhost:9092").split(","),
      clientId:
        process.env.KAFKA_CLIENT_ID || process.env.SERVICE_NAME || "tp6",
    },
    producerOnlyMode: true,
    producer: { idempotent: true, maxInFlightRequests: 1 },
    postfixId: "",
  });
  private stopped = false;
  private relay?: Promise<void>;
  constructor(private readonly db: Database) {}
  async connect() {
    await this.client.connect();
    this.relay = this.runRelay();
  }
  async publish(
    topic: string,
    key: string,
    value: unknown,
    headers: Record<string, string> = {},
  ) {
    await firstValueFrom(this.client.emit(topic, { key, value, headers }));
  }
  private async runRelay() {
    while (!this.stopped) {
      try {
        // A single relay per database, including across replicas. Head-of-line publication
        // preserves sequence; SKIP LOCKED would let a second replica reorder same-key events.
        await this.db.transaction(async (tx) => {
          const lock = await tx.query(
            "SELECT pg_try_advisory_xact_lock(62006) AS acquired",
          );
          if (!lock.rows[0].acquired) return;
          const pending = await tx.query(
            "SELECT * FROM outbox WHERE status='PENDING' ORDER BY seq LIMIT 50 FOR UPDATE",
          );
          for (const row of pending.rows) {
            await this.publish(row.topic, row.customer_id, row.event);
            await tx.query(
              "UPDATE outbox SET status='SENT',sent_at=now() WHERE seq=$1",
              [row.seq],
            );
          }
          await tx.query(
            "DELETE FROM processed_events WHERE processed_at < now()-interval '7 days'",
          );
        });
      } catch (e) {
        this.logger.error(`Outbox pendiente; se reintentará: ${String(e)}`);
      }
      await wait(500);
    }
  }
  async consume(
    value: unknown,
    context: KafkaContext,
    handler: (e: BusinessEvent) => Promise<void>,
  ) {
    const message = context.getMessage(),
      topic = context.getTopic(),
      partition = context.getPartition();
    // Keep the consumer alive while retries/SMTP/provisioning run.
    const pulse = setInterval(() => {
      void context
        .getHeartbeat()()
        .catch((e) => this.logger.warn(String(e)));
    }, 2000);
    try {
      try {
        await retryThree(async () => {
          const decoded = Buffer.isBuffer(value)
            ? JSON.parse(value.toString("utf8"))
            : value;
          const e = parseEvent(decoded, topic, message.key?.toString() ?? "");
          await handler(e);
        });
      } catch (error) {
        try {
          // Preserve raw original bytes, even if JSON/contract validation failed.
          await this.publish(
            topic + ".dlq",
            message.key?.toString() || "",
            message.value?.toString() ?? "",
            {
              error: String(error).slice(0, 1000),
              "consumer-group": process.env.KAFKA_GROUP_ID || "",
              "original-topic": topic,
              "original-partition": String(partition),
              "original-offset": message.offset,
              attempts: "3",
            },
          );
        } catch (dlqError) {
          throw new KafkaRetriableException(
            `DLQ no disponible: ${String(dlqError)}`,
          );
        }
      }
      try {
        await context
          .getConsumer()
          .commitOffsets([
            {
              topic,
              partition,
              offset: (BigInt(message.offset) + 1n).toString(),
            },
          ]);
      } catch (e) {
        throw new KafkaRetriableException(String(e));
      }
    } finally {
      clearInterval(pulse);
    }
  }
  async onModuleDestroy() {
    this.stopped = true;
    await this.relay;
    await this.client.close();
  }
}
