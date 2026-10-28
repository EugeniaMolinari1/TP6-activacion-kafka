import { Injectable, OnModuleDestroy } from "@nestjs/common";
import nodemailer from "nodemailer";
import { Database, once } from "../../../libs/kafka-toolkit/database";
import { BusinessEvent } from "../../../libs/contracts";
@Injectable()
export class NotificationService implements OnModuleDestroy {
  private readonly mail = nodemailer.createTransport({
    host: process.env.SMTP_HOST || "mailhog",
    port: Number(process.env.SMTP_PORT || 1025),
    secure: false,
    connectionTimeout: 5000,
    socketTimeout: 10000,
  });
  constructor(private readonly db: Database) {}
  // Mailhog is a simulation: deterministic Message-ID + inspection reconciles a crash
  // after SMTP accepted but before the transaction committed. No microservice RPCs.
  private async alreadyDelivered(messageId: string) {
    const base = process.env.MAILHOG_API_URL || "http://mailhog:8025";
    for (let start = 0; ; start += 100) {
      const response = await fetch(
        `${base}/api/v2/messages?start=${start}&limit=100`,
        { signal: AbortSignal.timeout(5000) },
      );
      if (!response.ok) throw new Error(`Mailhog HTTP ${response.status}`);
      const data = (await response.json()) as {
        total: number;
        items: Array<{ Content: { Headers: Record<string, string[]> } }>;
      };
      if (
        data.items.some((m) =>
          (m.Content.Headers["Message-ID"] || []).includes(messageId),
        )
      )
        return true;
      if (start + 100 >= data.total) return false;
    }
  }
  async receive(e: BusinessEvent) {
    await this.db.transaction(async (tx) => {
      await tx.query("SELECT pg_advisory_xact_lock(hashtextextended($1,0))", [
        e.correlationId + e.eventType,
      ]);
      await once(tx, e, async () => {
        const existing = await tx.query(
          "SELECT 1 FROM notifications WHERE activation_id=$1 AND event_type=$2",
          [e.correlationId, e.eventType],
        );
        if (existing.rowCount) return;
        const messageId = `<${e.correlationId}.${e.eventType}@tp6.local>`;
        if (!(await this.alreadyDelivered(messageId)))
          await this.mail.sendMail({
            from: "activaciones@tp6.local",
            to: "cliente@demo.local",
            messageId,
            subject:
              e.eventType === "ActivationCompleted"
                ? "Tu plan está activo"
                : "No pudimos activar tu plan",
            text: `Cliente: ${e.customerId}\nActivación: ${e.correlationId}\nEvento: ${e.eventType}\n${e.payload.reason || "¡Bienvenido!"}`,
          });
        await tx.query(
          "INSERT INTO notifications(activation_id,event_type,event_id,message_id) VALUES($1,$2,$3,$4)",
          [e.correlationId, e.eventType, e.eventId, messageId],
        );
      });
    });
  }
  onModuleDestroy() {
    this.mail.close();
  }
}
