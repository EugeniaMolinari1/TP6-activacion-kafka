import {
  BadRequestException,
  Injectable,
  NotFoundException,
} from "@nestjs/common";
import { randomUUID } from "node:crypto";
import {
  Database,
  history,
  once,
  outbox,
} from "../../../libs/kafka-toolkit/database";
import {
  aggregate,
  BusinessEvent,
  createActivationSchema,
  makeEvent,
} from "../../../libs/contracts";
@Injectable()
export class ActivationService {
  constructor(private readonly db: Database) {}
  async create(input: unknown) {
    const parsed = createActivationSchema.safeParse(input);
    if (!parsed.success) throw new BadRequestException(parsed.error.flatten());
    const { customerId, planId, channel, simulateFailure } = parsed.data,
      activationId = randomUUID();
    const e = makeEvent(
      "ActivationRequested",
      "activation-api",
      activationId,
      customerId,
      { planId, channel, simulateFailure },
    );
    await this.db.transaction(async (tx) => {
      await tx.query(
        `INSERT INTO activations(activation_id,customer_id,plan_id,channel,simulate_failure,status) VALUES($1,$2,$3,$4,$5,'PENDING')`,
        [activationId, customerId, planId, channel, simulateFailure],
      );
      await history(tx, e);
      await outbox(tx, e);
    });
    return { activationId, status: "PENDING" };
  }
  async get(id: string) {
    const result = await this.db.pool.query(
      "SELECT * FROM activations WHERE activation_id=$1",
      [id],
    );
    if (!result.rowCount) throw new NotFoundException("Activación inexistente");
    const a = result.rows[0];
    const events = await this.db.pool.query(
      "SELECT event FROM activation_history WHERE activation_id=$1 ORDER BY seq",
      [id],
    );
    return {
      activationId: a.activation_id,
      customerId: a.customer_id,
      planId: a.plan_id,
      status: a.status,
      simulateFailure: a.simulate_failure,
      steps: { billing: a.billing_status, provisioning: a.provisioning_status },
      history: events.rows.map((r) => r.event),
      createdAt: a.created_at,
      updatedAt: a.updated_at,
    };
  }
  async receive(e: BusinessEvent) {
    await this.db.transaction(async (tx) => {
      await once(tx, e, async () => {
        const row = await tx.query(
          "SELECT * FROM activations WHERE activation_id=$1 FOR UPDATE",
          [e.correlationId],
        );
        if (!row.rowCount) throw new Error("Activación todavía no disponible");
        const a = row.rows[0];
        if (a.customer_id !== e.customerId)
          throw new Error("customerId no coincide con activación");
        await history(tx, e);
        const next = aggregate(
          a.status,
          a.billing_status,
          a.provisioning_status,
          e.eventType,
        );
        await tx.query(
          "UPDATE activations SET status=$2,billing_status=$3,provisioning_status=$4,updated_at=now() WHERE activation_id=$1",
          [e.correlationId, next.status, next.billing, next.provisioning],
        );
        if (
          next.status !== a.status &&
          (next.status === "ACTIVE" || next.status === "FAILED")
        ) {
          const final = makeEvent(
            next.status === "ACTIVE"
              ? "ActivationCompleted"
              : "ActivationFailed",
            "activation-api",
            e.correlationId,
            e.customerId,
            next.status === "ACTIVE"
              ? { planId: a.plan_id }
              : {
                  planId: a.plan_id,
                  reason: String(e.payload.reason || "Fallo de activación"),
                },
          );
          await history(tx, final);
          await outbox(tx, final);
        }
      });
    });
  }
}
