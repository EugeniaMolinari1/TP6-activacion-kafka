import "reflect-metadata";
import { test } from "node:test";
import assert from "node:assert/strict";
import { PGlite } from "@electric-sql/pglite";
import { commonSchema, schemas } from "../../libs/kafka-toolkit/schema";
import { Database } from "../../libs/kafka-toolkit/database";
import { makeEvent, parseEvent, BusinessEvent } from "../../libs/contracts";
import { ActivationService } from "../../apps/activation-api/src/activation.service";
import { BillingService } from "../../apps/billing-service/src/billing.service";
import { ProvisioningService } from "../../apps/provisioning-service/src/provisioning.service";
import { CrmService } from "../../apps/crm-analytics-service/src/crm.service";
import { LoyaltyService } from "../../apps/loyalty-service/src/loyalty.service";
// Real embedded PostgreSQL (WASM), same DDL/queries. Advisory locks are stubbed here:
// these tests verify SQL transactions and constraints; cross-process locks need Compose.
async function fixture(service: string) {
  const pg = new PGlite();
  await pg.exec(commonSchema + schemas[service]);
  const query = async (sql: string, args?: unknown[]) => {
    if (sql.includes("pg_advisory_xact_lock"))
      return { rows: [{}], rowCount: 1 };
    const r = await pg.query(sql, args);
    return { ...r, rowCount: r.rows.length || r.affectedRows || 0 };
  };
  const db = {
    pool: { query },
    transaction: async (fn: any) => {
      await pg.exec("BEGIN");
      try {
        const result = await fn({ query });
        await pg.exec("COMMIT");
        return result;
      } catch (e) {
        await pg.exec("ROLLBACK");
        throw e;
      }
    },
  } as unknown as Database;
  return { db, pg, query };
}
async function events(f: Awaited<ReturnType<typeof fixture>>) {
  return (await f.query("SELECT event FROM outbox ORDER BY seq")).rows.map(
    (r: any) => r.event as BusinessEvent,
  );
}
test("flujo SQL completo: creación 202/PENDING, resultados, historial, outbox y duplicados", async () => {
  const a = await fixture("activation-api"),
    b = await fixture("billing-service"),
    p = await fixture("provisioning-service");
  process.env.PROVISIONING_DELAY_MS = "1";
  try {
    const api = new ActivationService(a.db),
      bill = new BillingService(b.db),
      prov = new ProvisioningService(p.db);
    const created = await api.create({ customerId: "C-1", planId: "Digital" });
    assert.equal(created.status, "PENDING");
    const request = (await events(a))[0];
    parseEvent(request);
    await bill.receive(request);
    await bill.receive(request);
    await prov.receive(request);
    await prov.receive(request);
    assert.equal(
      (await b.query("SELECT * FROM billing_accounts")).rows.length,
      1,
    );
    assert.equal((await events(b)).length, 1);
    assert.equal((await events(p)).length, 1);
    await api.receive((await events(b))[0]);
    assert.equal((await api.get(created.activationId)).status, "IN_PROGRESS");
    await api.receive((await events(p))[0]);
    await api.receive((await events(p))[0]);
    const snapshot = await api.get(created.activationId);
    assert.equal(snapshot.status, "ACTIVE");
    assert.equal(snapshot.history.length, 4);
    assert.equal((await events(a)).length, 2);
  } finally {
    await Promise.all([a.pg.close(), b.pg.close(), p.pg.close()]);
  }
});
test("compensación antes y después del alta; nunca queda cuenta CREATED", async () => {
  for (const before of [false, true]) {
    const f = await fixture("billing-service");
    try {
      const bill = new BillingService(f.db),
        id = "act-" + before;
      const request = makeEvent(
        "ActivationRequested",
        "activation-api",
        id,
        "C-1",
        { planId: "Digital", simulateFailure: "none", channel: "web" },
      );
      const failure = makeEvent(
        "ActivationFailed",
        "activation-api",
        id,
        "C-1",
        { reason: "fallo" },
      );
      if (before) {
        await bill.receive(failure);
        await bill.receive(request);
      } else {
        await bill.receive(request);
        await bill.receive(failure);
      }
      await bill.receive(failure);
      await bill.receive(request);
      assert.equal(
        (await f.query("SELECT status FROM billing_accounts")).rows[0].status,
        "CANCELLED",
      );
      assert.deepEqual(
        (await events(f)).map((e) => e.eventType),
        ["BillingAccountCreated", "BillingAccountCancelled"],
      );
    } finally {
      await f.pg.close();
    }
  }
});
test("fallo provisioning: FAILED terminal, compensación e historial de resultado tardío", async () => {
  const a = await fixture("activation-api"),
    p = await fixture("provisioning-service");
  process.env.PROVISIONING_DELAY_MS = "1";
  try {
    const api = new ActivationService(a.db);
    const id = (
      await api.create({
        customerId: "C-2",
        planId: "Digital",
        simulateFailure: "provisioning",
      })
    ).activationId;
    await new ProvisioningService(p.db).receive((await events(a))[0]);
    await api.receive((await events(p))[0]);
    await api.receive(
      makeEvent("BillingAccountCreated", "billing-service", id, "C-2", {}),
    );
    const state = await api.get(id);
    assert.equal(state.status, "FAILED");
    assert.equal(state.history.length, 4);
    assert.equal(
      (await events(a)).filter((e) => e.eventType === "ActivationFailed")
        .length,
      1,
    );
  } finally {
    await Promise.all([a.pg.close(), p.pg.close()]);
  }
});
test("rollback: idempotencia no se confirma si falla el efecto", async () => {
  const f = await fixture("activation-api");
  try {
    const e = makeEvent(
      "BillingAccountCreated",
      "billing-service",
      "no-existe",
      "C-1",
      {},
    );
    await assert.rejects(() => new ActivationService(f.db).receive(e));
    assert.equal(
      (await f.query("SELECT * FROM processed_events")).rows.length,
      0,
    );
  } finally {
    await f.pg.close();
  }
});
test("CRM replay y loyalty son idempotentes; restricciones relacionales", async () => {
  const c = await fixture("crm-analytics-service"),
    l = await fixture("loyalty-service");
  try {
    const e = makeEvent(
      "ActivationCompleted",
      "activation-api",
      "act-1",
      "C-1",
      {},
    );
    const crm = new CrmService(c.db),
      loyalty = new LoyaltyService(l.db);
    for (let i = 0; i < 2; i++) {
      await crm.receive(e);
      await loyalty.receive(e);
    }
    assert.equal((await c.query("SELECT * FROM event_log")).rows.length, 1);
    assert.equal(
      (await l.query("SELECT points FROM loyalty_activations")).rows[0].points,
      100,
    );
    await assert.rejects(() =>
      l.query(
        "INSERT INTO loyalty_activations VALUES('other','C',-1,$1,now())",
        [e.eventId],
      ),
    );
  } finally {
    await Promise.all([c.pg.close(), l.pg.close()]);
  }
});
test("notificación: duplicado y recuperación tras SMTP aceptado antes del commit", async () => {
  const { NotificationService } = await import(
    "../../apps/notification-service/src/notification.service"
  );
  const f = await fixture("notification-service");
  try {
    const delivered = new Set<string>();
    let sends = 0,
      crash = true;
    const original = f.db.transaction.bind(f.db);
    (f.db as any).transaction = (fn: any) =>
      original(async (tx: any) => {
        const result = await fn(tx);
        if (crash) {
          crash = false;
          throw Error("crash antes de COMMIT");
        }
        return result;
      });
    const service = new NotificationService(f.db);
    (service as any).alreadyDelivered = async (id: string) => delivered.has(id);
    (service as any).mail = {
      sendMail: async ({ messageId }: any) => {
        sends++;
        delivered.add(messageId);
      },
      close: () => {},
    };
    const e = makeEvent(
      "ActivationCompleted",
      "activation-api",
      "act-mail",
      "C-1",
      {},
    );
    await assert.rejects(() => service.receive(e));
    assert.equal((await f.query("SELECT * FROM notifications")).rows.length, 0);
    await service.receive(e);
    await service.receive(e);
    assert.equal(sends, 1);
    assert.equal((await f.query("SELECT * FROM notifications")).rows.length, 1);
    service.onModuleDestroy();
  } finally {
    await f.pg.close();
  }
});
