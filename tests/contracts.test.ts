import { test } from "node:test";
import assert from "node:assert/strict";
import {
  aggregate,
  createActivationSchema,
  makeEvent,
  parseEvent,
  Topics,
} from "../libs/contracts";
import { retryThree } from "../libs/kafka-toolkit/retry";
test("contrato versionado: clave, topic, payload y versión validados", () => {
  const e = makeEvent("ActivationRequested", "activation-api", "act-1", "C-1", {
    planId: "Digital",
    channel: "web",
    simulateFailure: "none",
  });
  assert.equal(parseEvent(e, Topics.requested, "C-1").version, 1);
  for (const bad of [
    { ...e, version: 2 },
    { ...e, eventId: "invalido" },
    { ...e, payload: {} },
  ])
    assert.throws(() => parseEvent(bad));
  assert.throws(() => parseEvent(e, Topics.billing, "C-1"));
  assert.throws(() => parseEvent(e, Topics.requested, "otro"));
  assert.throws(() =>
    createActivationSchema.parse({ customerId: " ", planId: "X" }),
  );
});
test("saga: ambos órdenes de éxito finalizan una sola vez", () => {
  for (const types of [
    ["BillingAccountCreated", "ProvisioningCompleted"],
    ["ProvisioningCompleted", "BillingAccountCreated"],
  ] as const) {
    let s = aggregate("PENDING", null, null, types[0]);
    assert.equal(s.status, "IN_PROGRESS");
    s = aggregate(s.status, s.billing, s.provisioning, types[1]);
    assert.equal(s.status, "ACTIVE");
    assert.deepEqual(
      aggregate(s.status, s.billing, s.provisioning, "BillingFailed"),
      s,
    );
  }
});
test("saga: fallo final y resultados tardíos sin reabrir activación", () => {
  const s = aggregate("PENDING", null, null, "ProvisioningFailed");
  assert.equal(s.status, "FAILED");
  assert.deepEqual(
    aggregate(s.status, s.billing, s.provisioning, "BillingAccountCreated"),
    s,
  );
});
test("reintentos: tres fallos y backoff 1/2/4; éxito temprano no sigue", async () => {
  let attempts = 0;
  const delays: number[] = [];
  await assert.rejects(() =>
    retryThree(
      async () => {
        attempts++;
        throw new Error("técnico");
      },
      async (ms) => {
        delays.push(ms);
      },
    ),
  );
  assert.equal(attempts, 3);
  assert.deepEqual(delays, [1000, 2000, 4000]);
  attempts = 0;
  assert.equal(
    await retryThree(
      async () => {
        if (++attempts === 1) throw Error();
        return 42;
      },
      async () => {},
    ),
    42,
  );
  assert.equal(attempts, 2);
});
