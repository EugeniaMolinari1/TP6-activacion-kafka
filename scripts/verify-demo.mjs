// Run after docker compose up --build -d, from the repository root.
import assert from "node:assert/strict";
import { setTimeout as sleep } from "node:timers/promises";
import { randomUUID } from "node:crypto";
import { Kafka } from "kafkajs";
import { io } from "socket.io-client";
const API = process.env.API_URL || "http://localhost:3000";
const MAIL = process.env.MAILHOG_API_URL || "http://localhost:8025";
const kafka = new Kafka({
  clientId: "tp6-verification",
  brokers: [process.env.KAFKA_BROKERS || "localhost:9092"],
});
const producer = kafka.producer();
const socket = io(API, { reconnection: false, timeout: 10000 });
const updates = [];
socket.on("activation.updated", (a) => updates.push(a));
async function eventually(fn, label, ms = 45000) {
  const end = Date.now() + ms;
  while (Date.now() < end) {
    if (await fn()) return;
    await sleep(500);
  }
  throw Error(`Timeout: ${label}`);
}
async function read(id) {
  const r = await fetch(`${API}/activations/${id}`);
  assert.equal(r.status, 200);
  return r.json();
}
async function create(mode) {
  const r = await fetch(`${API}/activations`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      customerId: "VERIFY-" + randomUUID().slice(0, 8),
      planId: "Plan Digital",
      simulateFailure: mode,
    }),
  });
  assert.equal(r.status, 202);
  const a = await r.json();
  assert.equal(a.status, "PENDING");
  socket.emit("watch", a.activationId);
  return a.activationId;
}
async function mailsFor(id) {
  let found = [];
  for (let start = 0; ; start += 100) {
    const r = await fetch(`${MAIL}/api/v2/messages?start=${start}&limit=100`);
    assert.ok(r.ok);
    const d = await r.json();
    found.push(
      ...d.items.filter((m) =>
        (m.Content.Headers["Message-ID"] || []).some((v) => v.includes(id)),
      ),
    );
    if (start + 100 >= d.total) return found;
  }
}
let dlqConsumer;
try {
  await eventually(async () => socket.connected, "conexión Socket.IO", 15000);
  await producer.connect();
  for (const mode of ["none", "provisioning", "billing"]) {
    const id = await create(mode),
      expected = mode === "none" ? "ACTIVE" : "FAILED";
    await eventually(
      async () => (await read(id)).status === expected,
      `estado ${expected}`,
    );
    if (mode === "provisioning")
      await eventually(
        async () =>
          (await read(id)).history.some(
            (e) => e.eventType === "BillingAccountCancelled",
          ),
        "compensación",
      );
    await eventually(
      async () => (await mailsFor(id)).length === 1,
      "email único",
    );
    await eventually(
      async () =>
        updates.some((a) => a.activationId === id && a.status === expected),
      "actualización WebSocket",
    );
    const a = await read(id),
      requested = a.history.find((e) => e.eventType === "ActivationRequested");
    await producer.send({
      topic: "activation.requested",
      messages: [{ key: a.customerId, value: JSON.stringify(requested) }],
    });
    const final = a.history.find((e) =>
      ["ActivationCompleted", "ActivationFailed"].includes(e.eventType),
    );
    await producer.send({
      topic: "activation.events",
      messages: [{ key: a.customerId, value: JSON.stringify(final) }],
    });
    await sleep(3000);
    assert.equal((await mailsFor(id)).length, 1);
    assert.equal((await read(id)).history.length, a.history.length);
    console.log(`OK ${mode}: ${id}; historial, WebSocket, email y duplicados`);
  }
  const invalidId = randomUUID();
  let seen = false;
  dlqConsumer = kafka.consumer({ groupId: "verify-dlq-" + randomUUID() });
  await dlqConsumer.connect();
  await dlqConsumer.subscribe({
    topic: "activation.requested.dlq",
    fromBeginning: true,
  });
  await dlqConsumer.run({
    eachMessage: async ({ message }) => {
      if (message.value?.toString().includes(invalidId)) {
        assert.equal(message.headers.attempts.toString(), "3");
        seen = true;
      }
    },
  });
  await producer.send({
    topic: "activation.requested",
    messages: [
      {
        key: "INVALID",
        value: JSON.stringify({ probe: invalidId, version: 999 }),
      },
    ],
  });
  await eventually(async () => seen, "mensaje inválido en DLQ");
  const next = await create("none");
  await eventually(
    async () => (await read(next)).status === "ACTIVE",
    "flujo posterior a DLQ",
  );
  console.log(
    "OK DLQ y continuidad de procesamiento. Escenarios 3/4/5: seguir docs/demo-script.md.",
  );
} finally {
  socket.disconnect();
  if (dlqConsumer) await dlqConsumer.disconnect();
  await producer.disconnect();
}
