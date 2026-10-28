import "reflect-metadata";
import { test } from "node:test";
import assert from "node:assert/strict";
import { KafkaToolkit } from "../libs/kafka-toolkit/kafka";
import { KafkaRetriableException } from "@nestjs/microservices";
import { makeEvent, Topics } from "../libs/contracts";
function context(raw: string, key = "C-1") {
  const commits: any[] = [];
  const ctx = {
    getMessage: () => ({
      key: Buffer.from(key),
      value: Buffer.from(raw),
      offset: "9007199254740993",
    }),
    getTopic: () => Topics.requested,
    getPartition: () => 1,
    getHeartbeat: () => async () => {},
    getConsumer: () => ({
      commitOffsets: async (x: any) => {
        commits.push(x);
      },
    }),
  };
  return { ctx, commits };
}
test("offset confirmado después del efecto, sin perder precisión bigint", async () => {
  const e = makeEvent("ActivationRequested", "activation-api", "act-1", "C-1", {
    planId: "Digital",
    channel: "web",
    simulateFailure: "none",
  });
  const f = context(JSON.stringify(e));
  let processed = false;
  await KafkaToolkit.prototype.consume.call(
    {} as any,
    Buffer.from(JSON.stringify(e)),
    f.ctx as any,
    async () => {
      assert.equal(f.commits.length, 0);
      processed = true;
    },
  );
  assert.equal(processed, true);
  assert.equal(f.commits[0][0].offset, "9007199254740994");
});
test("mensaje inválido a DLQ con original y headers; siguiente mensaje continúa", async () => {
  const f = context("{invalid");
  const publications: any[] = [];
  const fake = {
    publish: async (...args: any[]) => {
      assert.equal(f.commits.length, 0);
      publications.push(args);
    },
  };
  await KafkaToolkit.prototype.consume.call(
    fake as any,
    "{invalid",
    f.ctx as any,
    async () => assert.fail("no debe ejecutar negocio"),
  );
  assert.equal(publications[0][0], "activation.requested.dlq");
  assert.equal(publications[0][2], "{invalid");
  assert.equal(publications[0][3].attempts, "3");
  assert.equal(f.commits.length, 1);
});
test("DLQ caída: no se confirma offset; Kafka podrá reentregar", async () => {
  const f = context("bad");
  await assert.rejects(
    () =>
      KafkaToolkit.prototype.consume.call(
        {
          publish: async () => {
            throw Error("offline");
          },
        } as any,
        "bad",
        f.ctx as any,
        async () => {},
      ),
    KafkaRetriableException,
  );
  assert.equal(f.commits.length, 0);
});
