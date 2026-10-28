import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync, existsSync } from "node:fs";
import { parse } from "yaml";
test("Compose: infraestructura, cinco apps, loyalty perfil, escala y rutas existentes", () => {
  const c = parse(readFileSync("docker-compose.yml", "utf8"));
  for (const name of [
    "postgres",
    "kafka",
    "kafka-init",
    "kafka-ui",
    "mailhog",
    "activation-api",
    "billing-service",
    "provisioning-service",
    "notification-service",
    "crm-analytics-service",
    "loyalty-service",
    "demo-ui",
  ])
    assert.ok(c.services[name]);
  assert.equal(
    c.services.kafka.environment.KAFKA_PROCESS_ROLES,
    "broker,controller",
  );
  assert.ok(c.services.postgres.healthcheck);
  assert.ok(c.services["activation-api"].healthcheck);
  assert.equal(c.services["billing-service"].container_name, undefined);
  assert.equal(c.services["billing-service"].ports, undefined);
  assert.deepEqual(c.services["loyalty-service"].profiles, ["loyalty"]);
  for (const service of Object.values(c.services) as any[])
    if (service.build) {
      assert.ok(existsSync(service.build.context));
      if (service.build.dockerfile)
        assert.ok(existsSync(service.build.dockerfile));
    }
});
