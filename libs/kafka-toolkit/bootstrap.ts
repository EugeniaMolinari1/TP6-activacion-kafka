import "reflect-metadata";
import { rmSync, writeFileSync } from "node:fs";
import { Module } from "@nestjs/common";
import { NestFactory } from "@nestjs/core";
import { MicroserviceOptions, Transport } from "@nestjs/microservices";
import { Database } from "./database";
import { KafkaToolkit } from "./kafka";
export async function bootstrap(service: string, group: string) {
  rmSync("/tmp/tp6-ready", { force: true });
  process.env.SERVICE_NAME = service;
  process.env.KAFKA_GROUP_ID ||= group;
  const controllerModule = await import(
    `../../apps/${service}/src/app.controller`
  );
  const serviceFiles: Record<string, [string, string]> = {
    "activation-api": ["activation", "ActivationService"],
    "billing-service": ["billing", "BillingService"],
    "provisioning-service": ["provisioning", "ProvisioningService"],
    "notification-service": ["notification", "NotificationService"],
    "crm-analytics-service": ["crm", "CrmService"],
    "loyalty-service": ["loyalty", "LoyaltyService"],
  };
  const [file, cls] = serviceFiles[service];
  const providerModule = await import(
    `../../apps/${service}/src/${file}.service`
  );
  const providers: any[] = [Database, KafkaToolkit, providerModule[cls]];
  if (service === "activation-api")
    providers.push(
      (await import("../../apps/activation-api/src/events.gateway"))
        .EventsGateway,
    );
  @Module({
    controllers: [
      controllerModule.ActivationController || controllerModule.AppController,
    ],
    providers,
  })
  class AppModule {}
  const options: MicroserviceOptions = {
    transport: Transport.KAFKA,
    options: {
      client: {
        brokers: (process.env.KAFKA_BROKERS || "localhost:9092").split(","),
        clientId: process.env.KAFKA_CLIENT_ID || service,
        retry: { retries: 15 },
      },
      consumer: { groupId: process.env.KAFKA_GROUP_ID, sessionTimeout: 30000 },
      postfixId: "",
      parser: { keepBinary: true },
      subscribe: { fromBeginning: true },
      run: { autoCommit: false, partitionsConsumedConcurrently: 3 },
    },
  };
  const app = await NestFactory.create(AppModule);
  app.enableShutdownHooks();
  await app.get(Database).init(service);
  await app.get(KafkaToolkit).connect();
  app.connectMicroservice(options);
  await app.startAllMicroservices();
  if (service === "activation-api") {
    app.enableCors({
      origin: process.env.CORS_ORIGIN || "http://localhost:5173",
    });
    await app.listen(3000, "0.0.0.0");
  } else {
    await app.init();
  }
  writeFileSync("/tmp/tp6-ready", "ready");
}
