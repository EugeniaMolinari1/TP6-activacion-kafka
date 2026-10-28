import { randomUUID } from "node:crypto";
import { z } from "zod";
export const Topics = {
  requested: "activation.requested",
  billing: "billing.events",
  provisioning: "provisioning.events",
  activation: "activation.events",
} as const;
export const eventTopics = {
  ActivationRequested: Topics.requested,
  BillingAccountCreated: Topics.billing,
  BillingFailed: Topics.billing,
  BillingAccountCancelled: Topics.billing,
  ProvisioningCompleted: Topics.provisioning,
  ProvisioningFailed: Topics.provisioning,
  ActivationCompleted: Topics.activation,
  ActivationFailed: Topics.activation,
} as const;
export type EventType = keyof typeof eventTopics;
export const createActivationSchema = z
  .object({
    customerId: z.string().trim().min(1).max(100),
    planId: z.string().trim().min(1).max(100),
    channel: z.string().trim().min(1).max(30).default("web"),
    simulateFailure: z
      .enum(["none", "billing", "provisioning"])
      .default("none"),
  })
  .strict();
export type ActivationInput = z.infer<typeof createActivationSchema>;
const base = z.object({
  eventId: z.string().uuid(),
  eventType: z.enum(Object.keys(eventTopics) as [EventType, ...EventType[]]),
  version: z.literal(1),
  occurredAt: z.string().datetime(),
  correlationId: z.string().min(1).max(100),
  customerId: z.string().min(1).max(100),
  source: z.string().min(1),
  payload: z.record(z.unknown()),
});
export type BusinessEvent = z.infer<typeof base>;
export function parseEvent(
  value: unknown,
  topic?: string,
  key?: string,
): BusinessEvent {
  const e = base.parse(value);
  if (topic && eventTopics[e.eventType] !== topic)
    throw new Error("Evento incompatible con topic");
  if (key !== undefined && key !== e.customerId)
    throw new Error("Kafka key debe ser customerId");
  if (e.eventType === "ActivationRequested")
    createActivationSchema.parse({ customerId: e.customerId, ...e.payload });
  if (e.eventType.endsWith("Failed") && typeof e.payload.reason !== "string")
    throw new Error("Falta reason");
  return e;
}
export function makeEvent(
  type: EventType,
  source: string,
  correlationId: string,
  customerId: string,
  payload: Record<string, unknown>,
): BusinessEvent {
  return {
    eventId: randomUUID(),
    eventType: type,
    source,
    correlationId,
    customerId,
    payload,
    version: 1,
    occurredAt: new Date().toISOString(),
  };
}
export type Status = "PENDING" | "IN_PROGRESS" | "ACTIVE" | "FAILED";
export function aggregate(
  status: Status,
  billing: string | null,
  provisioning: string | null,
  type: EventType,
) {
  if (
    status === "ACTIVE" ||
    status === "FAILED" ||
    type === "BillingAccountCancelled"
  )
    return { status, billing, provisioning };
  if (type === "BillingAccountCreated") billing = "OK";
  if (type === "ProvisioningCompleted") provisioning = "OK";
  if (type === "BillingFailed") billing = "FAILED";
  if (type === "ProvisioningFailed") provisioning = "FAILED";
  status =
    billing === "FAILED" || provisioning === "FAILED"
      ? "FAILED"
      : billing === "OK" && provisioning === "OK"
        ? "ACTIVE"
        : "IN_PROGRESS";
  return { status, billing, provisioning };
}
