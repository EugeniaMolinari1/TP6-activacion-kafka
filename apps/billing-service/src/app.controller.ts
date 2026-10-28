import { Controller } from "@nestjs/common";
import {
  Ctx,
  EventPattern,
  KafkaContext,
  Payload,
} from "@nestjs/microservices";
import { Topics } from "../../../libs/contracts";
import { KafkaToolkit } from "../../../libs/kafka-toolkit/kafka";
import { BillingService } from "./billing.service";
@Controller()
export class AppController {
  constructor(
    private readonly service: BillingService,
    private readonly kafka: KafkaToolkit,
  ) {}
  @EventPattern(Topics.requested)
  requested(@Payload() e: unknown, @Ctx() ctx: KafkaContext) {
    return this.kafka.consume(e, ctx, (event) => this.service.receive(event));
  }
  @EventPattern(Topics.activation)
  activation(@Payload() e: unknown, @Ctx() ctx: KafkaContext) {
    return this.kafka.consume(e, ctx, (event) => this.service.receive(event));
  }
}
