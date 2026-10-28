import { Controller } from "@nestjs/common";
import {
  Ctx,
  EventPattern,
  KafkaContext,
  Payload,
} from "@nestjs/microservices";
import { Topics } from "../../../libs/contracts";
import { KafkaToolkit } from "../../../libs/kafka-toolkit/kafka";
import { CrmService } from "./crm.service";
@Controller()
export class AppController {
  constructor(
    private readonly service: CrmService,
    private readonly kafka: KafkaToolkit,
  ) {}
  @EventPattern(Topics.requested)
  requested(@Payload() e: unknown, @Ctx() ctx: KafkaContext) {
    return this.kafka.consume(e, ctx, (event) => this.service.receive(event));
  }
  @EventPattern(Topics.billing)
  billing(@Payload() e: unknown, @Ctx() ctx: KafkaContext) {
    return this.kafka.consume(e, ctx, (event) => this.service.receive(event));
  }
  @EventPattern(Topics.provisioning)
  provisioning(@Payload() e: unknown, @Ctx() ctx: KafkaContext) {
    return this.kafka.consume(e, ctx, (event) => this.service.receive(event));
  }
  @EventPattern(Topics.activation)
  activation(@Payload() e: unknown, @Ctx() ctx: KafkaContext) {
    return this.kafka.consume(e, ctx, (event) => this.service.receive(event));
  }
}
