import { Controller } from "@nestjs/common";
import {
  Ctx,
  EventPattern,
  KafkaContext,
  Payload,
} from "@nestjs/microservices";
import { Topics } from "../../../libs/contracts";
import { KafkaToolkit } from "../../../libs/kafka-toolkit/kafka";
import { NotificationService } from "./notification.service";
@Controller()
export class AppController {
  constructor(
    private readonly service: NotificationService,
    private readonly kafka: KafkaToolkit,
  ) {}
  @EventPattern(Topics.activation)
  activation(@Payload() e: unknown, @Ctx() ctx: KafkaContext) {
    return this.kafka.consume(e, ctx, (event) => this.service.receive(event));
  }
}
