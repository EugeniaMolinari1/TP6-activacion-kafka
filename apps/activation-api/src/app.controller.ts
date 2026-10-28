import { Body, Controller, Get, HttpCode, Param, Post } from "@nestjs/common";
import {
  Ctx,
  EventPattern,
  KafkaContext,
  Payload,
} from "@nestjs/microservices";
import { Topics } from "../../../libs/contracts";
import { KafkaToolkit } from "../../../libs/kafka-toolkit/kafka";
import { ActivationService } from "./activation.service";
@Controller()
export class ActivationController {
  constructor(
    private readonly service: ActivationService,
    private readonly kafka: KafkaToolkit,
  ) {}
  @Post("activations") @HttpCode(202) create(@Body() body: unknown) {
    return this.service.create(body);
  }
  @Get("activations/:id") get(@Param("id") id: string) {
    return this.service.get(id);
  }
  @Get("health") health() {
    return { status: "ok" };
  }
  @EventPattern(Topics.billing)
  billing(@Payload() e: unknown, @Ctx() ctx: KafkaContext) {
    return this.kafka.consume(e, ctx, (event) => this.service.receive(event));
  }
  @EventPattern(Topics.provisioning)
  provisioning(@Payload() e: unknown, @Ctx() ctx: KafkaContext) {
    return this.kafka.consume(e, ctx, (event) => this.service.receive(event));
  }
}
