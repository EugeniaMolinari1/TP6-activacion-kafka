import { OnModuleDestroy } from "@nestjs/common";
import {
  ConnectedSocket,
  MessageBody,
  SubscribeMessage,
  WebSocketGateway,
} from "@nestjs/websockets";
import { Socket } from "socket.io";
import { ActivationService } from "./activation.service";
// Durable history is the source of truth. Per-socket snapshots every 500ms also
// recover a crash between a DB commit and a socket emit and close the POST/join race.
@WebSocketGateway({
  cors: { origin: process.env.CORS_ORIGIN || "http://localhost:5173" },
})
export class EventsGateway implements OnModuleDestroy {
  private subscriptions = new Map<
    string,
    { socket: Socket; id: string; last: string }
  >();
  private busy = false;
  private timer = setInterval(() => {
    void this.push();
  }, 500);
  constructor(private readonly service: ActivationService) {}
  @SubscribeMessage("watch")
  async watch(@ConnectedSocket() socket: Socket, @MessageBody() id: unknown) {
    if (typeof id !== "string" || id.length > 100)
      return { error: "ID inválido" };
    const item = { socket, id, last: "" };
    this.subscriptions.set(socket.id, item);
    try {
      await this.send(item);
      return { ok: true };
    } catch {
      return { error: "Activación inexistente" };
    }
  }
  handleDisconnect(socket: Socket) {
    this.subscriptions.delete(socket.id);
  }
  private async send(item: { socket: Socket; id: string; last: string }) {
    const snapshot = await this.service.get(item.id),
      serialized = JSON.stringify(snapshot);
    if (serialized !== item.last) {
      item.socket.emit("activation.updated", snapshot);
      item.last = serialized;
    }
  }
  private async push() {
    if (this.busy) return;
    this.busy = true;
    try {
      await Promise.all(
        [...this.subscriptions.values()].map((item) =>
          this.send(item).catch(() => undefined),
        ),
      );
    } finally {
      this.busy = false;
    }
  }
  onModuleDestroy() {
    clearInterval(this.timer);
    this.subscriptions.clear();
  }
}
