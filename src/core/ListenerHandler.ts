import { EventEmitter } from "./EventEmitter";
import type { ListenerOptions, EventKey, Listener, EventMap } from "../types";

export class ListenerHandler<T extends EventMap> {
  constructor(
    private emitter: EventEmitter<T>,
    private eventName: EventKey<T>,
    private listener: Listener
  ) {}

  disconnect(): this {
    this.emitter.removeListener(this.eventName, this.listener as any);
    return this;
  }

  reconnect(options?: ListenerOptions): this {
    this.emitter.addListener(this.eventName, this.listener as any, options);
    return this;
  }

  update(options: ListenerOptions): this {
    this.disconnect();
    this.reconnect(options);
    return this;
  }
}
