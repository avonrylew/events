import { ListenerHandler } from "../core/ListenerHandler";

export type Listener = (...args: any[]) => void;

export interface ListenerOptions {
  ttl?: number;
  times?: number;
  priority?: number;
}

export interface ListenerMeta {
  priority: number;
  times: number;
  timeoutId: any;
  original: Listener | null;
  lastAccess: number;
  handle: ListenerHandler<any> | null;
}
