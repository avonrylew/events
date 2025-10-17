export type EventMap = Record<string | symbol, (...args: any[]) => void>;
export type EventKey<T extends EventMap> = keyof T;
export type EventArgs<T extends EventMap, K extends EventKey<T>> = Parameters<T[K]>;

export interface ListenerOptions {
  ttl?: number;
  times?: number;
  priority?: number;
}

type Listener = (...args: any[]) => void;

const ARGS_POOL: any[][] = [];
const MAX_POOL_SIZE = 20;
let poolIndex = 0;

function getArgs(count: number): any[] {
  if (poolIndex > 0) {
    const args = ARGS_POOL[--poolIndex];
    args.length = count;
    return args;
  }
  return new Array(count);
}

function releaseArgs(args: any[]): void {
  if (args.length <= 10 && poolIndex < MAX_POOL_SIZE) {
    for (let i = 0; i < args.length; i++) args[i] = undefined;
    args.length = 0;
    ARGS_POOL[poolIndex++] = args;
  }
}

interface ListenerMeta {
  priority: number;
  times: number;
  timeoutId: any;
  original: Listener | null;
}

class EventStream<T extends any[]> {
  private handlers: Array<(...args: T) => void> = [];
  private registered: Array<(...args: T) => void> = [];
  private emitter: EventEmitter<any>;
  private event: string | symbol;
  private disposed = false;

  constructor(emitter: EventEmitter<any>, event: string | symbol) {
    this.emitter = emitter;
    this.event = event;
  }

  map<R>(fn: (...args: T) => R): EventStream<[R]> {
    if (this.disposed) throw new Error('Stream disposed');
    const stream = new EventStream<[R]>(this.emitter, this.event);
    const handler = (...args: T): void => {
      if (stream.disposed) return;
      const result = fn(...args);
      const h = stream.handlers.slice();
      for (let i = 0; i < h.length; i++) h[i](result);
    };
    this.handlers.push(handler);
    this.registered.push(handler);
    this.emitter.addListener(this.event as any, handler as any);
    return stream;
  }

  filter(pred: (...args: T) => boolean): EventStream<T> {
    if (this.disposed) throw new Error('Stream disposed');
    const stream = new EventStream<T>(this.emitter, this.event);
    const handler = (...args: T): void => {
      if (stream.disposed || !pred(...args)) return;
      const h = stream.handlers.slice();
      for (let i = 0; i < h.length; i++) h[i](...args);
    };
    this.handlers.push(handler);
    this.registered.push(handler);
    this.emitter.addListener(this.event as any, handler as any);
    return stream;
  }

  debounce(ms: number): EventStream<T> {
    if (this.disposed) throw new Error('Stream disposed');
    const stream = new EventStream<T>(this.emitter, this.event);
    let tid: any = 0;
    const handler = (...args: T): void => {
      if (stream.disposed) return;
      if (tid) clearTimeout(tid);
      tid = setTimeout(() => {
        tid = 0;
        if (stream.disposed) return;
        const h = stream.handlers.slice();
        for (let i = 0; i < h.length; i++) h[i](...args);
      }, ms);
    };
    this.handlers.push(handler);
    this.registered.push(handler);
    this.emitter.addListener(this.event as any, handler as any);
    return stream;
  }

  throttle(ms: number): EventStream<T> {
    if (this.disposed) throw new Error('Stream disposed');
    const stream = new EventStream<T>(this.emitter, this.event);
    let last = 0;
    const handler = (...args: T): void => {
      if (stream.disposed) return;
      const now = Date.now();
      if (now - last < ms) return;
      last = now;
      const h = stream.handlers.slice();
      for (let i = 0; i < h.length; i++) h[i](...args);
    };
    this.handlers.push(handler);
    this.registered.push(handler);
    this.emitter.addListener(this.event as any, handler as any);
    return stream;
  }

  take(n: number): EventStream<T> {
    if (this.disposed) throw new Error('Stream disposed');
    const stream = new EventStream<T>(this.emitter, this.event);
    let count = 0;
    const handler = (...args: T): void => {
      if (stream.disposed || count >= n) return;
      count++;
      const h = stream.handlers.slice();
      for (let i = 0; i < h.length; i++) h[i](...args);
      if (count === n) {
        this.emitter.removeListener(this.event as any, handler as any);
        const idx = this.registered.indexOf(handler);
        if (idx !== -1) this.registered.splice(idx, 1);
      }
    };
    this.handlers.push(handler);
    this.registered.push(handler);
    this.emitter.addListener(this.event as any, handler as any);
    return stream;
  }

  skip(n: number): EventStream<T> {
    if (this.disposed) throw new Error('Stream disposed');
    const stream = new EventStream<T>(this.emitter, this.event);
    let count = 0;
    const handler = (...args: T): void => {
      if (stream.disposed || count++ < n) return;
      const h = stream.handlers.slice();
      for (let i = 0; i < h.length; i++) h[i](...args);
    };
    this.handlers.push(handler);
    this.registered.push(handler);
    this.emitter.addListener(this.event as any, handler as any);
    return stream;
  }

  subscribe(cb: (...args: T) => void): () => void {
    if (this.disposed) throw new Error('Stream disposed');
    this.handlers.push(cb);
    return (): void => {
      const idx = this.handlers.indexOf(cb);
      if (idx !== -1) this.handlers.splice(idx, 1);
    };
  }

  dispose(): void {
    if (this.disposed) return;
    this.disposed = true;
    for (let i = 0; i < this.registered.length; i++) {
      this.emitter.removeListener(this.event as any, this.registered[i] as any);
    }
    this.handlers.length = 0;
    this.registered.length = 0;
    (this as any).emitter = null;
  }
}

export class EventEmitter<T extends EventMap> {
  private events: any = {};
  private onceEvents: any = {};
  private wildcards: Listener[] = [];
  private pipes: EventEmitter<any>[] = [];
  private metadata: WeakMap<Listener, ListenerMeta> | null = null;
  private maxListeners = 10;

  addListener<K extends EventKey<T>>(eventName: K, listener: T[K], options?: ListenerOptions): this {
    const key = eventName as string | symbol;
    const fn = listener as Listener;
    
    if (!options) {
      const current = this.events[key];
      if (!current) {
        this.events[key] = fn;
      } else if (typeof current === 'function') {
        this.events[key] = [current, fn];
      } else {
        current[current.length] = fn;
      }
      return this;
    }

    const priority = options.priority ?? 0;
    const times = options.times ?? -1;
    let finalListener = fn;
    let originalListener: Listener | null = null;

    if (times > 0) {
      originalListener = fn;
      const self = this;
      finalListener = function(...args: any[]): any {
        const meta = self.metadata?.get(finalListener);
        if (meta && --meta.times === 0) {
          self.removeListener(eventName, finalListener as T[K]);
        }
        return originalListener!(...args);
      } as Listener;
    }

    if (!this.metadata) this.metadata = new WeakMap();
    
    let tid: any = 0;
    if (options.ttl && options.ttl > 0) {
      tid = setTimeout(() => {
        this.removeListener(eventName, finalListener as T[K]);
      }, options.ttl);
    }

    this.metadata.set(finalListener, { priority, times, timeoutId: tid, original: originalListener });

    const current = this.events[key];
    
    if (priority !== 0) {
      let arr: Listener[];
      if (!current) {
        arr = [];
        this.events[key] = arr;
      } else if (typeof current === 'function') {
        arr = [current];
        this.events[key] = arr;
      } else {
        arr = current;
      }

      if (arr.length > 10) {
        let left = 0, right = arr.length;
        while (left < right) {
          const mid = (left + right) >>> 1;
          const midMeta = this.metadata.get(arr[mid]);
          if (priority > (midMeta?.priority ?? 0)) {
            right = mid;
          } else {
            left = mid + 1;
          }
        }
        arr.splice(left, 0, finalListener);
      } else {
        let inserted = false;
        for (let i = 0, len = arr.length; i < len; i++) {
          const meta = this.metadata.get(arr[i]);
          if (priority > (meta?.priority ?? 0)) {
            arr.splice(i, 0, finalListener);
            inserted = true;
            break;
          }
        }
        if (!inserted) arr[arr.length] = finalListener;
      }
    } else {
      if (!current) {
        this.events[key] = finalListener;
      } else if (typeof current === 'function') {
        this.events[key] = [current, finalListener];
      } else {
        current[current.length] = finalListener;
      }
    }

    return this;
  }

  on<K extends EventKey<T>>(eventName: K, listener: T[K], options?: ListenerOptions): this {
    return this.addListener(eventName, listener, options);
  }

  stream<K extends EventKey<T>>(eventName: K): EventStream<EventArgs<T, K>> {
    return new EventStream<EventArgs<T, K>>(this, eventName as string | symbol);
  }

  once<K extends EventKey<T>>(eventName: K, listener: T[K], options?: ListenerOptions): this {
    const key = eventName as string | symbol;
    const fn = listener as Listener;
    
    if (!options) {
      const current = this.onceEvents[key];
      if (!current) {
        this.onceEvents[key] = fn;
      } else if (typeof current === 'function') {
        this.onceEvents[key] = [current, fn];
      } else {
        current[current.length] = fn;
      }
      return this;
    }

    const priority = options.priority ?? 0;
    
    if (!this.metadata) this.metadata = new WeakMap();
    
    let tid: any = 0;
    if (options.ttl && options.ttl > 0) {
      tid = setTimeout(() => {
        this.removeListener(eventName, listener);
      }, options.ttl);
    }

    this.metadata.set(fn, { priority, times: -1, timeoutId: tid, original: null });

    const current = this.onceEvents[key];
    
    if (priority !== 0) {
      let arr: Listener[];
      if (!current) {
        arr = [];
        this.onceEvents[key] = arr;
      } else if (typeof current === 'function') {
        arr = [current];
        this.onceEvents[key] = arr;
      } else {
        arr = current;
      }

      let inserted = false;
      for (let i = 0, len = arr.length; i < len; i++) {
        const meta = this.metadata.get(arr[i]);
        if (priority > (meta?.priority ?? 0)) {
          arr.splice(i, 0, fn);
          inserted = true;
          break;
        }
      }
      if (!inserted) arr[arr.length] = fn;
    } else {
      if (!current) {
        this.onceEvents[key] = fn;
      } else if (typeof current === 'function') {
        this.onceEvents[key] = [current, fn];
      } else {
        current[current.length] = fn;
      }
    }

    return this;
  }

  private cleanupMeta(fn: Listener): void {
    if (!this.metadata) return;
    const meta = this.metadata.get(fn);
    if (meta?.timeoutId) clearTimeout(meta.timeoutId);
    this.metadata.delete(fn);
  }

  removeListener<K extends EventKey<T>>(eventName?: K, listener?: T[K]): this {
    if (!eventName) {
      if (this.metadata) {
        for (const key in this.events) {
          const v = this.events[key];
          if (typeof v === 'function') {
            this.cleanupMeta(v);
          } else if (v) {
            for (let i = 0, len = v.length; i < len; i++) this.cleanupMeta(v[i]);
          }
        }
        for (const key in this.onceEvents) {
          const v = this.onceEvents[key];
          if (typeof v === 'function') {
            this.cleanupMeta(v);
          } else if (v) {
            for (let i = 0, len = v.length; i < len; i++) this.cleanupMeta(v[i]);
          }
        }
      }
      this.events = {};
      this.onceEvents = {};
      return this;
    }

    const key = eventName as string | symbol;

    if (!listener) {
      if (this.metadata) {
        const el = this.events[key];
        if (el) {
          if (typeof el === 'function') {
            this.cleanupMeta(el);
          } else {
            for (let i = 0, len = el.length; i < len; i++) this.cleanupMeta(el[i]);
          }
        }
        const ol = this.onceEvents[key];
        if (ol) {
          if (typeof ol === 'function') {
            this.cleanupMeta(ol);
          } else {
            for (let i = 0, len = ol.length; i < len; i++) this.cleanupMeta(ol[i]);
          }
        }
      }
      this.events[key] = this.onceEvents[key] = void 0;
      return this;
    }

    const fn = listener as Listener;

    let l = this.events[key];
    if (l) {
      if (l === fn) {
        this.cleanupMeta(fn);
        this.events[key] = void 0;
      } else if (typeof l !== 'function') {
        const i = l.indexOf(fn);
        if (i > -1) {
          this.cleanupMeta(fn);
          l.splice(i, 1);
          if (l.length === 1) this.events[key] = l[0];
          else if (!l.length) this.events[key] = void 0;
        } else if (this.metadata) {
          for (let i = l.length - 1; i >= 0; i--) {
            const meta = this.metadata.get(l[i]);
            if (meta?.original === fn) {
              this.cleanupMeta(l[i]);
              l.splice(i, 1);
              if (l.length === 1) this.events[key] = l[0];
              else if (!l.length) this.events[key] = void 0;
              break;
            }
          }
        }
      }
    }

    l = this.onceEvents[key];
    if (l) {
      if (l === fn) {
        this.cleanupMeta(fn);
        this.onceEvents[key] = void 0;
      } else if (typeof l !== 'function') {
        const i = l.indexOf(fn);
        if (i > -1) {
          this.cleanupMeta(fn);
          l.splice(i, 1);
          if (l.length === 1) this.onceEvents[key] = l[0];
          else if (!l.length) this.onceEvents[key] = void 0;
        } else if (this.metadata) {
          for (let i = l.length - 1; i >= 0; i--) {
            const meta = this.metadata.get(l[i]);
            if (meta?.original === fn) {
              this.cleanupMeta(l[i]);
              l.splice(i, 1);
              if (l.length === 1) this.onceEvents[key] = l[0];
              else if (!l.length) this.onceEvents[key] = void 0;
              break;
            }
          }
        }
      }
    }

    return this;
  }

  off<K extends EventKey<T>>(eventName?: K, listener?: T[K]): this {
    return this.removeListener(eventName, listener);
  }

  emit<K extends EventKey<T>>(eventName: K, a?: any, b?: any, c?: any, d?: any, e?: any): boolean {
    const key = eventName as string | symbol;
    const l = this.events[key];
    
    if (typeof l === 'function') {
      const al = arguments.length - 1;
      switch (al) {
        case 0: l(); break;
        case 1: l(a); break;
        case 2: l(a, b); break;
        case 3: l(a, b, c); break;
        case 4: l(a, b, c, d); break;
        case 5: l(a, b, c, d, e); break;
        default: {
          const args = getArgs(al);
          for (let i = 0; i < al; i++) args[i] = arguments[i + 1];
          l(...args);
          releaseArgs(args);
        }
      }
      
      const ol = this.onceEvents[key];
      if (ol) {
        if (typeof ol === 'function') {
          this.cleanupMeta(ol);
          this.onceEvents[key] = void 0;
          switch (al) {
            case 0: ol(); break;
            case 1: ol(a); break;
            case 2: ol(a, b); break;
            case 3: ol(a, b, c); break;
            case 4: ol(a, b, c, d); break;
            case 5: ol(a, b, c, d, e); break;
            default: {
              const args = getArgs(al);
              for (let i = 0; i < al; i++) args[i] = arguments[i + 1];
              ol(...args);
              releaseArgs(args);
            }
          }
        } else {
          const n = ol.length;
          if (this.metadata) {
            for (let i = 0; i < n; i++) this.cleanupMeta(ol[i]);
          }
          this.onceEvents[key] = void 0;
          switch (al) {
            case 0:
              for (let i = 0; i < n; i++) ol[i]();
              break;
            case 1:
              for (let i = 0; i < n; i++) ol[i](a);
              break;
            case 2:
              for (let i = 0; i < n; i++) ol[i](a, b);
              break;
            case 3:
              for (let i = 0; i < n; i++) ol[i](a, b, c);
              break;
            case 4:
              for (let i = 0; i < n; i++) ol[i](a, b, c, d);
              break;
            case 5:
              for (let i = 0; i < n; i++) ol[i](a, b, c, d, e);
              break;
            default: {
              const args = getArgs(al);
              for (let j = 0; j < al; j++) args[j] = arguments[j + 1];
              for (let i = 0; i < n; i++) ol[i](...args);
              releaseArgs(args);
            }
          }
        }
      }
      return true;
    }
    
    const ol = this.onceEvents[key];
    const w = this.wildcards;
    const p = this.pipes;
    
    if (!l && !ol && !w.length && !p.length) return false;
    
    const al = arguments.length - 1;
    
    if (l) {
      const n = l.length;
      switch (al) {
        case 0:
          for (let i = 0; i < n; i++) l[i]();
          break;
        case 1:
          for (let i = 0; i < n; i++) l[i](a);
          break;
        case 2:
          for (let i = 0; i < n; i++) l[i](a, b);
          break;
        case 3:
          for (let i = 0; i < n; i++) l[i](a, b, c);
          break;
        case 4:
          for (let i = 0; i < n; i++) l[i](a, b, c, d);
          break;
        case 5:
          for (let i = 0; i < n; i++) l[i](a, b, c, d, e);
          break;
        default: {
          const args = getArgs(al);
          for (let j = 0; j < al; j++) args[j] = arguments[j + 1];
          for (let i = 0; i < n; i++) l[i](...args);
          releaseArgs(args);
        }
      }
    }
    
    if (ol) {
      if (typeof ol === 'function') {
        this.cleanupMeta(ol);
        this.onceEvents[key] = void 0;
        switch (al) {
          case 0: ol(); break;
          case 1: ol(a); break;
          case 2: ol(a, b); break;
          case 3: ol(a, b, c); break;
          case 4: ol(a, b, c, d); break;
          case 5: ol(a, b, c, d, e); break;
          default: {
            const args = getArgs(al);
            for (let i = 0; i < al; i++) args[i] = arguments[i + 1];
            ol(...args);
            releaseArgs(args);
          }
        }
      } else {
        const n = ol.length;
        if (this.metadata) {
          for (let i = 0; i < n; i++) this.cleanupMeta(ol[i]);
        }
        this.onceEvents[key] = void 0;
        switch (al) {
          case 0:
            for (let i = 0; i < n; i++) ol[i]();
            break;
          case 1:
            for (let i = 0; i < n; i++) ol[i](a);
            break;
          case 2:
            for (let i = 0; i < n; i++) ol[i](a, b);
            break;
          case 3:
            for (let i = 0; i < n; i++) ol[i](a, b, c);
            break;
          case 4:
            for (let i = 0; i < n; i++) ol[i](a, b, c, d);
            break;
          case 5:
            for (let i = 0; i < n; i++) ol[i](a, b, c, d, e);
            break;
          default: {
            const args = getArgs(al);
            for (let j = 0; j < al; j++) args[j] = arguments[j + 1];
            for (let i = 0; i < n; i++) ol[i](...args);
            releaseArgs(args);
          }
        }
      }
    }
    
    const wl = w.length;
    if (wl) {
      switch (al) {
        case 0:
          for (let i = 0; i < wl; i++) w[i](eventName);
          break;
        case 1:
          for (let i = 0; i < wl; i++) w[i](eventName, a);
          break;
        case 2:
          for (let i = 0; i < wl; i++) w[i](eventName, a, b);
          break;
        case 3:
          for (let i = 0; i < wl; i++) w[i](eventName, a, b, c);
          break;
        case 4:
          for (let i = 0; i < wl; i++) w[i](eventName, a, b, c, d);
          break;
        case 5:
          for (let i = 0; i < wl; i++) w[i](eventName, a, b, c, d, e);
          break;
        default: {
          const args = getArgs(al + 1);
          args[0] = eventName;
          for (let j = 0; j < al; j++) args[j + 1] = arguments[j + 1];
          for (let i = 0; i < wl; i++) w[i](...args);
          releaseArgs(args);
        }
      }
    }
    
    const pl = p.length;
    if (pl) {
      switch (al) {
        case 0:
          for (let i = 0; i < pl; i++) p[i].emit(eventName);
          break;
        case 1:
          for (let i = 0; i < pl; i++) p[i].emit(eventName, a);
          break;
        case 2:
          for (let i = 0; i < pl; i++) p[i].emit(eventName, a, b);
          break;
        case 3:
          for (let i = 0; i < pl; i++) p[i].emit(eventName, a, b, c);
          break;
        case 4:
          for (let i = 0; i < pl; i++) p[i].emit(eventName, a, b, c, d);
          break;
        case 5:
          for (let i = 0; i < pl; i++) p[i].emit(eventName, a, b, c, d, e);
          break;
        default: {
          const args = getArgs(al);
          for (let j = 0; j < al; j++) args[j] = arguments[j + 1];
          for (let i = 0; i < pl; i++) p[i].emit(eventName, ...args);
          releaseArgs(args);
        }
      }
    }
    
    return true;
  }

  emitAsync<K extends EventKey<T>>(eventName: K, ...args: any[]): Promise<any[]> {
    const key = eventName as string | symbol;
    const listeners = this.events[key];
    const once = this.onceEvents[key];
    const promises: Promise<any>[] = [];

    const collect = (fn: Listener): void => {
      try {
        const result: any = fn(...args);
        if (result != null && typeof result.then === 'function') {
          promises.push(result);
        }
      } catch (err) {
        promises.push(Promise.reject(err));
      }
    };

    if (listeners) {
      if (typeof listeners === 'function') {
        collect(listeners);
      } else {
        for (let i = 0, len = listeners.length; i < len; i++) collect(listeners[i]);
      }
    }

    if (once) {
      if (typeof once === 'function') {
        this.cleanupMeta(once);
        this.onceEvents[key] = void 0;
        collect(once);
      } else {
        if (this.metadata) {
          for (let i = 0, len = once.length; i < len; i++) this.cleanupMeta(once[i]);
        }
        this.onceEvents[key] = void 0;
        for (let i = 0, len = once.length; i < len; i++) collect(once[i]);
      }
    }

    return Promise.allSettled(promises).then(results =>
      results.map(r => r.status === 'fulfilled' ? r.value : r.reason)
    );
  }

  serialize<K extends EventKey<T>>(eventName: K, args: any[]): string {
    return JSON.stringify({ event: String(eventName), args });
  }

  deserialize(data: string): { event: string; args: any[] } | null {
    try {
      const parsed = JSON.parse(data);
      return parsed.event && Array.isArray(parsed.args) ? parsed : null;
    } catch {
      return null;
    }
  }

  onAny(listener: (eventName: EventKey<T>, ...args: any[]) => void): this {
    this.wildcards[this.wildcards.length] = listener;
    return this;
  }

  offAny(listener?: (eventName: EventKey<T>, ...args: any[]) => void): this {
    if (!listener) {
      this.wildcards.length = 0;
      return this;
    }
    const i = this.wildcards.indexOf(listener);
    if (i > -1) this.wildcards.splice(i, 1);
    return this;
  }

  pipe(target: EventEmitter<any>): this {
    this.pipes[this.pipes.length] = target;
    return this;
  }

  unpipe(target?: EventEmitter<any>): this {
    if (!target) {
      this.pipes.length = 0;
      return this;
    }
    const i = this.pipes.indexOf(target);
    if (i > -1) this.pipes.splice(i, 1);
    return this;
  }

  removeAllListeners<K extends EventKey<T>>(eventName?: K): this {
    if (eventName) {
      const key = eventName as string | symbol;
      if (this.metadata) {
        const el = this.events[key];
        if (el) {
          if (typeof el === 'function') {
            this.cleanupMeta(el);
          } else {
            for (let i = 0, len = el.length; i < len; i++) this.cleanupMeta(el[i]);
          }
        }
        const ol = this.onceEvents[key];
        if (ol) {
          if (typeof ol === 'function') {
            this.cleanupMeta(ol);
          } else {
            for (let i = 0, len = ol.length; i < len; i++) this.cleanupMeta(ol[i]);
          }
        }
      }
      this.events[key] = this.onceEvents[key] = void 0;
    } else {
      if (this.metadata) {
        for (const key in this.events) {
          const v = this.events[key];
          if (typeof v === 'function') {
            this.cleanupMeta(v);
          } else if (v) {
            for (let i = 0, len = v.length; i < len; i++) this.cleanupMeta(v[i]);
          }
        }
        for (const key in this.onceEvents) {
          const v = this.onceEvents[key];
          if (typeof v === 'function') {
            this.cleanupMeta(v);
          } else if (v) {
            for (let i = 0, len = v.length; i < len; i++) this.cleanupMeta(v[i]);
          }
        }
      }
      this.events = {};
      this.onceEvents = {};
      this.wildcards.length = 0;
    }
    return this;
  }

  listenerCount<K extends EventKey<T>>(eventName?: K): number {
    if (!eventName) {
      let total = this.wildcards.length;
      for (const key in this.events) {
        const v = this.events[key];
        if (v) total += typeof v === 'function' ? 1 : v.length;
      }
      for (const key in this.onceEvents) {
        const v = this.onceEvents[key];
        if (v) total += typeof v === 'function' ? 1 : v.length;
      }
      return total;
    }
    const key = eventName as string | symbol;
    const el = this.events[key];
    const ol = this.onceEvents[key];
    return (el ? (typeof el === 'function' ? 1 : el.length) : 0) + 
           (ol ? (typeof ol === 'function' ? 1 : ol.length) : 0);
  }

  eventNames(): EventKey<T>[] {
    const names = new Set<EventKey<T>>();
    for (const k in this.events) {
      if (this.events[k]) names.add(k as EventKey<T>);
    }
    for (const k in this.onceEvents) {
      if (this.onceEvents[k]) names.add(k as EventKey<T>);
    }
    return Array.from(names);
  }

  listeners<K extends EventKey<T>>(eventName: K): T[K][] {
    const key = eventName as string | symbol;
    const el = this.events[key];
    const ol = this.onceEvents[key];
    const result: T[K][] = [];
    
    if (el) {
      if (typeof el === 'function') {
        result[0] = el as T[K];
      } else {
        for (let i = 0, len = el.length; i < len; i++) {
          result[result.length] = el[i] as T[K];
        }
      }
    }
    
    if (ol) {
      if (typeof ol === 'function') {
        result[result.length] = ol as T[K];
      } else {
        for (let i = 0, len = ol.length; i < len; i++) {
          result[result.length] = ol[i] as T[K];
        }
      }
    }
    
    return result;
  }

  rawListeners<K extends EventKey<T>>(eventName: K): T[K][] {
    return this.listeners(eventName);
  }

  waitFor<K extends EventKey<T>>(eventName: K, timeout?: number): Promise<EventArgs<T, K>> {
    return new Promise((resolve, reject) => {
      let timer: any;
      const handler = (...args: EventArgs<T, K>): void => {
        if (timer) clearTimeout(timer);
        resolve(args);
      };
      this.once(eventName, handler as T[K]);
      if (timeout) {
        timer = setTimeout(() => {
          this.removeListener(eventName, handler as T[K]);
          reject(new Error(`Timeout: ${String(eventName)}`));
        }, timeout);
      }
    });
  }

  race<K extends EventKey<T>>(events: K[], timeout?: number): Promise<{ event: K; args: any[] }> {
    return new Promise((resolve, reject) => {
      let timer: any;
      const handlers: Listener[] = [];
      const eventCount = events.length;
     
      const cleanup = (): void => {
        if (timer) clearTimeout(timer);
        for (let i = 0; i < eventCount; i++) {
          this.removeListener(events[i], handlers[i] as T[typeof events[number]]);
        }
      };
     
      for (let i = 0; i < eventCount; i++) {
        const eventName = events[i];
        const handler = (...args: any[]): void => {
          cleanup();
          resolve({ event: eventName, args });
        };
        handlers[i] = handler;
        this.once(eventName, handler as T[K]);
      }
     
      if (timeout) {
        timer = setTimeout(() => {
          cleanup();
          reject(new Error(`Timeout: no events fired`));
        }, timeout);
      }
    });
  }

  setMaxListeners(n: number): this {
    this.maxListeners = n;
    return this;
  }

  getMaxListeners(): number {
    return this.maxListeners;
  }

  prependListener<K extends EventKey<T>>(eventName: K, listener: T[K]): this {
    const key = eventName as string | symbol;
    const l = this.events[key];
    if (!l) {
      this.events[key] = listener as Listener;
    } else if (typeof l === 'function') {
      this.events[key] = [listener as Listener, l];
    } else {
      l.unshift(listener as Listener);
    }
    return this;
  }

  prependOnceListener<K extends EventKey<T>>(eventName: K, listener: T[K]): this {
    const key = eventName as string | symbol;
    const l = this.onceEvents[key];
    if (!l) {
      this.onceEvents[key] = listener as Listener;
    } else if (typeof l === 'function') {
      this.onceEvents[key] = [listener as Listener, l];
    } else {
      l.unshift(listener as Listener);
    }
    return this;
  }

  dispose(): void {
    this.removeAllListeners();
    this.wildcards.length = 0;
    this.pipes.length = 0;
  }
}

export default EventEmitter;
