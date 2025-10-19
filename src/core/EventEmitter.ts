import type { EventKey, EventMap, EventArgs, Listener, ListenerMeta, ListenerOptions } from '../types';
import { EventStream } from './EventStream';
import { ListenerHandler } from './ListenerHandler';
import { getArgs, compilePattern, PATTERN_CACHE } from '../helpers';

export class EventEmitter<T extends EventMap> {
  private events: any = {};
  private onceEvents: any = {};
  private wildcards: Map<string, {listener: Listener, pattern: RegExp}[]> = new Map();
  private anyListeners: Listener[] = [];
  private pipes: EventEmitter<any>[] = [];
  private metadata?: WeakMap<Listener, ListenerMeta>;
  private maxListeners = 10;
  private cleanupInterval?: NodeJS.Timeout;
  private cleanupThreshold = 300000;
  private autoCleanup = false;
  
  private flags = 0;
  private static readonly HAS_WILDCARDS = 1;
  private static readonly HAS_PIPES = 2;
  private static readonly HAS_ANY = 4;

  constructor(opts?: { autoCleanup?: boolean; autoCleanupThreshold?: number }) {
    if (opts?.autoCleanupThreshold) {
      this.cleanupThreshold = opts.autoCleanupThreshold;
    }
    if (opts?.autoCleanup || opts?.autoCleanupThreshold) {
      this.autoCleanup = true;
      this.startAutoCleanup(this.cleanupThreshold);
    }
  }

  private startAutoCleanup(ms: number): void {
    if (this.cleanupInterval) return;
    
    this.cleanupInterval = setInterval(() => {
      this.performAutoCleanup();
    }, ms);
    
    if (typeof process !== 'undefined' && process.once) {
      const cleanup = (): void => {
        if (this.cleanupInterval) {
          clearInterval(this.cleanupInterval);
          this.cleanupInterval = undefined;
        }
      };
      process.once('exit', cleanup);
      process.once('SIGINT', cleanup);
      process.once('SIGTERM', cleanup);
    }
  }

  private performAutoCleanup(): void {
    if (!this.metadata) return;
    
    const now = Date.now();
    const threshold = this.cleanupThreshold;
    
    for (const key in this.events) {
      const listeners = this.events[key];
      if (!listeners) continue;
      
      if (typeof listeners === 'function') {
        const meta = this.metadata.get(listeners);
        if (meta && (now - meta.lastAccess) > threshold) {
          this.removeListener(key as any, listeners);
        }
      } else {
        for (let i = listeners.length - 1; i >= 0; i--) {
          const meta = this.metadata.get(listeners[i]);
          if (meta && (now - meta.lastAccess) > threshold) {
            this.removeListener(key as any, listeners[i]);
          }
        }
      }
    }
  
    for (const key in this.onceEvents) {
      const listeners = this.onceEvents[key];
      if (!listeners) continue;
      
      if (typeof listeners === 'function') {
        const meta = this.metadata.get(listeners);
        if (meta && (now - meta.lastAccess) > threshold) {
          this.removeListener(key as any, listeners);
        }
      } else {
        for (let i = listeners.length - 1; i >= 0; i--) {
          const meta = this.metadata.get(listeners[i]);
          if (meta && (now - meta.lastAccess) > threshold) {
            this.removeListener(key as any, listeners[i]);
          }
        }
      }
    }
  }

  private addWildcardListener(eventName: string, listener: Listener, options?: ListenerOptions): void {
    const pattern = compilePattern(eventName);
    const entry = { listener, pattern };
    
    if (!this.wildcards.has(eventName)) {
      this.wildcards.set(eventName, [entry]);
    } else {
      this.wildcards.get(eventName)!.push(entry);
    }
    
    this.flags |= EventEmitter.HAS_WILDCARDS;
    
    if (options) {
      if (!this.metadata) this.metadata = new WeakMap();
      const meta: ListenerMeta = {
        priority: options.priority ?? 0,
        times: options.times ?? -1,
        timeoutId: options.ttl && options.ttl > 0 ? 
          setTimeout(() => this.removeListener(eventName as any, listener as any), options.ttl) : 0,
        original: null,
        lastAccess: Date.now(),
        handle: null
      };
      this.metadata.set(listener, meta);
    }
  }

  private cleanupListener(fn: Listener): void {
    if (!this.metadata) return;
    const meta = this.metadata.get(fn);
    if (meta?.timeoutId) {
      clearTimeout(meta.timeoutId);
    }
    this.metadata.delete(fn);
  }

  addListener<K extends EventKey<T>>(eventName: K, listener: T[K], options?: ListenerOptions): this {
    const key = eventName as string | symbol;
    const fn = listener as any;
    
    if (typeof key === 'string' && (key.includes('*') || key.includes('?'))) {
      this.addWildcardListener(key, fn, options);
      return this;
    }
 
    if (!options) {
      const current = this.events[key];
      if (!current) {
        this.events[key] = fn;
      } else if (typeof current === 'function') {
        this.events[key] = [current, fn];
      } else {
        current[current.length] = fn;
        if (current.length > this.maxListeners && (current.length & (current.length - 1)) === 0) {
          console.warn(`Warning: Possible EventEmitter memory leak detected. ${current.length} listeners added for event "${String(key)}". Use emitter.setMaxListeners() to increase limit.`);
        }
      }
      return this;
    }

    if (!this.metadata) this.metadata = new WeakMap();
    
    const priority = options.priority ?? 0;
    const times = options.times ?? -1;
    let finalListener = fn;
    let originalListener: Listener | null = null;

    if (times > 0) {
      originalListener = fn;
      const self = this;
      finalListener = function(this: any, ...args: any[]): any {
        const meta = self.metadata!.get(finalListener);
        if (meta && --meta.times === 0) {
          self.removeListener(eventName, finalListener as any);
        }
        return originalListener!.apply(this, args);
      } as Listener;
    }
    
    const meta: ListenerMeta = {
      priority,
      times,
      timeoutId: options.ttl && options.ttl > 0 ? 
        setTimeout(() => this.removeListener(eventName, finalListener as any), options.ttl) : 0,
      original: originalListener,
      lastAccess: Date.now(),
      handle: null
    };
    this.metadata.set(finalListener, meta);

    const current = this.events[key];
    
    if (priority === 0) {
      if (!current) {
        this.events[key] = finalListener;
      } else if (typeof current === 'function') {
        this.events[key] = [current, finalListener];
      } else {
        current[current.length] = finalListener;
      }
    } else {
      let arr: Listener[];
      if (!current) {
        arr = [finalListener];
        this.events[key] = arr;
      } else if (typeof current === 'function') {
        arr = [current];
        this.events[key] = arr;
      } else {
        arr = current;
      }

      let inserted = false;
      for (let i = 0, len = arr.length; i < len; i++) {
        const existingMeta = this.metadata.get(arr[i]);
        if (priority > (existingMeta?.priority ?? 0)) {
          arr.splice(i, 0, finalListener);
          inserted = true;
          break;
        }
      }
      if (!inserted) {
        arr[arr.length] = finalListener;
      }
    }
    
    return this;
  }

  on<K extends EventKey<T>>(eventName: K, listener: T[K], options?: ListenerOptions): this {
    return this.addListener(eventName, listener, options);
  }

  prependListener<K extends EventKey<T>>(eventName: K, listener: T[K], options?: ListenerOptions): this {
    const key = eventName as string | symbol;
    const fn = listener as any;
    
    if (!options) {
      const l = this.events[key];
      if (!l) {
        this.events[key] = fn;
      } else if (typeof l === 'function') {
        this.events[key] = [fn, l];
      } else {
        l.unshift(fn);
      }
      return this;
    }
    
    const opts = { ...options, priority: (options.priority || 0) + 1 };
    return this.addListener(eventName, listener, opts);
  }

  once<K extends EventKey<T>>(eventName: K, listener: T[K], options?: ListenerOptions): this {
    const key = eventName as string | symbol;
    const fn = listener as any;
    
    // Fast path: no options, no wildcards
    if (!options && typeof key !== 'string') {
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
    
    if (!this.metadata) this.metadata = new WeakMap();
    
    const priority = options?.priority ?? 0;
    const meta: ListenerMeta = {
      priority,
      times: -1,
      timeoutId: options?.ttl && options.ttl > 0 ? 
        setTimeout(() => this.removeListener(eventName, listener), options.ttl) : 0,
      original: null,
      lastAccess: Date.now(),
      handle: null
    };
    this.metadata.set(fn, meta);
    
    const current = this.onceEvents[key];
    
    if (priority === 0) {
      if (!current) {
        this.onceEvents[key] = fn;
      } else if (typeof current === 'function') {
        this.onceEvents[key] = [current, fn];
      } else {
        current[current.length] = fn;
      }
    } else {
      let arr: Listener[];
      if (!current) {
        arr = [fn];
        this.onceEvents[key] = arr;
      } else if (typeof current === 'function') {
        arr = [current];
        this.onceEvents[key] = arr;
      } else {
        arr = current;
      }

      let inserted = false;
      for (let i = 0, len = arr.length; i < len; i++) {
        const existingMeta = this.metadata.get(arr[i]);
        if (priority > (existingMeta?.priority ?? 0)) {
          arr.splice(i, 0, fn);
          inserted = true;
          break;
        }
      }
      if (!inserted) {
        arr[arr.length] = fn;
      }
    }
    
    return this;
  }

  prependOnceListener<K extends EventKey<T>>(eventName: K, listener: T[K], options?: ListenerOptions): this {
    const key = eventName as string | symbol;
    const fn = listener as any;
    
    if (!options) {
      const l = this.onceEvents[key];
      if (!l) {
        this.onceEvents[key] = fn;
      } else if (typeof l === 'function') {
        this.onceEvents[key] = [fn, l];
      } else {
        l.unshift(fn);
      }
      return this;
    }
    
    const opts = { ...options, priority: (options.priority || 0) + 1 };
    return this.once(eventName, listener, opts);
  }

  removeListener<K extends EventKey<T>>(eventName?: K, listener?: T[K]): this {
    if (!eventName) {
      this.removeAllListeners();
      return this;
    }

    const key = eventName as string | symbol;
    
    if (!listener) {
      if (typeof key === 'string' && this.wildcards.has(key)) {
        const entries = this.wildcards.get(key)!;
        for (const entry of entries) {
          this.cleanupListener(entry.listener);
        }
        this.wildcards.delete(key);
        if (this.wildcards.size === 0) {
          this.flags &= ~EventEmitter.HAS_WILDCARDS;
        }
      }
      
      const el = this.events[key];
      const ol = this.onceEvents[key];
      
      if (el) {
        if (typeof el === 'function') {
          this.cleanupListener(el);
        } else {
          for (let i = 0, len = el.length; i < len; i++) {
            this.cleanupListener(el[i]);
          }
        }
        delete this.events[key];
      }
      
      if (ol) {
        if (typeof ol === 'function') {
          this.cleanupListener(ol);
        } else {
          for (let i = 0, len = ol.length; i < len; i++) {
            this.cleanupListener(ol[i]);
          }
        }
        delete this.onceEvents[key];
      }
      
      return this;
    }

    const fn = listener as any;
  
    if (typeof key === 'string' && this.wildcards.has(key)) {
      const entries = this.wildcards.get(key)!;
      for (let i = entries.length - 1; i >= 0; i--) {
        if (entries[i].listener === fn) {
          this.cleanupListener(fn);
          entries.splice(i, 1);
          if (entries.length === 0) {
            this.wildcards.delete(key);
            if (this.wildcards.size === 0) {
              this.flags &= ~EventEmitter.HAS_WILDCARDS;
            }
          }
          return this;
        }
      }
    }
    
    // Remove from regular events
    let l = this.events[key];
    if (l) {
      if (l === fn) {
        this.cleanupListener(fn);
        delete this.events[key];
      } else if (typeof l !== 'function') {
        const idx = l.indexOf(fn);
        if (idx > -1) {
          this.cleanupListener(fn);
          l.splice(idx, 1);
          if (l.length === 1) this.events[key] = l[0];
          else if (l.length === 0) delete this.events[key];
        } else if (this.metadata) {
          // Check for wrapped listeners
          for (let i = l.length - 1; i >= 0; i--) {
            const meta = this.metadata.get(l[i]);
            if (meta?.original === fn) {
              this.cleanupListener(l[i]);
              l.splice(i, 1);
              if (l.length === 1) this.events[key] = l[0];
              else if (l.length === 0) delete this.events[key];
              break;
            }
          }
        }
      }
    }
    
    l = this.onceEvents[key];
    if (l) {
      if (l === fn) {
        this.cleanupListener(fn);
        delete this.onceEvents[key];
      } else if (typeof l !== 'function') {
        const idx = l.indexOf(fn);
        if (idx > -1) {
          this.cleanupListener(fn);
          l.splice(idx, 1);
          if (l.length === 1) this.onceEvents[key] = l[0];
          else if (l.length === 0) delete this.onceEvents[key];
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
        }
      }
    
      const ol = this.onceEvents[key];
      if (ol) {
        if (typeof ol === 'function') {
          this.cleanupListener(ol);
          delete this.onceEvents[key];
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
            }
          }
        } else {
          const n = ol.length;
          for (let i = 0; i < n; i++) this.cleanupListener(ol[i]);
          delete this.onceEvents[key];
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
            }
          }
        }
      }

      if (this.flags) {
        this.emitExtras(eventName, al, a, b, c, d, e);
      }
      
      return true;
    }
    
    const ol = this.onceEvents[key];
    const w = this.anyListeners;
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
        }
      }
    }
    
    if (ol) {
      if (typeof ol === 'function') {
        this.cleanupListener(ol);
        delete this.onceEvents[key];
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
          }
        }
      } else {
        const n = ol.length;
        for (let i = 0; i < n; i++) this.cleanupListener(ol[i]);
        delete this.onceEvents[key];
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
          }
        }
      }
    }
    
    if (this.flags) {
      this.emitExtras(eventName, al, a, b, c, d, e);
    }
    return true;
  }

  private emitExtras<K extends EventKey<T>>(eventName: K, al: number, a?: any, b?: any, c?: any, d?: any, e?: any): void {
    const key = eventName as string | symbol;

    if (this.flags & EventEmitter.HAS_WILDCARDS) {
      const eventStr = String(key);
      for (const [, entries] of this.wildcards) {
        for (const { listener, pattern } of entries) {
          if (pattern.test(eventStr)) {
            switch (al) {
              case 0: listener(); break;
              case 1: listener(a); break;
              case 2: listener(a, b); break;
              case 3: listener(a, b, c); break;
              case 4: listener(a, b, c, d); break;
              case 5: listener(a, b, c, d, e); break;
              default: {
                const args = getArgs(al);
                for (let i = 0; i < al; i++) args[i] = arguments[i + 2];
                listener(...args);
              }
            }
          }
        }
      }
    }

    if (this.flags & EventEmitter.HAS_ANY) {
      const w = this.anyListeners;
      const wl = w.length;
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
          for (let j = 0; j < al; j++) args[j + 1] = arguments[j + 2];
          for (let i = 0; i < wl; i++) w[i](...args);
        }
      }
    }

    if (this.flags & EventEmitter.HAS_PIPES) {
      const p = this.pipes;
      const pl = p.length;
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
          for (let j = 0; j < al; j++) args[j] = arguments[j + 2];
          for (let i = 0; i < pl; i++) p[i].emit(eventName, ...args);
        }
      }
    }
  }

  emitAsync<K extends EventKey<T>>(eventName: K, ...args: EventArgs<T, K>): Promise<any[]> {
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
        this.cleanupListener(once);
        delete this.onceEvents[key];
        collect(once);
      } else {
        if (this.metadata) {
          for (let i = 0, len = once.length; i < len; i++) this.cleanupListener(once[i]);
        }
        delete this.onceEvents[key];
        for (let i = 0, len = once.length; i < len; i++) collect(once[i]);
      }
    }

    return Promise.allSettled(promises).then(results =>
      results.map(r => r.status === 'fulfilled' ? r.value : r.reason)
    );
  }

  onAny(listener: (eventName: EventKey<T>, ...args: any[]) => void): this {
    this.anyListeners[this.anyListeners.length] = listener;
    this.flags |= EventEmitter.HAS_ANY;
    return this;
  }

  offAny(listener?: (eventName: EventKey<T>, ...args: any[]) => void): this {
    if (!listener) {
      this.anyListeners.length = 0;
      this.flags &= ~EventEmitter.HAS_ANY;
      return this;
    }
    const i = this.anyListeners.indexOf(listener);
    if (i > -1) {
      this.anyListeners.splice(i, 1);
      if (this.anyListeners.length === 0) {
        this.flags &= ~EventEmitter.HAS_ANY;
      }
    }
    return this;
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
          this.removeListener(events[i], handlers[i] as T[K]);
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

  stream<K extends EventKey<T>>(eventName: K): EventStream<EventArgs<T, K>> {
    return new EventStream(this, eventName as string | symbol)
  }

  listenerHandler<K extends EventKey<T>>(eventName: K, listener: T[K]): ListenerHandler<T> {
    return new ListenerHandler(this, eventName, listener);
  }

  removeAllListeners<K extends EventKey<T>>(eventName?: K): this {
    if (eventName) {
      const key = eventName as string | symbol;
      if (typeof key === 'string') {
        this.wildcards.delete(key);
        if (this.wildcards.size === 0) {
          this.flags &= ~EventEmitter.HAS_WILDCARDS;
        }
      }

      const el = this.events[key];
      const ol = this.onceEvents[key];
      
      if (el) {
        if (typeof el === 'function') {
          this.cleanupListener(el);
        } else {
          for (let i = 0, len = el.length; i < len; i++) {
            this.cleanupListener(el[i]);
          }
        }
        delete this.events[key];
      }
      
      if (ol) {
        if (typeof ol === 'function') {
          this.cleanupListener(ol);
        } else {
          for (let i = 0, len = ol.length; i < len; i++) {
            this.cleanupListener(ol[i]);
          }
        }
        delete this.onceEvents[key];
      }
    } else {
      this.wildcards.clear();
      this.anyListeners.length = 0;
      this.flags &= ~(EventEmitter.HAS_WILDCARDS | EventEmitter.HAS_ANY);
      
      for (const key in this.events) {
        const el = this.events[key];
        if (el) {
          if (typeof el === 'function') {
            this.cleanupListener(el);
          } else {
            for (let i = 0, len = el.length; i < len; i++) {
              this.cleanupListener(el[i]);
            }
          }
        }
      }
      
      for (const key in this.onceEvents) {
        const ol = this.onceEvents[key];
        if (ol) {
          if (typeof ol === 'function') {
            this.cleanupListener(ol);
          } else {
            for (let i = 0, len = ol.length; i < len; i++) {
              this.cleanupListener(ol[i]);
            }
          }
        }
      }
      
      this.events = {};
      this.onceEvents = {};
      this.pipes.length = 0;
      this.flags &= ~EventEmitter.HAS_PIPES;
    }
    
    return this;
  }

  listenerCount<K extends EventKey<T>>(eventName?: K): number {
    if (!eventName) {
      let total = this.anyListeners.length;
      for (const [, entries] of this.wildcards) {
        total += entries.length;
      }
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
    let total = (el ? (typeof el === 'function' ? 1 : el.length) : 0) + 
                (ol ? (typeof ol === 'function' ? 1 : ol.length) : 0);
    if (typeof key === 'string') {
      for (const [pattern, entries] of this.wildcards) {
        const regex = PATTERN_CACHE.get(pattern) || compilePattern(pattern);
        if (regex.test(key)) {
          total += entries.length;
        }
      }
    }
    
    return total;
  }

  eventNames(): EventKey<T>[] {
    const names = new Set<EventKey<T>>();
    for (const k in this.events) {
      if (this.events[k]) names.add(k as EventKey<T>);
    }
    for (const k in this.onceEvents) {
      if (this.onceEvents[k]) names.add(k as EventKey<T>);
    }
    for (const k of this.wildcards.keys()) {
      names.add(k as EventKey<T>);
    }
    return Array.from(names);
  }

  listeners<K extends EventKey<T>>(eventName: K): T[K][] {
    const key = eventName as string | symbol;
    const result: T[K][] = [];
    const el = this.events[key];
    if (el) {
      if (typeof el === 'function') {
        result.push(el as T[K]);
      } else {
        for (let i = 0, len = el.length; i < len; i++) {
          result.push(el[i] as T[K]);
        }
      }
    }
    
    const ol = this.onceEvents[key];
    if (ol) {
      if (typeof ol === 'function') {
        result.push(ol as T[K]);
      } else {
        for (let i = 0, len = ol.length; i < len; i++) {
          result.push(ol[i] as T[K]);
        }
      }
    }
    
    if (typeof key === 'string') {
      for (const [pattern, entries] of this.wildcards) {
        const regex = PATTERN_CACHE.get(pattern) || compilePattern(pattern);
        if (regex.test(key)) {
          for (const entry of entries) {
            result.push(entry.listener as T[K]);
          }
        }
      }
    }
    
    return result;
  }

  rawListeners<K extends EventKey<T>>(eventName: K): T[K][] {
    return this.listeners(eventName);
  }

  pipe(target: EventEmitter<any>): this {
    this.pipes.push(target);
    this.flags |= EventEmitter.HAS_PIPES;
    return this;
  }

  unpipe(target?: EventEmitter<any>): this {
    if (!target) {
      this.pipes.length = 0;
      this.flags &= ~EventEmitter.HAS_PIPES;
    } else {
      const i = this.pipes.indexOf(target);
      if (i > -1) {
        this.pipes.splice(i, 1);
        if (this.pipes.length === 0) {
          this.flags &= ~EventEmitter.HAS_PIPES;
        }
      }
    }
    return this;
  }

  setMaxListeners(n: number): this {
    this.maxListeners = n;
    return this;
  }

  getMaxListeners(): number {
    return this.maxListeners;
  }

  destroy(): void {
    this.removeAllListeners();
    if (this.cleanupInterval) {
      clearInterval(this.cleanupInterval);
      this.cleanupInterval = undefined;
    }
    // Clear all references to prevent memory leaks
    this.events = {};
    this.onceEvents = {};
    this.wildcards.clear();
    this.anyListeners.length = 0;
    this.pipes.length = 0;
    this.metadata = undefined;
    this.flags = 0;
  }
}

export default EventEmitter;
