import type { EventEmitter } from "./EventEmitter";

export class EventStream<T extends any[]> {
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

  distinctUntilChanged(compareFn?: (a: T, b: T) => boolean): EventStream<T> {
    if (this.disposed) throw new Error('Stream disposed');
    const stream = new EventStream<T>(this.emitter, this.event);
    let lastArgs: T | null = null;
    const compare = compareFn || ((a: T, b: T) => {
      if (a.length !== b.length) return false;
      for (let i = 0; i < a.length; i++) {
        if (a[i] !== b[i]) return false;
      }
      return true;
    });
    const handler = (...args: T): void => {
      if (stream.disposed) return;
      if (lastArgs && compare(lastArgs, args)) return;
      lastArgs = args;
      const h = stream.handlers.slice();
      for (let i = 0; i < h.length; i++) h[i](...args);
    };
    this.handlers.push(handler);
    this.registered.push(handler);
    this.emitter.addListener(this.event as any, handler as any);
    return stream;
  }

  merge(other: EventStream<T>): EventStream<T> {
    if (this.disposed) throw new Error('Stream disposed');
    const stream = new EventStream<T>(this.emitter, this.event);
    const handler = (...args: T): void => {
      if (stream.disposed) return;
      const h = stream.handlers.slice();
      for (let i = 0; i < h.length; i++) h[i](...args);
    };
    this.handlers.push(handler);
    this.registered.push(handler);
    this.emitter.addListener(this.event as any, handler as any);
    other.subscribe(handler);
    return stream;
  }

  scan<R>(fn: (acc: R, ...args: T) => R, seed: R): EventStream<[R]> {
    if (this.disposed) throw new Error('Stream disposed');
    const stream = new EventStream<[R]>(this.emitter, this.event);
    let acc = seed;
    const handler = (...args: T): void => {
      if (stream.disposed) return;
      acc = fn(acc, ...args);
      const h = stream.handlers.slice();
      for (let i = 0; i < h.length; i++) h[i](acc);
    };
    this.handlers.push(handler);
    this.registered.push(handler);
    this.emitter.addListener(this.event as any, handler as any);
    return stream;
  }

  toPromise(timeout?: number): Promise<T> {
    return new Promise((resolve, reject) => {
      let timer: any;
      const unsubscribe = this.subscribe((...args: T) => {
        if (timer) clearTimeout(timer);
        unsubscribe();
        resolve(args);
      });
      if (timeout) {
        timer = setTimeout(() => {
          unsubscribe();
          reject(new Error('Stream timeout'));
        }, timeout);
      }
    });
  }

  async *toAsyncIterable(): AsyncIterableIterator<T> {
    const queue: T[] = [];
    const resolvers: Array<(value: IteratorResult<T>) => void> = [];
    let done = false;
    const unsubscribe = this.subscribe((...args: T) => {
      if (resolvers.length > 0) {
        resolvers.shift()!({ value: args, done: false });
      } else {
        queue.push(args);
      }
    });
    try {
      while (!done && !this.disposed) {
        if (queue.length > 0) {
          yield queue.shift()!;
        } else {
          const result = await new Promise<IteratorResult<T>>(resolve => resolvers.push(resolve));
          if (!result.done) yield result.value;
        }
      }
    } finally {
      done = true;
      unsubscribe();
    }
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
