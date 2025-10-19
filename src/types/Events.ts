export type EventMap = Record<string | symbol, (...args: any[]) => void>;
export type EventKey<T extends EventMap> = keyof T;
export type EventArgs<T extends EventMap, K extends EventKey<T>> = Parameters<T[K]>;
