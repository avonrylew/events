# Events
Peak event emitter
## Roadmap

| Category | Description | Status |
|-----------|--------------|--------|
|  **Wildcard / Namespaced Events** | Listen to grouped events like `user.*` | ❌ Not yet |
|  **Event Context / this Binding** | Access emitter or context inside callbacks | ✅ Implemented |
|  **Error Event Handling** | Dedicated `"error"` event for exceptions | ❌ Not yet (partial) |
|  **Max Listeners Warnings** | Prevent memory leaks | ❌ Not yet (does not enforce due to performance hits, will try to work around it.) |
|  **Event Propagation Control** | Stop propagation at any listener | ❌ Not yet |
|  **Typed Async Streams** | Fully typed reactive pipelines | ⚙️ Partial |
|  **Dynamic Listener Prioritization** | Change listener priority at runtime | ❌ Not yet (planned for 0.0.6, creates chainable .setPriority() |
|  **Listener Metadata Exposure** | Access TTL, priority, timestamp info | ✅ Implemented |
|  **Batch Emit / Multiple Events** | Emit multiple events efficiently | ❌ Not yet (perfomance hits, might make a seperate API method? .batchEmit()? idk)|
|  **Automatic Cleanup** | Remove orphaned listeners automatically | ❌ Not yet (planned for 0.0.6) |
|  **Event Serialization & Transport** | WebSockets, IPC, Workers integration | ❌ Not yet (partial, has serialization and deserialization but no actual ws use) |
|  **Observable / Promise Conversion** | RxJS or AsyncIterable support | ⚙️ Partial (not a priority, add more API methods under EventStreams) |
|  **Selective Cross-Emitter Piping** | Pipe only specific events | ✅ Implemented |
|  **Global Event Throttling** | Rate limit high-volume events | ✅ Implemented |
|  **Stronger Generics** | Fully typed async streams | ✅ Implemented |
|  **Fluent API / Chaining** | Chain `.on()`, `.once()`, `.map()`, `.pipe()` | ✅ Implemented |
|  **Node.js API Compatibility** | Drop-in replacement for Node `EventEmitter` | ✅ Implemented |
|  **Hot Reload / Live Update** | Replace listeners dynamically | ❌ Not yet (partial but i want it to better by chaining which it does not yet support) |
|  **Advanced Event Filtering** | Regex / predicate-based routing | ❌ Not yet (planned for 0.0.6, adding pattern matching events aswell) |
|  **Plugins** | Extend emitter with decorators (logging, metrics, analytics, debug, event validation and dedicated error handling) and Event Timeline/Graph for statistical purposes | ❌ Not yet (pain) |

---

## Example Usage

```ts
import { EventEmitter } from "@avonrylew/events";

interface Events {
  ready: () => void;
  data: (payload: { id: number; name: string }) => void;
  surprise: () => void;
}

const emitter = new EventEmitter<Events>();

emitter.on("ready", () => console.log("🚀 System Ready!"));

emitter.on("data", (payload) => console.log("Received:", payload));

emitter.emit("ready");
emitter.emit("data", { id: 1, name: "john doe" });
```
# 🧩 API Highlights
```
to be updated
```
# 🧑‍💻 License
Mozilla Public License 2.0
© 2025 avonrylew — All rights reserved.
