# Events
Peak event emitter
## Roadmap

| Category | Description | Status |
|-----------|--------------|--------|
|  **Wildcard / Namespaced Events** | Listen to grouped events like `user.*` | ✅ Implemented |
|  **Event Context / this Binding** | Access emitter or context inside callbacks | ✅ Implemented |
|  **Error Event Handling** | Dedicated `"error"` event for exceptions | ❌ Not yet (partial) |
|  **Max Listeners Warnings** | Prevent memory leaks | ✅ Implemented |
|  **Event Propagation Control** | Stop propagation at any listener | ❌ Not yet |
|  **Typed Async Streams** | Fully typed reactive pipelines |✅ Implemented |
|  **Dynamic Listener Prioritization** | Change listener priority at runtime | ❌ Not yet (planned for 0.0.6, creates chainable .setPriority() |
|  **Listener Metadata Exposure** | Access TTL, priority, timestamp info | ✅ Implemented |
|  **Batch Emit / Multiple Events** | Emit multiple events efficiently | ✅ Implemented |
|  **Automatic Cleanup** | Remove orphaned listeners automatically | ✅ Implemented |
|  **Event Transport** | WebSockets, IPC, Workers integration | ❌ Not yet |
|  **Observable / Promise Conversion** | RxJS like operators| ✅ Implemented  |
|  **Selective Cross-Emitter Piping** | Pipe only specific events | ✅ Implemented |
|  **Global Event Throttling** | Rate limit high-volume events | ❌ Not yet |
|  **Stronger Generics** | Fully typed async streams | ✅ Implemented |
|  **Fluent API / Chaining** | Chain `.on()`, `.once()`, `.map()`, `.pipe()` | ❌ Not yet (partial) |
|  **Hot Reload / Live Update** | Replace listeners dynamically | ❌ Not yet |
|  **Advanced Event Filtering** | Regex / predicate-based routing | ❌ Not yet (planned for 0.0.6)|
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
