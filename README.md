# ✅ Current Features
| 🔹 Feature               | Description                                                                |
| ------------------------ | -------------------------------------------------------------------------- |
| 🧩 Strongly-Typed Events | Full TypeScript support for events and arguments.                          |
| 🔄 Event Streams         | `.map()`, `.filter()`, `.debounce()`, `.throttle()`, `.take()`, `.skip()`. |
| ⚡ Async Support          | `.emitAsync()` collects promises and errors.                               |
| 🛠 Listener Control      | TTL, priority, times, prepend/append, once listeners.                      |
| 🔗 Piping & onAny        | Forward events `.pipe()`, listen to all `.onAny()`.                        |
| 💾 Serialization         | `.serialize()` / `.deserialize()` events.                                  |
| 📝 Utility Methods       | `.wait()`, `.race()`, `.listeners()`, `.names()`, `.count()`.              |
| 🧹 Disposal & Cleanup    | `.dispose()` removes listeners and references.                             |


# planned
| ✨ Feature                             | Description                                      |
| ------------------------------------- | ------------------------------------------------ |
| 🌐 Wildcard / Namespaced Events       | Listen to grouped events like `user.*`.          |
| 🛑 Cancelable Events                  | Stop propagation via `event.preventDefault()`.   |
| 🧭 Event Context / `this` Binding     | Access emitter or context inside callbacks.      |
| ❌ Error Event Handling                | Dedicated `"error"` event for exceptions.        |
| ⚠️ Max Listeners Warnings             | Prevent memory leaks.                            |
| 🌳 Event Bubbling / Hierarchy         | Parent-child emitter chains.                     |
| 🔄 Event Propagation Control          | Stop propagation at any listener.                |
| 🧪 Typed Async Streams                | Fully typed reactive pipelines.                  |
| 🔝 Dynamic Listener Prioritization    | Change listener priority at runtime.             |
| 🗂 Listener Metadata Exposure         | Access TTL, priority, timestamp info.            |
| 🆔 Listener Names / IDs               | Assign identifiers for easier removal.           |
| 📦 Batch Emit / Multiple Events       | Emit multiple events efficiently.                |
| ⏪ Event History / Replay              | Replay recent events for late subscribers.       |
| 🐞 Debug / Logging Tools              | `.dump()`, `.listenerTree()`, `.eventLog()`.     |
| 🧹 Automatic Cleanup                  | Remove orphaned listeners automatically.         |
| 📡 Event Serialization & Transport    | WebSockets, IPC, Workers integration.            |
| 🖥 Cluster / Multi-Process Support    | Share events across Node.js processes.           |
| 🔗 Observable / Promise Conversion    | RxJS or AsyncIterable support.                   |
| 🔄 Selective Cross-Emitter Piping     | Pipe only specific events.                       |
| ⏱ Global Event Throttling             | Rate limit high-volume events.                   |
| 🧩 Stronger Generics                  | Fully typed async streams.                       |
| 🔗 Fluent API / Chaining              | Chain `.on()`, `.once()`, `.map()`, `.pipe()`.   |
| ✅ Event Validation                    | Optional runtime schema checks.                  |
| 📝 Default Values / Fallback Handlers | Run defaults if no listener exists.              |
| 🔌 Node.js API Compatibility          | Drop-in replacement for Node EventEmitter.       |
| 📊 Event Timeline / Graph             | Visualize emitted events and flow.               |
| 📈 Listener Statistics                | Track invocation count, execution time, memory.  |
| 🔄 Hot Reload / Live Update           | Replace listeners dynamically.                   |
| 🔍 Advanced Event Filtering           | Regex / predicate-based routing.                 |
| 🧩 Plugin Architecture                | Extend emitter with logging, metrics, analytics. |
