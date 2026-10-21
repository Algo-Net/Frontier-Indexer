# Emitters and Transports

The indexer can broadcast the data it processes to external systems in real-time. This is achieved through a combination of **Emitters** and **Transports**.

## Emitters

An `Emitter<I>` is a high-level component that manages the dispatching of data batches to one or more transport implementations. Emitters are designed to be non-blocking; when an emitter is initialized, it spawns an asynchronous worker task that processes batches off an internal bounded channel, ensuring that the main indexing pipeline is not delayed by network latency.

### How it works

1. **Initialization**: An emitter is created with a list of transports that implement the `Transport<I>` trait for a specific data type `I`, establishing an internal bounded MPSC queue and background worker task.
2. **Dispatching**: The `dispatch` method takes a pipeline name and a batch of items. The batch must implement the `Dispatchable` trait (which is implemented for `Vec<I>` and `HashMap<String, I>`).
3. **Asynchronous Delivery**: The worker task iterates through the batch items and calls `Transport::send` on every configured transport concurrently with a per-transport timeout. If a transport fails or times out, a warning is logged without preventing other transports from receiving the data.

### Integration in Handlers

In a handler, an emitter is typically held as an `Arc<Emitter<T>>` and used in the `commit` stage:

```rust
// Example dispatch in a handler's commit method
self.emitter.dispatch(Self::NAME, batch);
```

## Transports

A **Transport** is a specific implementation of a delivery protocol. All transports must implement the `Transport<I>` trait.

### The Transport Trait

To ensure that a transport can handle any data type the indexer might produce, the `Transport<I>` trait requires the implementation to also be a `Router`. A `Router` must implement the `Routing<I>` trait for all supported world and app event types.

This design ensures type safety across the entire pipeline: if a transport is registered for a pipeline, it is guaranteed to know how to handle the specific data types emitted by that pipeline.

### Supported Transports

The indexer currently provides several built-in transport implementations:

- **AMQP**: Sends messages to an AMQP-compatible broker (like RabbitMQ). It uses a topic exchange where the routing key is typically formatted as `indexer.<pipeline>`.
- **NATS**: Publishes messages to a NATS server using a subject prefix.
- **Redis**: Uses Redis Pub/Sub to broadcast events to channels.
- **SocketIO**: Starts an embedded Socket.IO server, allowing web clients to connect and receive real-time updates directly from the indexer.

### Adding a New Transport

To add a new transport:
1. Create a new module in `src/transports/`.
2. Implement the `Routing<I>` trait for the data types you wish to support.
3. Implement the `Transport<I>` trait to define the actual delivery logic.
4. Register the transport in `src/transports/init.rs` so it can be initialized from environment variables.
