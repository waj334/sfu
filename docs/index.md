# Getting Started with inLive SFU

inLive SFU is designed to be a high-performance, developer-friendly Selective Forwarding Unit for building real-time communication applications. This guide will help you understand the core concepts and how to use the package effectively.

## Core Concepts & Hierarchy

To build an app with this package, it's important to understand how the components relate to each other:

1.  **Manager**: The root component. It manages multiple rooms and provides global configuration.
2.  **Room**: A virtual space where media is shared. Clients in the same room can discover and subscribe to each other's tracks.
3.  **Client**: Represents a single participant (peer connection) in a room. A client can both publish and subscribe to media.
4.  **Track**: The actual media stream (audio or video). A client publishes local tracks and subscribes to remote tracks from other clients.

## Development Guides

Follow these guides to learn how to implement specific features and patterns:

### 1. Patterns & Examples
*   **[Use Cases](./use-cases.md)**: (New) Implementation patterns for Live Streaming vs. Video Conferencing.

### 2. Fundamentals
*   **[Room Management](./room.md)**: How to create, find, and close rooms using the Manager.
*   **[Client Lifecycle](./client.md)**: Adding and removing clients from rooms.
*   **[Signaling & Negotiation](./signal.md)**: Understanding the WebRTC offer/answer flow and dynamic renegotiation.

### 2. Media Strategies
*   **[Publishing Media](./publishing-media.md)**: Technical "how-to" for Simulcast, SVC, and RED.
*   **[Video Quality Strategies](./video-strategies.md)**: (New) High-level strategies for ResizeObserver and IntersectionObserver.
*   **[Video Subscription](./video-subscription.md)**: How to discover and subscribe to remote tracks.

### 3. Real-time Interactivity
*   **[Data Channels](./data-channels.md)**: (New) Using data channels for low-latency messaging and custom signaling.
*   **[Observability & Smart UIs](./observability.md)**: (New) Implementing Active Speaker Detection (VAD) and Network Quality indicators.

### 4. Advanced Customization
*   **[Extending the SFU](./extension.md)**: Hooking into internal events with the Extension system.
*   **[Network Reliability](./network-reliability.md)**: (Updated) Understanding NACK, RED, and FEC.