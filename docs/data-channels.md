# Data Channels

Data channels in inLive SFU provide a way to send low-latency, real-time data between clients. This is useful for features like chat, reactions, whiteboard synchronization, or custom signaling.

## Types of Data Channels

There are two primary ways to use data channels in the SFU:

1.  **Room-wide Broadcast Channels**: Created by the server, these channels automatically broadcast any message sent by one client to all other clients in the room.
2.  **Internal System Channels**: Used by the SFU to communicate system information (like stats or VAD events) to the client.

## Creating a Broadcast Channel

To create a data channel that all clients in a room can use for communication, use the `CreateDataChannel` method on the `Room` or `SFU` instance.

```go
// From the room instance
opts := sfu.DataChannelOptions{
    Ordered: true,
}

err := room.CreateDataChannel("chat", opts)
if err != nil {
    log.Fatal("Failed to create data channel:", err)
}
```

### DataChannelOptions

| Option | Description |
| :--- | :--- |
| `Ordered` | If true, messages are guaranteed to arrive in order (TCP-like). If false, messages may arrive out of order for lower latency (UDP-like). |
| `ClientIDs` | (Optional) A list of specific client IDs that should have access to this channel. If empty, all clients in the room will get the channel. |

## Client-Side Usage (JavaScript)

When the SFU creates a data channel, the WebRTC `ondatachannel` event will fire on the client's `RTCPeerConnection`.

```javascript
peerConnection.ondatachannel = (event) => {
    const channel = event.channel;
    
    if (channel.label === "chat") {
        channel.onmessage = (e) => {
            console.log("Received chat message:", e.data);
        };
        
        // Send a message to everyone else in the room
        channel.send("Hello everyone!");
    }
};
```

## How it Works Under the Hood

1.  When `room.CreateDataChannel("label", opts)` is called, the SFU iterates through all connected clients.
2.  For each client, it calls `pc.CreateDataChannel("label")`.
3.  The SFU sets up a "Message Forwarder" for that specific channel.
4.  When any client sends a message on that channel, the SFU receives it and iterates through all *other* clients in the room, sending the message to their corresponding data channel.

## Performance Considerations

*   **Broadcast Volume**: Since the SFU broadcasts messages to all clients, the number of messages sent grows with `N * (N-1)` where N is the number of clients. For very large rooms, consider being selective with which clients receive certain channels.
*   **Reliability vs. Latency**: Use `Ordered: false` for features where the most recent data is more important than every single message (e.g., mouse cursor positions in a whiteboard).
