# Client Lifecycle & Management

A `Client` represents a single participant's connection to the SFU. Once a client joins a room, it can publish media tracks and subscribe to tracks from others.

## Managing Clients in a Room

Clients are added to a room by the server. You can also retrieve or stop clients at any time.

### Adding a Client
```go
opts := sfu.DefaultClientOptions()
client, err := room.AddClient(clientID, clientName, opts)
```

### Retrieving a Client
```go
// From the room
client, err := room.GetClient(clientID)

// Or from the SFU manager
client, err := manager.GetRoom(roomID).GetClient(clientID)
```

### Stopping a Client
To disconnect a client and remove all their tracks:
```go
room.StopClient(clientID)
```

## Responding to Client Events

The `Client` struct provides several callbacks to help you manage its state:

### Connection State
Monitor the health and lifecycle of the WebRTC connection:
```go
client.OnConnectionStateChanged(func(state webrtc.PeerConnectionState) {
    if state == webrtc.PeerConnectionStateConnected {
        fmt.Println("Client connected!")
    } else if state == webrtc.PeerConnectionStateDisconnected {
        fmt.Println("Client disconnected.")
    }
})
```

### Participant Lifecycle
```go
// When the client has successfully joined
client.OnJoined(func() {
    fmt.Printf("Client %s has joined\n", client.ID())
})

// When the client has left the room
client.OnLeft(func() {
    fmt.Printf("Client %s has left\n", client.ID())
})
```

## Track Events

The `Client` object is the primary place to listen for incoming media:

*   `OnTracksAvailable`: Triggered when others in the room publish tracks. Use this to know what you *can* subscribe to.
*   `OnTracksAdded`: Triggered when the *local* client adds new tracks to its own connection.

For a detailed guide on media, see **[Publishing Media](./publishing-media.md)**.
