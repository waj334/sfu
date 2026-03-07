# Extending the SFU with Extensions

The inLive SFU provides a flexible extension system that allows you to hook into the lifecycle of rooms and clients. This is useful for implementing custom logic like:
*   **Authentication & Authorization**: Validating tokens before a client joins.
*   **External Logging & Monitoring**: Sending events to a database or analytics service.
*   **Presence Management**: Tracking who is online across multiple rooms.

## Extension Interfaces

There are two primary interfaces you can implement: `IManagerExtension` and `IExtension`.

### 1. IManagerExtension

This interface allows you to hook into events at the `Manager` level, such as room creation and access.

```go
type IManagerExtension interface {
    // Called when a room is requested. If an extension returns a room or an error, 
    // subsequent extensions are ignored.
    OnGetRoom(manager *Manager, roomID string) (*Room, error)
    
    // Called before a new room is created. Use this for global room creation limits or auth.
    OnBeforeNewRoom(id, name, roomType string) error
    
    // Called after a new room is successfully created.
    OnNewRoom(manager *Manager, room *Room)
    
    // Called when a room is closed.
    OnRoomClosed(manager *Manager, room *Room)
}
```

### 2. IExtension

This interface is for room-specific events, primarily focused on client lifecycle within that room.

```go
type IExtension interface {
    // Called before a client is added to a room. Perfect for per-room authentication.
    OnBeforeClientAdded(room *Room, clientID string) error
    
    // Called after a client has joined the room.
    OnClientAdded(room *Room, client *Client)
    
    // Called after a client has left the room.
    OnClientRemoved(room *Room, client *Client)
}
```

## Implementation Example: Simple Logger

Here is how you might implement a simple logger extension:

```go
type MyLogger struct{}

func (m *MyLogger) OnBeforeNewRoom(id, name, roomType string) error {
    fmt.Printf("Attempting to create room: %s (%s)\n", name, id)
    return nil
}

func (m *MyLogger) OnNewRoom(manager *sfu.Manager, room *sfu.Room) {
    fmt.Printf("Room created: %s\n", room.ID())
}

// Registering the extension
manager := sfu.NewManager(ctx, "my-server", opts)
manager.AddExtension(&MyLogger{})
```

## Using Extensions for Authentication

You can use the `OnBefore...` hooks to prevent unauthorized access by returning an error.

```go
func (m *AuthExt) OnBeforeClientAdded(room *sfu.Room, clientID string) error {
    if !isValidToken(clientID) {
        return errors.New("unauthorized: invalid client token")
    }
    return nil
}

// Add to a specific room
room.AddExtension(&AuthExt{})
```

By leveraging these hooks, you can keep your core application logic separate from the SFU's media handling logic.
