# Room Management

A `Room` is the primary container for a media session. It manages the collection of clients and their shared media tracks.

## Creating and Closing Rooms

Rooms are managed through the `Manager`. Each room has a unique ID, a name, and a set of options.

### Creating a Room
```go
manager := sfu.NewManager(ctx, "my-server", opts)

roomID := manager.CreateRoomID()
roomName := "my-awesome-room"
roomType := sfu.RoomTypeLocal

roomOpts := sfu.DefaultRoomOptions()
room, err := manager.NewRoom(roomID, roomName, roomType, roomOpts)
```

### Finding an Existing Room
```go
room, err := manager.GetRoom(roomID)
```

### Closing a Room
Closing a room will stop all clients and clean up all media tracks.
```go
// From the manager
manager.CloseRoom(roomID)

// Directly from the room instance
room.Close()
```

## Room Metadata & Properties

Rooms can store metadata that is shared across all participants. This is useful for synchronization or simply for descriptive purposes.

```go
// Setting metadata
room.Meta().Set("topic", "Project Update")

// Getting properties
fmt.Println("Room Name:", room.Name())
fmt.Println("Room Type:", room.Kind())
```

## Monitoring Room State

You can listen for events at the room level to react to changes in the session:

```go
// When any client joins the room
room.OnClientJoined(func(client *sfu.Client) {
    fmt.Printf("New participant: %s\n", client.Name())
})

// When any client leaves the room
room.OnClientLeft(func(client *sfu.Client) {
    fmt.Printf("Participant left: %s\n", client.Name())
})

// When the entire room is closed
room.OnRoomClosed(func(id string) {
    fmt.Printf("Room %s has been terminated\n", id)
})
```

## Global Statistics

To monitor the health and usage of an entire room, you can retrieve global stats:

```go
stats := room.Stats()
fmt.Printf("Active Clients: %d\n", stats.ActiveClients)
fmt.Printf("Total Tracks: %d\n", stats.TotalTracks)
```

For more detailed telemetry, see the **[Observability & Smart UIs](./observability.md)** guide.
