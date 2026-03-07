# Observability & Smart UIs

To build a high-quality real-time application, your user interface needs to react to what's happening in the media session. inLive SFU provides the telemetry needed to build features like Active Speaker Detection, Signal Bars, and Adaptive Quality.

## Active Speaker Detection (VAD)

Voice Activity Detection (VAD) allows your UI to highlight the person currently speaking. The SFU provides callbacks both when a client *sends* voice data and when it *receives* voice data from others.

### Enabling VAD
When adding a client to a room, ensure `EnableVoiceDetection` is set to `true` in the `ClientOptions`.

```go
opts := sfu.DefaultClientOptions()
opts.EnableVoiceDetection = true
client, _ := room.AddClient(clientID, clientName, opts)
```

### Server-Side Callbacks
You can listen for voice activity on the server to trigger events (e.g., logging or server-side recording markers).

```go
client.OnVoiceSentDetected(func(activity voiceactivedetector.VoiceActivity) {
    // activity contains: TrackID, StreamID, SSRC, and duration of speech
    fmt.Printf("Client %s is speaking on track %s\n", client.ID(), activity.TrackID)
})
```

### Building a "Talking" Indicator in the UI
The SFU can also send VAD events directly to the client via an internal data channel. In your JavaScript code, listen for these messages to update your UI:

```javascript
// On the client side, the SFU will use a data channel labeled "internal"
peerConnection.ondatachannel = (event) => {
    if (event.channel.label === "internal") {
        event.channel.onmessage = (msg) => {
            const data = JSON.parse(msg.data);
            if (data.type === "vad") {
                // data.data will contain the VAD activity
                updateTalkingIndicator(data.data.track_id, data.data.is_speaking);
            }
        };
    }
};
```

## Network Quality (Signal Bars)

Monitoring the health of a client's connection is critical for providing feedback to the user when their network is struggling.

### Monitoring Network Condition
You can subscribe to changes in the network condition perceived by the SFU:

```go
client.OnNetworkConditionChanged(func(condition networkmonitor.NetworkConditionType) {
    switch condition {
    case networkmonitor.NetworkConditionGood:
        // Everything is fine
    case networkmonitor.NetworkConditionFair:
        // Minor packet loss or jitter detected
    case networkmonitor.NetworkConditionPoor:
        // Significant issues, consider notifying the user
    }
})
```

### Accessing Detailed Stats
For more granular data, you can access the `ClientStats` and `RoomStats` directly:

```go
// Get stats for a specific client
stats := client.Stats()
fmt.Printf("Bitrate: %d bps, Packet Loss: %f%%\n", stats.Bitrate, stats.PacketLoss)

// Get global room stats
roomStats := room.Stats()
```

## Adaptive Quality

inLive SFU automatically manages quality levels based on available bandwidth, but you can also control this manually or observe the decisions being made.

### Bandwidth Estimation
The SFU estimates the available bandwidth for each client. You can retrieve this value to show the user their "Max Speed":

```go
bandwidth := client.GetEstimatedBandwidth() // Returns bits per second
```

### Manual Quality Control
If you want to allow a user to manually select a video quality (e.g., "Low", "Medium", "High"), you can use `SetQuality`:

```go
// Force a client to receive a specific quality level
client.SetQuality(sfu.QualityLow)
```

By combining these features, you can build a UI that feels "alive" and responsive to the real-world network conditions of your users.
