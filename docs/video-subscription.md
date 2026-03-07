# Video Subscription & Playback

To receive and play media in a room, a client must subscribe to the tracks published by other participants. This guide explains the flow for discovering and subscribing to available tracks.

## The Subscription Flow

The subscription process in inLive SFU is designed to be explicit, giving you control over which tracks to receive and when.

### 1. Discovering Available Tracks
When a participant in the room publishes a new track (and sets its source type), the SFU triggers the `OnTracksAvailable` callback for all other clients in that room.

```go
client.OnTracksAvailable(func(tracks []sfu.ITrack) {
    for _, track := range tracks {
        fmt.Printf("New track available: %s from client %s\n", track.ID(), track.ClientID())
        
        // You can now decide whether to subscribe to this track
    }
})
```

### 2. Subscribing to Tracks
To start receiving media for a specific set of tracks, use the `SubscribeTracks` method. This method takes a list of `SubscribeTrackRequest` objects.

```go
requests := []sfu.SubscribeTrackRequest{
    {
        ClientID: "remote-client-id",
        TrackID:  "remote-track-id",
    },
}

err := client.SubscribeTracks(requests)
if err != nil {
    log.Printf("Failed to subscribe: %v", err)
}
```

### 3. Handling the Subscription (Renegotiation)
When you call `SubscribeTracks`, the SFU will initiate a WebRTC renegotiation. 
*   The SFU will add a new transceiver to the peer connection.
*   The `OnRenegotiation` callback on the client will be triggered.
*   Your application must then complete the SDP Offer/Answer exchange to finalize the subscription.

## Automatic Subscription Strategy

If your application logic is to simply subscribe to everything that becomes available, you can implement an automatic subscription strategy within the `OnTracksAvailable` callback:

```go
client.OnTracksAvailable(func(tracks []sfu.ITrack) {
    requests := make([]sfu.SubscribeTrackRequest, len(tracks))
    for i, track := range tracks {
        requests[i] = sfu.SubscribeTrackRequest{
            ClientID: track.ClientID(),
            TrackID:  track.ID(),
        }
    }
    
    client.SubscribeTracks(requests)
})
```

## Receiving the Media (JavaScript)

On the client-side (browser), once the negotiation is complete, the standard WebRTC `ontrack` event will fire:

```javascript
peerConnection.ontrack = (event) => {
    const stream = event.streams[0];
    const videoElement = document.getElementById('remote-video');
    videoElement.srcObject = stream;
});
```

## Managing Track Removal

When a remote participant stops publishing a track or leaves the room, the SFU will automatically handle the removal of the track from your connection. You can listen for the `OnTrackRemoved` callback to update your UI:

```go
client.OnTrackRemoved(func(sourceType string, track *webrtc.TrackLocalStaticRTP) {
    fmt.Printf("Track removed: %s\n", track.ID())
    // Update your UI to remove the video player
})
```

## Summary of Callbacks

| Callback | Triggered When... |
| :--- | :--- |
| `OnTracksAvailable` | A remote participant publishes a new track. |
| `OnTracksReady` | Similar to available, but ensures the tracks are fully initialized. |
| `OnTrackRemoved` | A track you were subscribed to is no longer available. |
