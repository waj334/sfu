# Use Cases & Implementation Patterns

inLive SFU is a versatile tool that can be used for various real-time communication scenarios. While the SFU provides the core media handling, your application logic determines the specific use case.

## 1. Live Streaming (One-to-Many)

In a live streaming scenario, one or a few **Broadcasters** publish media, while a large number of **Viewers** only consume it.

### Implementation Strategy
*   **Role Management**: Your application should distinguish between broadcaster and viewer clients (e.g., using different `ClientOptions` or metadata).
*   **Broadcaster Configuration**:
    *   Set high-quality bitrates in `RoomOptions`.
    *   Use **Simulcast** or **SVC** to ensure the stream can be adapted for viewers on poor networks.
*   **Viewer Configuration**:
    *   Viewers should start their connection with `recvonly` transceivers (they don't publish media).
    *   **Automatic Subscription**: In the `OnTracksAvailable` callback, viewers should automatically call `SubscribeTracks` for any track published by the broadcaster.
*   **Interactivity**: Use **Data Channels** for real-time features like live chat or "like" reactions without adding overhead to the media tracks.

### Example: Viewer Subscription Logic
```go
viewerClient.OnTracksAvailable(func(tracks []sfu.ITrack) {
    reqs := make([]sfu.SubscribeTrackRequest, len(tracks))
    for i, t := range tracks {
        reqs[i] = sfu.SubscribeTrackRequest{ClientID: t.ClientID(), TrackID: t.ID()}
    }
    viewerClient.SubscribeTracks(reqs)
})
```

---

## 2. Video Conference (Many-to-Many)

In a meeting or conference, every participant is both a publisher and a subscriber.

### Implementation Strategy
*   **Balanced Bitrates**: Configure `RoomOptions` to balance quality across many participants. Don't set individual bitrates too high, as the cumulative bandwidth for subscribers grows quickly.
*   **Active Speaker Detection**: Use the SFU's **Voice Activity Detection (VAD)**.
    *   On the server: Listen for `OnVoiceSentDetected` to identify the current speaker.
    *   On the client: Use the VAD events from the internal data channel to highlight the speaker's video tile.
*   **UI Optimization**:
    *   Use **ResizeObserver** strategies (see **[Video Quality Strategies](./video-strategies.md)**) to request lower-quality streams for participants whose video tiles are small or hidden.
*   **Dynamic Subscriptions**: In very large meetings, you might only subscribe to the top 5-10 active speakers to save CPU and bandwidth on the client device.

### Example: Handling New Participants
```go
meetingClient.OnTracksAvailable(func(tracks []sfu.ITrack) {
    // In a meeting, we usually want to see everyone as they join
    meetingClient.SubscribeTracks(toRequests(tracks))
})
```

---

## 3. Difference in Handling

The SFU handles these cases differently primarily through **Bandwidth Management** and **Subscription Logic**:

| Feature | Live Streaming | Video Conference |
| :--- | :--- | :--- |
| **Primary Goal** | High quality for one source | Low latency and interactivity for all |
| **Subscription** | Broadcaster -> All Viewers | Everyone -> Everyone (Mesh-like via SFU) |
| **Data Channels** | Used for Chat / Reactions | Used for UI Sync / VAD / Control |
| **Quality Control** | Priority on Broadcaster stability | Priority on fair bandwidth distribution |
| **Stats Monitoring** | Focus on Broadcaster's egress | Focus on every participant's health |

## Summary
By combining the building blocks provided by inLive SFU—Signaling, Subscriptions, Data Channels, and Observability—you can tailor the experience to fit your specific application needs.
