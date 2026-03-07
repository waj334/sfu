# Signaling & Negotiation

After adding a client to a room, the client must establish a WebRTC connection with the SFU server. This is achieved through a process called **Signaling**, where the client and server exchange information to establish the connection. 

WebRTC signaling primarily involves exchanging **SDP (Session Description Protocol)** for media capabilities and **ICE (Interactive Connectivity Establishment)** candidates for network routing.

## Perfect Negotiation

Renegotiation can be initiated by either the client or the SFU. However, a conflict can occur if both sides attempt to renegotiate simultaneously—for example, when a client adds a new track at the exact moment the SFU is trying to add a track from another participant.

To prevent these conflicts, inLive SFU follows the **[Perfect Negotiation](https://developer.mozilla.org/en-US/docs/Web/API/WebRTC_API/Perfect_negotiation)** pattern. This ensures a stable, predictable flow where one side is designated as the "polite" peer and the other as the "impolite" peer. The SFU handles this logic internally, so the client only needs to follow the callbacks.

## The Negotiation Flow

### 1. Initial Connection
To establish the first connection, the client and server follow these steps:

1.  **Register Callbacks**: Before starting, the client should register listeners for these events:
    *   `OnRenegotiation`: Triggered when the SFU needs the client to perform a new negotiation.
    *   `OnAllowedRemoteNegotiation`: Triggered when the SFU is ready for the client to initiate its own negotiation.
    *   `OnTracksAdded`: Triggered when the client adds tracks; the client must then specify the track source type (e.g., `media` or `screen`).
    *   `OnTracksAvailable`: Triggered when new tracks are available from other participants for subscription.
    *   `OnIceCandidate`: Triggered when the SFU generates a new ICE candidate that must be sent to the client.

2.  **Generate & Send Offer**: The client generates a WebRTC Offer and sends it to the server (via WebSocket, REST, etc.).
3.  **Process Offer**: On the server, call `client.Negotiate(offer)`. This method processes the offer and returns an **SDP Answer**, which you must send back to the client.
4.  **Exchange ICE Candidates**: As the SFU generates candidates, they are sent to the client via the `OnIceCandidate` callback. The client should similarly send its own candidates to the server using `client.AddICECandidate(candidate)`.
5.  **Set Track Sources**: After negotiation, for any tracks added by the client, the server will trigger `OnTracksAdded`. The client **must** call `client.SetTracksSourceType()` to categorize them (e.g., `sfu.TrackTypeMedia` or `sfu.TrackTypeScreen`).

### 2. Adding or Removing Tracks (Renegotiation)
When adding a new track (e.g., starting screen sharing) after the initial connection:

1.  **Check Status**: Call `client.IsAllowNegotiation()`. If it returns `true`, you can start. If `false`, the SFU is currently busy with its own negotiation; you should wait for the `OnAllowedRemoteNegotiation` event.
2.  **Negotiate**: Once allowed, the client generates a new Offer and calls `client.Negotiate(offer)` again.
3.  **Handle SFU-Initiated Changes**: If another user joins and the SFU wants to send you their track, the `OnRenegotiation` callback will fire. Your client should then create a new Offer and send it to `client.Negotiate()`.

## Next
- [Publishing Media Tracks](./publishing-media.md)