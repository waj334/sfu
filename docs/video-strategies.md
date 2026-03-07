# Video Quality Strategies (UI/UX)

To provide the best experience for your users, your application should intelligently manage video quality based on how the video is being consumed. This guide covers high-level strategies for integrating your UI with the SFU's adaptive bitrate features.

## Adaptive Quality Decisions

The SFU makes quality decisions based on several factors, and your client application can provide critical hints to optimize this process.

### 1. Bandwidth-Aware Selection
The SFU's built-in bandwidth estimator automatically adjusts the quality level (Low, Mid, High) when the network is congested. You can observe these changes or even manually override them using `client.SetQuality()`.

### 2. Viewport-Based Optimization (ResizeObserver)
A common waste of resources is sending a high-resolution (720p) stream when it is only being displayed in a small 200px thumbnail. You can use the `ResizeObserver` API in the browser to inform the SFU of the actual size of the video player.

```javascript
const observer = new ResizeObserver((entries) => {
    for (let entry of entries) {
        const { width, height } = entry.contentRect;
        // Send these dimensions to the SFU via an internal data channel 
        // or a custom signaling message.
        sendVideoSize(trackId, width, height);
    }
});

observer.observe(videoElement);
```

On the server, the SFU will use these dimensions to select the closest available quality layer (Simulcast) or temporal/spatial layer (SVC) that fits the player size.

### 3. Visibility-Based Optimization (IntersectionObserver)
In large meetings with many participants, many video players might be off-screen (scrolled away). Streaming these hidden videos wastes both CPU and bandwidth.

Use the `IntersectionObserver` API to detect when a video player is no longer visible and tell the SFU to pause that specific track.

```javascript
const observer = new IntersectionObserver((entries) => {
    entries.forEach((entry) => {
        if (!entry.isIntersecting) {
            // Video is off-screen, tell SFU to pause/low-quality
            setTrackVisibility(trackId, false);
        } else {
            // Video is back on-screen
            setTrackVisibility(trackId, true);
        }
    });
});

observer.observe(videoElement);
```

## Simulcast vs. SVC: Which to Choose?

When building your app, you'll need to decide between **Simulcast** (sending multiple tracks) and **SVC** (sending one layered track).

| Feature | Simulcast (H.264) | SVC (VP9/AV1) |
| :--- | :--- | :--- |
| **CPU (Client)** | High (encoding multiple streams) | Moderate (encoding one layered stream) |
| **Bandwidth (Client)** | Higher (redundant header info) | Very Efficient |
| **Compatibility** | Excellent (supported by all browsers) | Good (Chromium/Safari support L3T3) |
| **SFU Overhead** | Low (simple forwarding) | High (layer stripping/bitstream manipulation) |

### Recommendation
*   Use **Simulcast** if you want maximum compatibility with older devices and the simplest server-side logic.
*   Use **SVC** if you want to optimize for bandwidth efficiency and are targeting modern browsers.
