window.addEventListener("DOMContentLoaded", async () => {
    console.log("🚀 Metrics script running...");

    const redisEl = document.getElementById("redisLatency");
    const stackEl = document.getElementById("stackLatency");
    const lightRedis = document.getElementById("lightDistance");
    const lightStack = document.getElementById("stackLightDistance");

    if (!redisEl || !stackEl) {
        console.error("❌ Missing metric elements in HTML");
        return;
    }

    try {
        // ---------- REDIS LATENCY ----------
        const redisResp = await fetch("/api/redis-latency");
        const redisData = await redisResp.json();
        console.log("📡 Redis data:", redisData);

        let redisMs;
        if (redisData.latency_ms > 1000) {
            redisMs = 1000 / redisData.latency_ms; // Convert req/s → ms/op
        } else {
            redisMs = redisData.latency_ms;
        }

        redisEl.textContent = redisMs.toFixed(3) + " ms";
        console.log("✅ Redis latency displayed:", redisMs.toFixed(3));

        // LIGHT DISTANCE for Redis
        if (lightRedis) {
            const timeSeconds = redisMs / 1000;
            const distanceMeters = 299792458 * timeSeconds;
            lightRedis.textContent = `💡 Light travels ${formatDistance(distanceMeters)} in this time`;
            pulse(lightRedis);
        }

        // ---------- FULL STACK LATENCY ----------
        const stackResp = await fetch("/api/stack-speed");
        const stackData = await stackResp.json();
        console.log("📡 Stack data:", stackData);

        const stackMs = stackData.stack_ms || 0.0;
        stackEl.textContent = stackMs.toFixed(3) + " ms";
        console.log("✅ Stack latency displayed:", stackMs.toFixed(3));

        // LIGHT DISTANCE for Full Stack
        if (lightStack) {
            const timeSeconds = stackMs / 1000;
            const distanceMeters = 299792458 * timeSeconds;
            lightStack.textContent = `💡 Light travels ${formatDistance(distanceMeters)} in this time`;
            pulse(lightStack);
        }

    } catch (err) {
        console.error("🔥 updateMetrics failed:", err);
        redisEl.textContent = "ERR";
        stackEl.textContent = "ERR";
        if (lightRedis) lightRedis.textContent = "💡 ERR";
        if (lightStack) lightStack.textContent = "💡 ERR";
    }
});

// ---------- Helper functions ----------

// Convert meters to readable units
function formatDistance(meters) {
    if (meters < 0.001) return (meters * 1000).toFixed(2) + " mm";
    if (meters < 1) return (meters * 100).toFixed(2) + " cm";
    if (meters < 1000) return meters.toFixed(2) + " m";
    return (meters / 1000).toFixed(2) + " km";
}

// Pulse animation for updates
function pulse(el) {
    el.classList.add("updated");
    setTimeout(() => el.classList.remove("updated"), 400);
}