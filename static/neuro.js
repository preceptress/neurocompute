document.addEventListener("DOMContentLoaded", () => {

    // =====================================================
    // DOM REFERENCES
    // =====================================================
    const redisEl = document.getElementById("redisLatency");
    const btn = document.getElementById("refreshBtn");

    if (!redisEl) {
        console.warn("redisLatency element not found");
        return;
    }

    // =====================================================
    // MEASUREMENT FUNCTION (single source of truth)
    // =====================================================
    async function measureRedisLatency() {
        try {
            if (btn) {
                btn.disabled = true;
                btn.textContent = "Measuring…";
            }

            const start = performance.now();

            const resp = await fetch("/api/redis-latency");
            const data = await resp.json();

            const end = performance.now();

            // Backend reports milliseconds
            const ms = data.latency_ms;

            // Convert to nanoseconds for display
            const ns = Math.round(ms * 1_000_000);

            redisEl.textContent = `${ns.toLocaleString()} ns`;
            redisEl.title = `${ms.toFixed(6)} ms (server-reported)`;

            // Optional: log total round-trip
            console.log(
                `Redis backend: ${ms.toFixed(6)} ms, fetch RTT: ${(end - start).toFixed(2)} ms`
            );

        } catch (err) {
            console.warn("Redis latency measurement failed", err);
            redisEl.textContent = "—";
        } finally {
            if (btn) {
                btn.textContent = "Measure Redis Speed";
                btn.disabled = false;
            }
        }
    }

    // =====================================================
    // INITIAL MEASUREMENT (PAGE LOAD)
    // =====================================================
    measureRedisLatency();

    // =====================================================
    // USER-TRIGGERED MEASUREMENT
    // =====================================================
    if (btn) {
        btn.addEventListener("click", measureRedisLatency);
    }

});