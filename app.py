from flask import Flask, render_template
import time, os

app = Flask(__name__, template_folder="templates", static_folder="static")

MAX_MS = 0.50  # gauge top-end in milliseconds (sub-millisecond scale)

def sample_ms():
    # Tiny bit of work so we never hit 0.0000
    start = time.perf_counter()
    for _ in range(3000):
        pass
    ms = (time.perf_counter() - start) * 1000.0
    return max(ms, 0.001)

@app.route("/")
def index():
    elapsed_ms = sample_ms()
    # 1 flap ≈ 4.35 ms (230 flaps/sec)
    bee_flaps = elapsed_ms / 4.35
    return render_template(
        "index.html",
        process_time=f"{elapsed_ms:.4f}",   # e.g., "0.2475"
        max_ms=f"{MAX_MS:.2f}",             # e.g., "0.50"
        bee_flaps=f"{bee_flaps:.1f}"        # e.g., "2.3"
    )
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 5000)), debug=True)
