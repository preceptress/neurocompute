#!/usr/bin/env python3
import os, time, subprocess, sys

SLEEP_IDLE = float(os.getenv("SLEEP_IDLE", "20"))     # when no work
SLEEP_BUSY = float(os.getenv("SLEEP_BUSY", "0.5"))    # when work exists
SWEEP_BATCH = int(os.getenv("SWEEP_BATCH", "200"))
SCORE_BATCH = int(os.getenv("SCORE_BATCH", "10"))

def run(cmd: list[str]) -> tuple[int, str]:
    p = subprocess.run(cmd, capture_output=True, text=True)
    out = (p.stdout or "") + (p.stderr or "")
    return p.returncode, out.strip()

def main():
    print("ALZ Agent Runner starting...")
    while True:
        did_work = False

        # 1) Sweep: mark rows as checked (queued)
        code, out = run(["python3", "agent_step3_sweep.py"])
        if code == 0:
            # If your sweep script prints how many it updated, parse it later.
            did_work = True
        else:
            print("SWEEP ERROR:\n", out, file=sys.stderr)

        # 2) LLM score: score a small batch
        env = os.environ.copy()
        env["AGENT_LIMIT"] = str(SCORE_BATCH)
        p = subprocess.run(["python3", "agent_step4_openai_score.py"], env=env, capture_output=True, text=True)
        out2 = (p.stdout or "") + (p.stderr or "")
        if p.returncode == 0:
            if "scored" in out2:
                did_work = True
            print(out2.strip())
        else:
            print("SCORE ERROR:\n", out2, file=sys.stderr)

        time.sleep(SLEEP_BUSY if did_work else SLEEP_IDLE)

if __name__ == "__main__":
    main()

