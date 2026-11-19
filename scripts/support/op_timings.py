"""Helpers for `hc client call dump-op-timings` output (test scripts only).

`hc` prints op hashes as raw byte arrays; HoloHash base64 is
"u" + base64url(39 bytes).

  op_timings.py hashes DUMP                 integrated op hashes, one per line
  op_timings.py diff NEW OLD                hashes in NEW (integrated) not in OLD
  op_timings.py check DUMP EXPECTED         JSON verdict: every EXPECTED hash is held,
                                            integrated, Accepted and locally validated
  op_timings.py hints EVENTS_JSONL          op hashes from relay events (dht_op refs)
  op_timings.py times DUMP HASHES           JSON {min,max} when_integrated (ms) of HASHES
"""
import base64
import json
import sys


def b64(raw):
    return "u" + base64.urlsafe_b64encode(bytes(raw)).decode().rstrip("=")


def load(path):
    with open(path) as f:
        return {b64(t["op_hash"]): t for t in json.load(f)["timings"]}


def lines(path):
    with open(path) as f:
        return [l.strip() for l in f if l.strip()]


def main(argv):
    cmd = argv[1]
    if cmd == "hashes":
        for h, t in load(argv[2]).items():
            if t["when_integrated"] is not None:
                print(h)
    elif cmd == "diff":
        new, old = load(argv[2]), load(argv[3])
        for h, t in new.items():
            if h not in old and t["when_integrated"] is not None:
                print(h)
    elif cmd == "check":
        held = load(argv[2])
        expected = lines(argv[3])
        missing, not_integrated, not_accepted, not_local = [], [], [], []
        for h in expected:
            t = held.get(h)
            if t is None:
                missing.append(h)
            elif t["when_integrated"] is None:
                not_integrated.append(h)
            elif t["validation_status"] != "Accepted":
                not_accepted.append(h)
            elif t.get("locally_validated") is not True:
                not_local.append(h)
        ok = not (missing or not_integrated or not_accepted or not_local)
        print(json.dumps({
            "expected": len(expected),
            "ok": ok,
            "missing": len(missing),
            "not_integrated": len(not_integrated),
            "not_accepted": len(not_accepted),
            "not_locally_validated": len(not_local),
            "held_total": len(held),
        }))
        return 0 if ok else 1
    elif cmd == "hints":
        seen = []
        for l in lines(argv[2]):
            e = json.loads(l)
            for r in e["refs"]:
                if r["type"] == "dht_op" and r["id"] not in seen:
                    seen.append(r["id"])
        print("\n".join(seen))
    elif cmd == "times":
        held = load(argv[2])
        ts = [held[h]["when_integrated"] // 1000 for h in lines(argv[3])
              if h in held and held[h]["when_integrated"] is not None]
        print(json.dumps({"count": len(ts), "min_ms": min(ts) if ts else None,
                          "max_ms": max(ts) if ts else None}))
    else:
        raise SystemExit(f"unknown command {cmd}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
