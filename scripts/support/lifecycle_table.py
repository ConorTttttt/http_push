"""Summarize docs/results-data/lifecycle/*.json as Markdown tables."""
import glob
import json
import os
import statistics
import sys

ORDER = ["baseline", "rto8", "patched-noreq", "round", "zero", "zero-rto8"]


def fmt(vals, unit=""):
    vals = [v for v in vals if v is not None]
    if not vals:
        return "–"
    if len(vals) == 1:
        return f"{vals[0]}{unit}"
    return f"{int(statistics.median(vals))}{unit} ({min(vals)}–{max(vals)})"


def main(d):
    runs = {}
    for p in sorted(glob.glob(os.path.join(d, "*.json"))):
        r = json.load(open(p))
        runs.setdefault(r["label"], []).append(r)
    labels = [l for l in ORDER if l in runs] + [l for l in runs if l not in ORDER]
    print("| Variant | Runs (failed) | Conductor ready, ms | Peers known, ms | First gossip round started, ms | First op fetched, ms | All required ops validated, ms | Required ops | C CPU until synced, ms |")
    print("|---|---|---|---|---|---|---|---|---|")
    for l in labels:
        ok = [r for r in runs[l] if not r.get("failed")]
        failed = len(runs[l]) - len(ok)
        ds = [r["data_sync"] for r in ok]
        print(f"| {l} | {len(runs[l])} ({failed}) "
              f"| {fmt([x['conductor_ready_ms'] for x in ds])} "
              f"| {fmt([x['peer_discovery_ms'] for x in ds])} "
              f"| {fmt([x['first_gossip_initiated_ms'] for x in ds])} "
              f"| {fmt([x.get('first_required_op_fetched_ms') for x in ds])} "
              f"| {fmt([x['all_required_ops_validated_ms'] for x in ds])} "
              f"| {fmt([x['required_ops'] for x in ds])} "
              f"| {fmt([r['cpu_ms']['c_until_synced'] for r in ok])} |")
    print()
    print("| Variant | Ops authored by A | Hints received by C | Detection p50 / max, ms | C HTTP sync, ms | HTTP in / out, B | Kitsune2 recv / send on open connections, B | Zero-arc gets: attempts, total ms |")
    print("|---|---|---|---|---|---|---|---|")
    for l in labels:
        ok = [r for r in runs[l] if not r.get("failed")]
        if not ok:
            continue
        n = [r["notification"] for r in ok]
        k2 = [r["bytes"]["kitsune2_open_connections"] for r in ok]
        gets = []
        for r in ok:
            g = r["data_sync"].get("zero_arc_gets") or []
            if g:
                gets.append("; ".join(f"{len(x['attempts'])}×/{x['total_ms']}" for x in g))
        print(f"| {l} | {fmt([r['ops_authored_by_a'] for r in ok])} "
              f"| {fmt([x['c_http_sync']['delivered'] for x in n])} "
              f"| {fmt([x['detection_b_or_d_integrated_to_relay_journal'].get('p50_ms') for x in n])} / "
              f"{fmt([x['detection_b_or_d_integrated_to_relay_journal'].get('max_ms') for x in n])} "
              f"| {fmt([x['c_http_sync_ms'] for x in n])} "
              f"| {fmt([r['bytes']['http_in'] for r in ok])} / {fmt([r['bytes']['http_out'] for r in ok])} "
              f"| {fmt([x['recv_bytes'] for x in k2])} / {fmt([x['send_bytes'] for x in k2])} "
              f"| {' · '.join(gets) if gets else '–'} |")


if __name__ == "__main__":
    main(sys.argv[1])
