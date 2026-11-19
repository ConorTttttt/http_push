"""Render three-node results.json files as Markdown tables (docs/results.md).

  python3 scripts/support/results_table.py .demo/three-node-*/results.json
"""
import json
import sys


def g(d, *path, default="–"):
    for p in path:
        if not isinstance(d, dict) or p not in d or d[p] is None:
            return default
        d = d[p]
    return d


def main(paths):
    runs = []
    for p in paths:
        with open(p) as f:
            r = json.load(f)
        r["_path"] = p
        runs.append(r)

    print("| Run | Detection (B integrated → relay journal), ms p50 / max | "
          "Notification (journal → online client, SSE), ms p50 / max | "
          "End-to-end (B integrated → online client), ms p50 / max | Hint sources |")
    print("|---|---|---|---|---|")
    for r in runs:
        if not r.get("synced", True) or "hinted_ops_detection_ms" not in r:
            continue
        name = f"{r['mode']}, C bootstrap {r.get('c_bootstrap_on_restart', 'on')}, peer hint {r.get('peer_hint', 'on')}"
        det = r["hinted_ops_detection_ms"]
        lat = g(r, "observer_sse", "latency", default={})
        print(f"| {name} | {det.get('p50_ms', '–')} / {det.get('max_ms', '–')} | "
              f"{g(lat, 'relay_to_app', 'p50_ms')} / {g(lat, 'relay_to_app', 'max_ms')} | "
              f"{g(lat, 'origin_to_app', 'p50_ms')} / {g(lat, 'origin_to_app', 'max_ms')} | "
              f"{', '.join(f'{k}: {v}' for k, v in r.get('hint_sources', {}).items())} |")

    print()
    print("| Run | Ops authored on A / new on B / hinted to C / validated by C | "
          "C hint sync over HTTP (ms, requests, bytes in) | Peer-hint request (bytes in) | "
          "C conductor ready, ms | First op received / all integrated, ms after start | "
          "Gossip attempts after restart |")
    print("|---|---|---|---|---|---|---|")
    for r in runs:
        name = f"{r['mode']}, C bootstrap {r.get('c_bootstrap_on_restart', 'on')}, peer hint {r.get('peer_hint', 'on')}"
        if not r.get("synced", True):
            chk = r.get("c_check", {})
            print(f"| {name} | – / – / {chk.get('expected', '–')} / "
                  f"{chk.get('expected', 0) - chk.get('missing', 0)} | – | – | – | "
                  f"not caught up within {r.get('timeout_s')} s | – |")
            continue
        hs = r["c_hint_sync"]
        ph = r["c_phases"]
        attempts = "; ".join(
            f"{'ok' if a['event'].startswith('Initiated') else 'FAIL'} {a['target'].split(' ')[0]}"
            for a in r.get("c_gossip_attempts", []))
        print(f"| {name} | {r['ops_new_on_a']} / {r['ops_new_on_b']} / {r['ops_hinted_to_c']} / "
              f"{r['ops_validated_by_c']} | {hs['duration_ms']}, {hs['transfer']['requests']}, "
              f"{hs['transfer']['bytes_received']} | {r['c_peer_hint_request']['bytes_received']} | "
              f"{r.get('c_conductor_ready_ms', '–')} | {ph['first_received_ms']} / "
              f"{ph['last_integrated_ms']} | {attempts} |")


if __name__ == "__main__":
    main(sys.argv[1:])
