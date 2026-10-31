#!/usr/bin/env python3
"""Run backup experiments inside Mininet and write a CSV.

1) start controller:  SDN_MODE=dynamic ryu-manager controller/controller.py
   run:               sudo python3 tests/run_experiments.py --label dynamic
2) restart controller with SDN_MODE=static and rerun with --label static (the baseline)
3) python3 tests/plot_results.py
"""
import argparse
import csv
import json
import os
import subprocess
import sys
import time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "topology"))
from topo import CONTROLLER_URL, build_net          # noqa: E402

SERVER, CLIENT = os.path.join(ROOT, "app", "server.py"), os.path.join(ROOT, "app", "client.py")
SERVERS = "10.0.0.3:9000,10.0.0.4:9000"
SCENARIOS = ["normal", "concurrent", "congestion", "linkfail"]
FIELDS = ["label", "scenario", "size_mb", "rep", "client", "success", "duration_s",
          "throughput_MBps", "reconnects", "retransmitted_chunks", "max_ack_gap_s", "servers_used"]


def make_file(path, mb):
    with open(path, "wb") as f:
        for _ in range(mb):
            f.write(os.urandom(1 << 20))


def start_servers(net):
    net.get("h3").cmd("pkill -f app/server.py")           # hosts share one PID namespace
    for n in ("h3", "h4"):
        h = net.get(n)
        h.cmd("rm -rf /tmp/store_%s" % n)
        h.cmd("python3 %s --port 9000 --store /tmp/store_%s > /tmp/server_%s.log 2>&1 &" % (SERVER, n, n))
    time.sleep(1)


def run_once(net, label, scenario, mb, rep, fail_after):
    start_servers(net)
    clients = ["h1", "h2"] if scenario == "concurrent" else ["h1"]
    procs = []
    if scenario == "congestion":                           # h2 -> h4 UDP flood on the default path A
        net.get("h4").cmd("iperf3 -s -1 -D -p 5201")
        net.get("h2").popen(["iperf3", "-c", "10.0.0.4", "-u", "-b", "9M", "-t", "300", "-p", "5201"],
                            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        time.sleep(5)                                      # let the controller measure it
    for c in clients:
        h = net.get(c)
        f = "/tmp/p24_%s_%dMB_%d.bin" % (c, mb, rep)
        make_file(f, mb)
        procs.append((c, h.popen(
            ["python3", CLIENT, "--file", f, "--servers", SERVERS, "--controller", CONTROLLER_URL,
             "--client-ip", h.IP(), "--max-time", "90"],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)))
    if scenario == "linkfail":
        time.sleep(fail_after)
        net.configLinkStatus("s2", "s4", "down")           # kill the link of path A mid-transfer
    rows = []
    for c, p in procs:
        out, _ = p.communicate()
        try:
            r = json.loads(out.strip().splitlines()[-1])
        except Exception:
            r = {"success": False}
        r.update(label=label, scenario=scenario, size_mb=mb, rep=rep, client=c)
        r["servers_used"] = "|".join(r.get("servers_used", []))
        rows.append(r)
    # cleanup
    if scenario == "linkfail":
        net.configLinkStatus("s2", "s4", "up")
    net.get("h2").cmd("pkill iperf3")
    time.sleep(4)
    return rows


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--label", required=True, help="static | dynamic (must match controller SDN_MODE)")
    ap.add_argument("--sizes", default="10,30")
    ap.add_argument("--reps", type=int, default=3)
    ap.add_argument("--scenarios", default=",".join(SCENARIOS))
    ap.add_argument("--fail-after", type=float, default=3.0)
    a = ap.parse_args()
    os.makedirs(os.path.join(ROOT, "results"), exist_ok=True)
    out = os.path.join(ROOT, "results", "results_%s.csv" % a.label)
    net = build_net()
    time.sleep(3)
    try:
        with open(out, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=FIELDS, extrasaction="ignore")
            w.writeheader()
            for sc in a.scenarios.split(","):
                for mb in map(int, a.sizes.split(",")):
                    for rep in range(a.reps):
                        for row in run_once(net, a.label, sc, mb, rep, a.fail_after):
                            w.writerow(row)
                            f.flush()
                            print(row.get("scenario"), mb, rep, row.get("client"),
                                  "ok" if row.get("success") else "FAIL", row.get("throughput_MBps"))
    finally:
        net.stop()
    print("wrote", out)


if __name__ == "__main__":
    main()
