# Project 24 – Distributed Backup System (TCP + SDN)

Clients split files into 256 KB chunks and back them up to one of two servers over TCP.
A Ryu controller picks server + path per session, reroutes on link failure / congestion, and
logs every decision. Interrupted transfers resume from the server's chunk list.

## Architecture
```
            +--- s2 ---+                 h1,h2 clients   h3,h4 backup servers
   h1,h2 -- s1          s4 -- h3,h4      Path A = s1-s2-s4   Path B = s1-s3-s4  (10 Mbps, 5 ms each link)
            +--- s3 ---+                 Management net 192.168.100.0/24 (hosts <-> controller REST :8080)
```
**Socket <-> SDN integration:** client calls `GET /select` (controller returns server+path and installs
flows) and reports `started/finished/failed` via `POST /event`; the controller marks failed servers
suspect and reroutes on link-down (OpenFlow port-status) or congestion (port-stats polling).

**Protocol:** length-prefixed frames (JSON header + binary payload), see `app/protocol.py`.
`BACKUP_INIT -> INIT_OK{have}` -> `CHUNK/ACK` (SHA-256 per chunk) -> `COMPLETE -> VERIFY_OK` (whole-file SHA-256).

## Setup (Ubuntu 22.04 VM)
```bash
sudo apt install -y mininet openvswitch-switch iperf3 python3-venv
python3.9 -m venv ~/ryuenv && source ~/ryuenv/bin/activate && pip install -r requirements.txt
```

## Manual demo
```bash
# T1 (venv)           SDN_MODE=dynamic ryu-manager controller/controller.py
# T2 (system python)  sudo python3 topology/topo.py
mininet> h1 ping -c2 h3
mininet> h3 python3 app/server.py --port 9000 --store /tmp/s3 &
mininet> h4 python3 app/server.py --port 9000 --store /tmp/s4 &
mininet> sh head -c 50000000 /dev/urandom > /tmp/big.bin
mininet> h1 python3 app/client.py --file /tmp/big.bin --servers 10.0.0.3:9000,10.0.0.4:9000 \
           --controller http://192.168.100.254:8080 --client-ip 10.0.0.1 &
mininet> link s2 s4 down          # watch controller log: port_down -> reroute
mininet> sh ovs-ofctl -O OpenFlow13 dump-flows s1
```

## Experiments (static baseline vs dynamic)
```bash
SDN_MODE=dynamic ryu-manager controller/controller.py     # T1
sudo python3 tests/run_experiments.py --label dynamic      # T2   (Ctrl-C T1 afterwards)
SDN_MODE=static  ryu-manager controller/controller.py     # T1 again
sudo python3 tests/run_experiments.py --label static       # T2
python3 tests/plot_results.py                              # plots -> results/*.png
```
Scenarios: `normal`, `concurrent` (h1+h2), `congestion` (9 Mbps UDP on path A), `linkfail` (s2-s4 down mid-transfer).
Metrics: throughput, completion time, longest ACK stall (recovery proxy), reconnects, retransmitted chunks, success rate.
Controller decisions: `results/controller_events.jsonl`.

## Known limitations / notes
* Controller assumes the fixed port map in `topology/topo.py` (no LLDP discovery).
* One active assignment per client host (keyed by client IP).
* `pkill` in the runner kills all `iperf3`/`server.py` processes (Mininet hosts share the PID namespace).
