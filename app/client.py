#!/usr/bin/env python3
"""Backup client: chunked TCP transfer, checksums, resume after failure, SDN controller hooks."""
import argparse
import json
import logging
import os
import socket
import sys
import time
import urllib.request

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from protocol import (DEFAULT_CHUNK, file_sha256, recv_msg, send_msg, sha256_hex)

log = logging.getLogger("client")


class BackupClient:
    def __init__(self, a):
        self.a = a
        self.size = os.path.getsize(a.file)
        self.total = max(1, -(-self.size // a.chunk_size))
        self.file_id = file_sha256(a.file)
        self.name = os.path.basename(a.file)
        self.fallback = [s.split(":") for s in a.servers.split(",")]
        self.client_ip = a.client_ip or self._detect_ip()
        self.attempt = 0
        self.chunks_sent = 0
        self.ack_times = []
        self.servers_used = []

    def _detect_ip(self):
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect((self.fallback[0][0], int(self.fallback[0][1])))
        ip = s.getsockname()[0]
        s.close()
        return ip

    # ---- SDN controller channel (REST over the management network) ----
    def _http(self, method, path, body=None):
        if not self.a.controller:
            return None
        data = json.dumps(body).encode() if body is not None else None
        req = urllib.request.Request(self.a.controller.rstrip("/") + path, data=data, method=method,
                                     headers={"Content-Type": "application/json"})
        try:
            with urllib.request.urlopen(req, timeout=2) as r:
                return json.loads(r.read())
        except Exception as e:
            log.warning("controller call %s failed: %s", path, e)
            return None

    def choose_server(self):
        r = self._http("GET", "/select?client=%s" % self.client_ip)
        if r and "server" in r:
            return r["server"], int(r["port"]), r.get("path", "?")
        host, port = self.fallback[self.attempt % len(self.fallback)]
        return host, int(port), "none"

    def notify(self, event, server):
        self._http("POST", "/event", {"event": event, "client": self.client_ip, "server": server})

    # ---- transfer ----
    def transfer(self, ip, port):
        a = self.a
        sock = socket.create_connection((ip, port), timeout=a.connect_timeout)
        sock.settimeout(a.io_timeout)
        try:
            send_msg(sock, {"type": "BACKUP_INIT", "file_id": self.file_id, "name": self.name,
                            "size": self.size, "chunk_size": a.chunk_size, "total_chunks": self.total})
            h, _ = recv_msg(sock)
            if h.get("type") != "INIT_OK":
                raise ConnectionError("bad INIT reply")
            have = set(h["have"])
            pending = [i for i in range(self.total) if i not in have]
            log.info("to %s:%d need %d/%d chunks (resume)", ip, port, len(pending), self.total)
            with open(a.file, "rb") as f:
                while pending:
                    batch = pending[:a.window]
                    for i in batch:
                        f.seek(i * a.chunk_size)
                        data = f.read(a.chunk_size)
                        send_msg(sock, {"type": "CHUNK", "index": i, "sha256": sha256_hex(data)}, data)
                        self.chunks_sent += 1
                    acked = set()
                    for _ in batch:
                        rh, _ = recv_msg(sock)
                        if rh["type"] == "ACK":
                            acked.add(rh["index"])
                            self.ack_times.append(time.time())
                    pending = [i for i in pending if i not in acked]
            send_msg(sock, {"type": "COMPLETE"})
            rh, _ = recv_msg(sock)
            return rh.get("type") == "VERIFY_OK"
        finally:
            sock.close()

    def run(self):
        a = self.a
        t0 = time.time()
        self.ack_times.append(t0)
        ok = False
        failures = 0
        while time.time() - t0 < a.max_time:
            ip, port, path = self.choose_server()
            self.servers_used.append("%s:%d[%s]" % (ip, port, path))
            self.notify("started", ip)
            try:
                ok = self.transfer(ip, port)
            except (OSError, ConnectionError, ValueError) as e:     # incl. socket.timeout
                log.warning("transfer to %s interrupted: %s", ip, e)
                ok = False
            if ok:
                self.notify("finished", ip)
                break
            self.notify("failed", ip)
            failures += 1
            self.attempt += 1
            time.sleep(min(0.5 + 0.5 * failures, 3))
        dur = time.time() - t0
        gaps = [b - a_ for a_, b in zip(self.ack_times, self.ack_times[1:])] or [0]
        res = {"file": self.name, "client": self.client_ip, "size_bytes": self.size, "success": ok,
               "duration_s": round(dur, 3),
               "throughput_MBps": round(self.size / 1e6 / dur, 3) if ok else 0,
               "reconnects": failures, "chunks_sent": self.chunks_sent,
               "retransmitted_chunks": max(0, self.chunks_sent - self.total),
               "max_ack_gap_s": round(max(gaps), 3), "servers_used": self.servers_used}
        return res


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--file", required=True)
    ap.add_argument("--servers", required=True, help="host:port,host:port (fallback list)")
    ap.add_argument("--controller", default="", help="e.g. http://192.168.100.254:8080")
    ap.add_argument("--client-ip", default="")
    ap.add_argument("--chunk-size", type=int, default=DEFAULT_CHUNK)
    ap.add_argument("--window", type=int, default=8, help="chunks in flight before reading ACKs")
    ap.add_argument("--connect-timeout", type=float, default=3)
    ap.add_argument("--io-timeout", type=float, default=5)
    ap.add_argument("--max-time", type=float, default=120)
    a = ap.parse_args()
    logging.basicConfig(level=logging.INFO, stream=sys.stderr, format="%(asctime)s %(name)s %(message)s")
    print(json.dumps(BackupClient(a).run()))      # last stdout line = machine-readable result
