#!/usr/bin/env python3
"""Multithreaded TCP backup server: stores chunks, verifies checksums, supports resume."""
import argparse
import logging
import os
import socket
import string
import threading

from protocol import (DEFAULT_PORT, file_sha256, recv_msg, send_msg, sha256_hex)

log = logging.getLogger("server")
HEX = set(string.hexdigits.lower())


class BackupServer:
    def __init__(self, host, port, store):
        self.host, self.port, self.store = host, port, store
        os.makedirs(store, exist_ok=True)

    def serve(self):
        srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        srv.bind((self.host, self.port))
        srv.listen(64)
        log.info("listening on %s:%d store=%s", self.host, self.port, self.store)
        while True:
            conn, addr = srv.accept()
            threading.Thread(target=self.handle, args=(conn, addr), daemon=True).start()

    def handle(self, conn, addr):
        conn.settimeout(30)
        fdir = meta = None
        try:
            while True:
                hdr, payload = recv_msg(conn)
                t = hdr.get("type")
                if t == "BACKUP_INIT":
                    fid = hdr["file_id"]
                    if len(fid) != 64 or not set(fid) <= HEX:
                        raise ValueError("bad file_id")
                    fdir = os.path.join(self.store, fid)
                    os.makedirs(os.path.join(fdir, "chunks"), exist_ok=True)
                    meta = {"name": os.path.basename(hdr["name"]),
                            "total": int(hdr["total_chunks"]), "file_id": fid}
                    have = sorted(int(n.split(".")[0]) for n in
                                  os.listdir(os.path.join(fdir, "chunks")) if n.endswith(".chunk"))
                    log.info("%s INIT %s have=%d/%d", addr[0], meta["name"], len(have), meta["total"])
                    send_msg(conn, {"type": "INIT_OK", "have": have})
                elif t == "CHUNK" and fdir:
                    idx = int(hdr["index"])
                    if sha256_hex(payload) != hdr["sha256"]:
                        log.warning("checksum mismatch chunk %d", idx)
                        send_msg(conn, {"type": "NACK", "index": idx})
                        continue
                    p = os.path.join(fdir, "chunks", "%08d.chunk" % idx)
                    with open(p + ".tmp", "wb") as f:
                        f.write(payload)
                    os.replace(p + ".tmp", p)          # atomic: partial chunks never counted
                    send_msg(conn, {"type": "ACK", "index": idx})
                elif t == "COMPLETE" and fdir:
                    send_msg(conn, self.assemble(fdir, meta))
                else:
                    raise ValueError("unexpected message %r" % t)
        except (ConnectionError, OSError, ValueError, KeyError) as e:
            log.info("%s connection ended: %s", addr[0], e)
        finally:
            conn.close()

    def assemble(self, fdir, meta):
        missing = [i for i in range(meta["total"])
                   if not os.path.exists(os.path.join(fdir, "chunks", "%08d.chunk" % i))]
        if missing:
            return {"type": "VERIFY_FAIL", "missing": missing}
        out = os.path.join(fdir, meta["name"])
        with open(out, "wb") as o:
            for i in range(meta["total"]):
                with open(os.path.join(fdir, "chunks", "%08d.chunk" % i), "rb") as c:
                    o.write(c.read())
        ok = file_sha256(out) == meta["file_id"]
        log.info("assembled %s verify=%s", out, ok)
        return {"type": "VERIFY_OK"} if ok else {"type": "VERIFY_FAIL", "missing": []}


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", default="0.0.0.0")
    ap.add_argument("--port", type=int, default=DEFAULT_PORT)
    ap.add_argument("--store", default="./backup_store")
    a = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
    BackupServer(a.host, a.port, a.store).serve()
