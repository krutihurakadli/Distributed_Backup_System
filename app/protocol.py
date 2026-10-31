"""Wire protocol shared by client and server.

Frame = 8-byte prefix (!II: header_len, payload_len) + JSON header + binary payload.
Message types:
  C->S  BACKUP_INIT{file_id,name,size,chunk_size,total_chunks}
  S->C  INIT_OK{have:[chunk indexes already stored]}      <- enables resume
  C->S  CHUNK{index,sha256} + payload
  S->C  ACK{index} | NACK{index}
  C->S  COMPLETE
  S->C  VERIFY_OK | VERIFY_FAIL{missing:[...]}
"""
import hashlib
import json
import struct

PREFIX = struct.Struct("!II")
MAX_HEADER = 1 << 20
MAX_PAYLOAD = 64 << 20
DEFAULT_CHUNK = 256 * 1024
DEFAULT_PORT = 9000


def recv_exact(sock, n):
    buf = bytearray()
    while len(buf) < n:
        part = sock.recv(n - len(buf))
        if not part:
            raise ConnectionError("peer closed connection")
        buf += part
    return bytes(buf)


def send_msg(sock, header, payload=b""):
    h = json.dumps(header).encode()
    sock.sendall(PREFIX.pack(len(h), len(payload)) + h + payload)


def recv_msg(sock):
    hl, pl = PREFIX.unpack(recv_exact(sock, PREFIX.size))
    if hl > MAX_HEADER or pl > MAX_PAYLOAD:
        raise ValueError("oversized frame")
    header = json.loads(recv_exact(sock, hl))
    payload = recv_exact(sock, pl) if pl else b""
    return header, payload


def sha256_hex(data):
    return hashlib.sha256(data).hexdigest()


def file_sha256(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for block in iter(lambda: f.read(1 << 20), b""):
            h.update(block)
    return h.hexdigest()
