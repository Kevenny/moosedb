#!/usr/bin/env python3
"""Gerador de carga trapper para o laboratório (protocolo do zabbix_sender).

Envia 1 valor por item a cada --interval segundos, espalhado uniformemente
no intervalo (fatias de 1 s), em lotes de até --batch valores por conexão.

    docker compose --profile tools run --rm tools python scripts/sender.py \
        --interval 60 --duration 3600

Itens: os mesmos de create_items.py (lab.f[i], lab.u[i], lab.s[i]).
Valores: float = senóide + ruído; uint = contador crescente; str = poucos
estados (como status de serviço). clock = agora; ns = sub-segundo real.
Só usa a biblioteca padrão.
"""
import argparse
import json
import math
import os
import random
import socket
import struct
import sys
import time

SERVER = os.environ.get("ZBX_TRAPPER_HOST", "localhost")
PORT = int(os.environ.get("ZBX_TRAPPER_PORT", "10051"))
STATES = ["ok", "ok", "ok", "ok", "degraded", "running", "running", "stopped"]


def send(server, port, data, timeout=30):
    payload = json.dumps({"request": "sender data", "data": data}).encode()
    packet = b"ZBXD\x01" + struct.pack("<II", len(payload), 0) + payload
    with socket.create_connection((server, port), timeout=timeout) as s:
        s.sendall(packet)
        hdr = b""
        while len(hdr) < 13:
            chunk = s.recv(13 - len(hdr))
            if not chunk:
                raise ConnectionError("resposta incompleta")
            hdr += chunk
        (length, _) = struct.unpack("<II", hdr[5:13])
        body = b""
        while len(body) < length:
            chunk = s.recv(length - len(body))
            if not chunk:
                break
            body += chunk
    resp = json.loads(body)
    info = resp.get("info", "")
    # "processed: 10; failed: 0; total: 10; seconds spent: 0.000123"
    fields = dict(p.strip().split(": ") for p in info.split(";") if ": " in p)
    return int(fields.get("processed", 0)), int(fields.get("failed", 0))


def value_for(kind, i, now):
    if kind == "f":
        return round(50 + 30 * math.sin(now / 3600.0 + i) + random.random() * 2, 4)
    if kind == "u":
        return int(now // 10) * (i % 7 + 1) + i
    return STATES[(i + int(now // 600)) % len(STATES)]


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--host", default="zbxlab-load")
    ap.add_argument("--float", type=int, default=2000, dest="n_float")
    ap.add_argument("--uint", type=int, default=2000, dest="n_uint")
    ap.add_argument("--str", type=int, default=1000, dest="n_str")
    ap.add_argument("--interval", type=float, default=60.0, help="segundos entre valores do mesmo item")
    ap.add_argument("--duration", type=float, default=0, help="segundos de execução (0 = sem fim)")
    ap.add_argument("--batch", type=int, default=1000)
    ap.add_argument("--server", default=SERVER)
    ap.add_argument("--port", type=int, default=PORT)
    args = ap.parse_args()

    keys = [("f", i, f"lab.f[{i}]") for i in range(args.n_float)] + \
           [("u", i, f"lab.u[{i}]") for i in range(args.n_uint)] + \
           [("s", i, f"lab.s[{i}]") for i in range(args.n_str)]
    if not keys:
        sys.exit("nenhum item")
    per_sec = len(keys) / args.interval
    print(f"{len(keys)} itens, 1 valor/item a cada {args.interval:g}s => {per_sec:.1f} valores/s "
          f"para {args.server}:{args.port}", flush=True)

    start = time.time()
    pos, carry = 0, 0.0
    sent = processed = failed = 0
    last_report = start
    tick = start
    while args.duration <= 0 or time.time() - start < args.duration:
        carry += per_sec
        n = int(carry)
        carry -= n
        now = time.time()
        batch = []
        for _ in range(n):
            kind, i, key = keys[pos]
            pos = (pos + 1) % len(keys)
            sec = int(now)
            batch.append({"host": args.host, "key": key, "value": str(value_for(kind, i, now)),
                          "clock": sec, "ns": min(int((now - sec) * 1e9) + random.randint(0, 999), 999999999)})
        for off in range(0, len(batch), args.batch):
            part = batch[off:off + args.batch]
            try:
                p, f = send(args.server, args.port, part)
            except (OSError, ValueError) as e:
                print(f"erro de envio: {e}", file=sys.stderr, flush=True)
                p, f = 0, len(part)
            sent += len(part)
            processed += p
            failed += f
        if now - last_report >= 10:
            el = now - start
            print(f"t={el:7.0f}s enviados={sent} processados={processed} falhas={failed} "
                  f"taxa={processed / el:.1f}/s", flush=True)
            last_report = now
        tick += 1.0
        delay = tick - time.time()
        if delay > 0:
            time.sleep(delay)
    el = time.time() - start
    print(f"FIM: {el:.0f}s enviados={sent} processados={processed} falhas={failed} taxa={processed / el:.1f}/s")


if __name__ == "__main__":
    main()
