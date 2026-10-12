#!/usr/bin/env python3
"""Cria, via API do Zabbix 7.0, um host com N itens trapper para o laboratório.

    docker compose --profile tools run --rm tools python scripts/create_items.py \
        --float 2000 --uint 2000 --str 1000

Chaves criadas: lab.f[i] (float), lab.u[i] (unsigned), lab.s[i] (character).
Idempotente: itens já existentes são mantidos. Só usa a biblioteca padrão.

Opções de housekeeping (necessárias com histórico em MooseDB — o housekeeper
de histórico faz DELETE ... WHERE, que a engine recusa):
    --history-hk off   -> housekeeping.update hk_history_mode=0
    --history-hk on    -> hk_history_mode=1 (padrão do Zabbix)
"""
import argparse
import json
import os
import sys
import time
import urllib.error
import urllib.request

API_URL = os.environ.get("ZBX_API_URL", "http://localhost:8088/api_jsonrpc.php")
API_USER = os.environ.get("ZBX_API_USER", "Admin")
API_PASSWORD = os.environ.get("ZBX_API_PASSWORD", "zabbix")

VALUE_TYPES = (("float", 0, "lab.f"), ("uint", 3, "lab.u"), ("str", 1, "lab.s"))


class Api:
    def __init__(self, url):
        self.url = url
        self.token = None
        self.id = 0

    def call(self, method, params):
        self.id += 1
        body = json.dumps({"jsonrpc": "2.0", "method": method, "params": params, "id": self.id}).encode()
        req = urllib.request.Request(self.url, data=body, headers={"Content-Type": "application/json-rpc"})
        if self.token:
            req.add_header("Authorization", "Bearer " + self.token)
        with urllib.request.urlopen(req, timeout=120) as r:
            resp = json.loads(r.read())
        if "error" in resp:
            raise RuntimeError(f"{method}: {resp['error']}")
        return resp["result"]

    def login(self, user, password, wait=180):
        deadline = time.time() + wait
        while True:
            try:
                self.token = self.call("user.login", {"username": user, "password": password})
                return
            except (urllib.error.URLError, ConnectionError, RuntimeError) as e:
                if time.time() > deadline:
                    raise
                print(f"aguardando API ({e})...", file=sys.stderr)
                time.sleep(5)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--host", default="zbxlab-load")
    ap.add_argument("--group", default="ZBX Lab")
    ap.add_argument("--float", type=int, default=2000, dest="n_float")
    ap.add_argument("--uint", type=int, default=2000, dest="n_uint")
    ap.add_argument("--str", type=int, default=1000, dest="n_str")
    ap.add_argument("--history", default="31d", help="período de histórico por item")
    ap.add_argument("--trends", default="365d", help="período de trends (float/uint)")
    ap.add_argument("--batch", type=int, default=500)
    ap.add_argument("--history-hk", choices=["on", "off"], help="liga/desliga o housekeeping de histórico")
    ap.add_argument("--out", default="results/items.json", help="lista de itens criados (itemid, key, value_type)")
    args = ap.parse_args()

    api = Api(API_URL)
    api.login(API_USER, API_PASSWORD)
    print("login ok em", API_URL)

    if args.history_hk:
        mode = 1 if args.history_hk == "on" else 0
        api.call("housekeeping.update", {"hk_history_mode": mode})
        print(f"housekeeping de histórico: hk_history_mode={mode}")

    groups = api.call("hostgroup.get", {"filter": {"name": [args.group]}, "output": ["groupid"]})
    groupid = groups[0]["groupid"] if groups else api.call("hostgroup.create", {"name": args.group})["groupids"][0]

    hosts = api.call("host.get", {"filter": {"host": [args.host]}, "output": ["hostid"]})
    if hosts:
        hostid = hosts[0]["hostid"]
    else:
        hostid = api.call("host.create", {"host": args.host, "groups": [{"groupid": groupid}]})["hostids"][0]
    print(f"host {args.host} hostid={hostid}")

    existing = {i["key_"] for i in api.call("item.get", {"hostids": hostid, "output": ["key_"]})}
    wanted = {"float": args.n_float, "uint": args.n_uint, "str": args.n_str}
    todo = []
    for name, vt, prefix in VALUE_TYPES:
        for i in range(wanted[name]):
            key = f"{prefix}[{i}]"
            if key in existing:
                continue
            item = {"hostid": hostid, "name": f"lab {name} {i}", "key_": key, "type": 2,
                    "value_type": vt, "history": args.history}
            if vt in (0, 3):
                item["trends"] = args.trends
            todo.append(item)

    t0 = time.time()
    for off in range(0, len(todo), args.batch):
        api.call("item.create", todo[off:off + args.batch])
        print(f"  criados {min(off + args.batch, len(todo))}/{len(todo)}", flush=True)
    print(f"{len(todo)} itens novos em {time.time() - t0:.1f}s ({len(existing)} já existiam)")

    items = api.call("item.get", {"hostids": hostid, "output": ["itemid", "key_", "value_type"],
                                  "search": {"key_": "lab."}, "startSearch": True})
    os.makedirs(os.path.dirname(args.out) or ".", exist_ok=True)
    with open(args.out, "w") as f:
        json.dump({"host": args.host, "hostid": hostid, "items": items}, f)
    print(f"{len(items)} itens lab.* no host; lista em {args.out}")


if __name__ == "__main__":
    main()
