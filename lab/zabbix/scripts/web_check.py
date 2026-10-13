#!/usr/bin/env python3
"""Verifica páginas do frontend que leem history* (login por formulário + cookie).

    docker compose --profile tools run --rm -w /lab/scripts tools python web_check.py

  * chart.php (gráfico de 1 item, 1 h)  -> espera image/png
  * zabbix.php?action=latest.view       -> "Latest data" (lastvalue via CHistoryManager)
"""
import http.cookiejar
import json
import os
import time
import urllib.parse
import urllib.request

BASE = os.environ.get("ZBX_WEB_URL", "http://zabbix-web:8080")
ITEMS = os.environ.get("ZBX_ITEMS_JSON", "/lab/results/items.json")


def main():
    cj = http.cookiejar.CookieJar()
    op = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(cj))
    data = urllib.parse.urlencode({"name": "Admin", "password": "zabbix", "enter": "Sign in"}).encode()
    op.open(BASE + "/index.php", data, timeout=60).read()
    meta = json.load(open(ITEMS))
    f = next(i for i in meta["items"] if i["value_type"] == "0")
    checks = [
        ("chart.php 1 item 1h", f"/chart.php?itemids[]={f['itemid']}&from=now-1h&to=now&width=1000&height=200&profileIdx=web.item.graph.filter"),
        ("chart.php 1 item 24h", f"/chart.php?itemids[]={f['itemid']}&from=now-24h&to=now&width=1000&height=200&profileIdx=web.item.graph.filter"),
        ("latest.view host lab", f"/zabbix.php?action=latest.view&hostids[]={meta['hostid']}&filter_set=1"),
    ]
    for label, path in checks:
        t0 = time.perf_counter()
        r = op.open(BASE + path, timeout=120)
        body = r.read()
        ms = (time.perf_counter() - t0) * 1000
        ctype = r.headers.get("Content-Type", "")
        err = b"Database error" in body or b"SQL statement execution has failed" in body
        print(f"{label:24s} HTTP {r.status} {ctype:28s} {len(body):8d} bytes {ms:8.1f} ms {'ERRO SQL' if err else 'ok'}")


if __name__ == "__main__":
    main()
