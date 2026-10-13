#!/usr/bin/env python3
"""Verificação funcional do frontend/API sobre as tabelas de histórico.

Exercita os caminhos de leitura do frontend que tocam history*:
  * history.get (CHistory.php) com sortfield=clock DESC + limit (chunking por tempo)
  * history.get por intervalo (time_from/time_till)
  * item.get com lastvalue (CHistoryManager::getLastValues)
  * history.clear (CHistoryManager::deleteHistoryFromSql) — espera-se ERRO em MooseDB

    docker compose --profile tools run --rm tools python scripts/api_check.py [--clear]
"""
import argparse
import json
import time

from create_items import Api, API_URL, API_USER, API_PASSWORD


def timed(api, method, params):
    t0 = time.perf_counter()
    try:
        r = api.call(method, params)
        return r, (time.perf_counter() - t0) * 1000, None
    except RuntimeError as e:
        return None, (time.perf_counter() - t0) * 1000, str(e)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--items", default="results/items.json")
    ap.add_argument("--clear", action="store_true", help="testa history.clear num item (apaga histórico dele!)")
    args = ap.parse_args()
    meta = json.load(open(args.items))
    items = meta["items"]
    f = next(i for i in items if i["value_type"] == "0")
    u = next(i for i in items if i["value_type"] == "3")
    s = next(i for i in items if i["value_type"] == "1")

    api = Api(API_URL)
    api.login(API_USER, API_PASSWORD)
    now = int(time.time())
    checks = [
        ("history.get float últimos 10", "history.get",
         {"history": 0, "itemids": [f["itemid"]], "sortfield": "clock", "sortorder": "DESC", "limit": 10}),
        ("history.get uint 1h", "history.get",
         {"history": 3, "itemids": [u["itemid"]], "time_from": now - 3600, "output": "extend"}),
        ("history.get str últimos 5", "history.get",
         {"history": 1, "itemids": [s["itemid"]], "sortfield": "clock", "sortorder": "DESC", "limit": 5}),
        ("item.get lastvalue (200 itens)", "item.get",
         {"itemids": [i["itemid"] for i in items[:200]], "output": ["itemid", "lastvalue", "lastclock"]}),
    ]
    for label, method, params in checks:
        r, ms, err = timed(api, method, params)
        n = len(r) if isinstance(r, list) else r
        extra = ""
        if isinstance(r, list) and r and "lastclock" in r[0]:
            extra = f" com_valor={sum(1 for x in r if x['lastclock'] != '0')}"
        print(f"{label:34s} {'ERRO ' + err if err else 'ok'} linhas={n if not err else '-'} {ms:.1f} ms{extra}")
    if args.clear:
        r, ms, err = timed(api, "history.clear", [f["itemid"]])
        print(f"{'history.clear (1 item float)':34s} {'ERRO ' + err if err else 'ok ' + json.dumps(r)} {ms:.1f} ms")


if __name__ == "__main__":
    main()
