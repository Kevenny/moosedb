#!/usr/bin/env python3
"""Gera eventos sintéticos de acesso a arquivos (determinístico: mesma semente => mesmos bytes).

  python scripts/gen_data.py --rows 20000000 --out data/20m
  python scripts/gen_data.py --rows 2000000  --out data/2m

Saída em --out: events_000.tsv ... (TSV, 1 M linhas por arquivo, ordenado por ts) e
params.env (janelas e alvos usados pelas consultas + estatísticas reais do conjunto).
Colunas: ts host process user_name folder file op bytes latency_us.
Requer numpy. Ver README.md §Suposições para as distribuições.
"""
import argparse, os, sys
import numpy as np

T0 = np.datetime64("2026-10-12T00:00:00", "us")          # segunda-feira; 7 dias
DAYS = 7
BASE_PROCS = """explorer.exe chrome.exe msedge.exe outlook.exe excel.exe winword.exe powerpnt.exe teams.exe
svchost.exe onedrive.exe code.exe python.exe java.exe node.exe backup_agent.exe av_scan.exe sqlservr.exe
robocopy.exe 7z.exe acrobat.exe notepad++.exe git.exe devenv.exe slack.exe zoom.exe firefox.exe
searchindexer.exe msbuild.exe rsync smbd""".split()
OPS = ["read", "open", "write", "rename", "delete", "create"]
OP_P = [0.52, 0.20, 0.18, 0.04, 0.03, 0.03]
OP_MED_LAT = np.array([250, 120, 600, 500, 900, 700], float)   # mediana da latência (us)
EXTS = ["docx", "xlsx", "pdf", "txt", "log", "dat", "png", "csv", "py", "bak", "tmp", "dll"]


def main():
    a = argparse.ArgumentParser()
    a.add_argument("--rows", type=int, default=20_000_000)
    a.add_argument("--out", default="data/20m")
    a.add_argument("--seed", type=int, default=42)
    a.add_argument("--part-rows", type=int, default=1_000_000)
    a = a.parse_args()
    N, scale = a.rows, a.rows / 20_000_000
    n_hosts, n_procs = 2000, 300
    n_folders = max(2000, int(50_000 * scale))
    n_files = max(50_000, int(2_400_000 * scale))      # pool; ~2 M distintos observados em 20 M linhas
    n_users = 1500
    os.makedirs(a.out, exist_ok=True)
    rng = np.random.default_rng(a.seed)

    # ---- dicionários
    kind = ["ws"] * 17 + ["srv"] * 2 + ["vdi"]
    hosts = np.array([f"{kind[i % 20]}-{i:04d}" for i in range(n_hosts)])
    procs = np.array(BASE_PROCS + [f"app{i:03d}.exe" for i in range(n_procs - len(BASE_PROCS))])
    users = np.array([f"u{i:04d}" for i in range(n_users)] + [f"svc_{i}" for i in range(8)])
    host_w = rng.lognormal(0, 1.2, n_hosts) * np.where(np.arange(n_hosts) % 20 >= 17, 1, 1) * \
        np.where(np.arange(n_hosts) % 20 == 17, 5, 1)       # servidores (srv) 5x mais ativos
    host_cdf = np.cumsum(host_w / host_w.sum())
    proc_w = (np.arange(1, n_procs + 1)) ** -1.1
    # perfil de processos por host: k processos distintos (Gumbel top-k ponderado), em ordem de popularidade
    k_h = np.clip(rng.lognormal(np.log(35), 0.6, n_hosts), 8, 150).astype(int)
    keys = np.log(proc_w)[None, :] + rng.gumbel(size=(n_hosts, n_procs))
    order = np.argsort(-keys, axis=1)
    prof = np.zeros((n_hosts, 150), np.int64)
    for h in range(n_hosts):
        prof[h, :k_h[h]] = np.sort(order[h, :k_h[h]])
    # pastas: tamanho enviesado; arquivos -> pasta
    fw = np.arange(1, n_folders + 1) ** -0.6
    folder_of_file = np.minimum(np.searchsorted(np.cumsum(fw / fw.sum()), rng.random(n_files)), n_folders - 1)
    folder_str = np.array([f"/srv/dept{i % 40:02d}/team{(i // 40) % 50:02d}/proj{i:05d}/{('docs','data','src','tmp','logs')[i % 5]}"
                           for i in range(n_folders)], dtype=object)
    ext = rng.integers(0, len(EXTS), n_files)
    file_str = np.array([f"{folder_str[folder_of_file[i]]}/f{i:07d}.{EXTS[ext[i]]}" for i in range(n_files)], dtype=object)
    file_cdf = np.cumsum(np.arange(1, n_files + 1, dtype=np.float64) ** -0.9)
    file_cdf /= file_cdf[-1]                                  # Zipf s=0.9 sobre n_files
    # ---- tempo: contagem por hora (dia útil 8-18h forte; noite/fim de semana fraco), ordenado
    hw = np.empty(DAYS * 24)
    for d in range(DAYS):
        for hr in range(24):
            wk = d < 5
            hw[d * 24 + hr] = (1.0 if 8 <= hr < 18 else 0.25 if 6 <= hr < 22 else 0.05) * (1.0 if wk else 0.15)
    per_hour = rng.multinomial(N, hw / hw.sum())
    ts_us = np.concatenate([np.sort(rng.integers(0, 3_600_000_000, c)) + i * 3_600_000_000 for i, c in enumerate(per_hour)])
    ts_all = T0 + ts_us.astype("timedelta64[us]")
    del ts_us

    host_cnt = np.zeros(n_hosts, np.int64)
    file_cnt = np.zeros(n_files, np.int64)
    ops_cnt = np.zeros(len(OPS), np.int64)
    nparts = (N + a.part_rows - 1) // a.part_rows
    for part in range(nparts):
        lo, hi = part * a.part_rows, min(N, (part + 1) * a.part_rows)
        n = hi - lo
        r = np.random.default_rng([a.seed, part])
        h = np.minimum(np.searchsorted(host_cdf, r.random(n)), n_hosts - 1)
        j = np.minimum((k_h[h] ** r.random(n)).astype(int) - 1, k_h[h] - 1).clip(0)   # Zipf s=1 dentro do perfil
        p = prof[h, j]
        u = np.where(r.random(n) < 0.06, n_users + p % 8, h % n_users)
        f = np.minimum(np.searchsorted(file_cdf, r.random(n)), n_files - 1)
        o = r.choice(len(OPS), n, p=OP_P)
        lat = OP_MED_LAT[o] * r.lognormal(0, 0.9, n) * np.where(r.random(n) < 0.005, 20, 1)
        lat = np.clip(lat, 1, 5e7).astype(np.int64)
        sz = np.clip(r.lognormal(np.log(8192), 1.8, n) * np.where(o == 2, 0.5, 1), 1, 2e9).astype(np.int64)
        sz = np.where((o == 0) | (o == 2), sz, 0)
        host_cnt += np.bincount(h, minlength=n_hosts)
        file_cnt += np.bincount(f, minlength=n_files)
        ops_cnt += np.bincount(o, minlength=len(OPS))
        tss = np.char.replace(np.datetime_as_string(ts_all[lo:hi], unit="us"), "T", " ")
        cols = [tss.tolist(), hosts[h].tolist(), procs[p].tolist(), users[u].tolist(),
                folder_str[folder_of_file[f]].tolist(), file_str[f].tolist(),
                np.array(OPS)[o].tolist(), sz.astype(str).tolist(), lat.astype(str).tolist()]
        with open(os.path.join(a.out, f"events_{part:03d}.tsv"), "w", newline="\n") as fh:
            fh.write("\n".join("\t".join(t) for t in zip(*cols)) + "\n")
        print(f"part {part + 1}/{nparts}", file=sys.stderr, flush=True)

    # ---- alvos das consultas (determinísticos a partir dos dados)
    h0 = np.argsort(-host_cnt, kind="stable")[min(99, n_hosts - 1)]        # 100º host mais ativo
    f0 = np.argsort(-file_cnt, kind="stable")[min(199, n_files - 1)]       # 200º arquivo mais acessado
    with open(os.path.join(a.out, "params.env"), "w") as fh:
        fh.write(f"ROWS={N}\nDAY0='2026-10-15 00:00:00'\nDAY1='2026-10-16 00:00:00'\n"
                 f"HOUR0='2026-10-15 10:00:00'\nHOUR1='2026-10-15 11:00:00'\n"
                 f"HOST0='{hosts[h0]}'\nFILE0='{file_str[f0]}'\n"
                 f"# estatisticas: hosts={n_hosts} procs={n_procs} pastas_pool={n_folders} arquivos_pool={n_files} "
                 f"arquivos_distintos={int((file_cnt > 0).sum())} eventos_host0={host_cnt[h0]} eventos_file0={file_cnt[f0]} "
                 f"top_file={file_cnt.max()}\n# ops={dict(zip(OPS, ops_cnt.tolist()))}\n")
    print(open(os.path.join(a.out, "params.env")).read())


if __name__ == "__main__":
    main()
