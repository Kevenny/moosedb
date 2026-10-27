#!/usr/bin/env python3
"""MooseDB power-loss harness. Runs INSIDE the moosedb-powerloss container
(see run.sh / README.md); needs /dev/fuse + CAP_SYS_ADMIN.

Per cycle: mount LazyFS over the datadir -> start mariadbd -> verify the previous
crash -> random mixed load from N clients (acks logged OUTSIDE the datadir) ->
power cut at a chosen phase (SIGSTOP mysqld, lazyfs::clear-cache, SIGKILL,
clear-cache again; or LazyFS crashing itself at a syscall) -> unmount.

Oracle (per table, per row id):
  * every acknowledged INSERT is present, whole, exactly once, with its content;
  * every acknowledged DELETE stays applied (no row comes back);
  * an in-flight statement (sent, no ack) is all-or-nothing;
  * no row that was never sent appears.
"""
import argparse, collections, datetime, itertools, json, os, random, re, select
import shutil, signal, subprocess, sys, threading, time
import pymysql

LAZYFS = '/opt/lazyfs/lazyfs/build/lazyfs'
PL = '/pl'
ROOT = PL + '/root'        # LazyFS backing dir = what "the disk" holds
DATA = PL + '/data'        # LazyFS mount point = mariadbd datadir
LOGS = PL + '/logs'        # error log, LazyFS log (normal fs, survives the cut)
SNAP = PL + '/snap_cur'    # copy of ROOT right after the last cut
SOCK = PL + '/mysqld.sock'
CNF = PL + '/my.cnf'
TOML = PL + '/lazyfs.toml'
FIFO = PL + '/faults.fifo'
DONE = PL + '/faults-done.fifo'
ERRLOG = LOGS + '/mysqld.err'
DB = 'moose'
EPOCH = datetime.datetime(2026, 1, 1)
EPOCH_CLOCK = 1767225600   # 2026-01-01 00:00:00 UTC
SLOT = 100_000             # id slot: cycle*1e6 + client*SLOT + n; slot % 10 == client
PROBE_CLIENT = 9

TABLES = {
    't_plain': "CREATE TABLE t_plain (ts DATETIME NOT NULL, host VARCHAR(8) NOT NULL COMMENT 'TAG',"
               " id BIGINT NOT NULL, stmt BIGINT NOT NULL, v DOUBLE NOT NULL, INDEX(ts))"
               " ENGINE=MooseDB CHUNK_INTERVAL='1 HOUR' MEMTABLE_SIZE=1048576",
    't_enc':   "CREATE TABLE t_enc (ts DATETIME NOT NULL, host VARCHAR(8) NOT NULL COMMENT 'TAG',"
               " id BIGINT NOT NULL, stmt BIGINT NOT NULL, v DOUBLE NOT NULL, INDEX(ts))"
               " ENGINE=MooseDB CHUNK_INTERVAL='1 HOUR' MEMTABLE_SIZE=1048576"
               " ENCRYPTION='YES' ENCRYPTION_KEY_ID=2",
    't_pk':    "CREATE TABLE t_pk (itemid BIGINT UNSIGNED NOT NULL COMMENT 'TAG', clock INT NOT NULL,"
               " id BIGINT NOT NULL, stmt BIGINT NOT NULL, v DOUBLE NOT NULL, PRIMARY KEY (itemid, clock))"
               " ENGINE=MooseDB TIMESTAMP_COLUMN='clock' CHUNK_INTERVAL='1 HOUR' MEMTABLE_SIZE=1048576",
}
TNAMES = list(TABLES)

# Phases a cut can be aimed at. 'stmt' phases: cut a random delay after a client
# sends a statement of that kind (max delay in s). 'sys' phases: LazyFS crashes
# itself at the syscall (drops all unsynced data), then mysqld gets kill -9.
STMT_PHASES = {'insert': 0.004, 'insert_multi': 0.012, 'insert_big': 0.3,
               'delete_direct': 0.012, 'delete_rows': 0.06, 'flush': 0.06,
               'optimize': 0.4, 'compact': 0.4, 'random': None}
SYS_PHASES = {  # name: (op, regex key, regex, statement that provokes it)
    'manifest_rename': ('rename', 'from_rgx', 'MANIFEST', 'FLUSH TABLES {t}'),
    'chunk_fsync':     ('fsync', 'from_rgx', r'chunk_.*\.tmp', 'FLUSH TABLES {t}'),
    'chunk_unlink':    ('unlink', 'from_rgx', r'chunk_.*\.tfl$', 'OPTIMIZE TABLE {t}'),
    'wal_fsync':       ('fsync', 'from_rgx', r'wal_.*\.wal$', None),
}
PHASES = list(STMT_PHASES) + list(SYS_PHASES)

CNF_TEXT = """[mariadbd]
datadir={data}
socket={sock}
skip-networking
user=root
log_error={err}
plugin_maturity=experimental
plugin_load_add=ha_moosedb
plugin_load_add=file_key_management
file_key_management_filename=/repo/mysql-test/suite/moosedb/std_data/keys.txt
moosedb_wal_sync_mode={sync}
innodb_flush_method=fsync
innodb_use_native_aio=0
innodb_buffer_pool_size=64M
innodb_log_file_size=16M
innodb_undo_tablespaces=0
innodb_buffer_pool_dump_at_shutdown=0
innodb_buffer_pool_load_at_startup=0
default_time_zone='+00:00'
log_warnings=2
max_connections=50
"""
TOML_TEXT = """[faults]
fifo_path="{fifo}"
fifo_path_completed="{done}"
[cache]
apply_eviction=false
[cache.simple]
custom_size="512mb"
blocks_per_page=1
[filesystem]
log_all_operations=false
logfile=""
"""


def log(msg):
    print(time.strftime('%H:%M:%S'), msg, flush=True)


def sh(*cmd, check=True, **kw):
    return subprocess.run(cmd, check=check, **kw)


# ----------------------------------------------------------------- rows -----
def tag(tbl, i):
    return i % 7 if tbl == 't_pk' else 'h%d' % (i % 5)


def row_sql(tbl, i, stmt):
    if tbl == 't_pk':
        return '(%d,%d,%d,%d,%r)' % (i % 7, EPOCH_CLOCK + i, i, stmt, i * 0.5)
    ts = (EPOCH + datetime.timedelta(seconds=i)).strftime('%Y-%m-%d %H:%M:%S')
    return "('%s','h%d',%d,%d,%r)" % (ts, i % 5, i, stmt, i * 0.5)


def time_lit(tbl, i):
    if tbl == 't_pk':
        return str(EPOCH_CLOCK + i)
    return "'%s'" % (EPOCH + datetime.timedelta(seconds=i)).strftime('%Y-%m-%d %H:%M:%S')


def dt_lit(i):
    return "'%s'" % (EPOCH + datetime.timedelta(seconds=i)).strftime('%Y-%m-%d %H:%M:%S')


DUMP = {
    't_plain': "SELECT id, stmt, host, TIMESTAMPDIFF(SECOND,'2026-01-01 00:00:00',ts), v FROM t_plain",
    't_enc': "SELECT id, stmt, host, TIMESTAMPDIFF(SECOND,'2026-01-01 00:00:00',ts), v FROM t_enc",
    't_pk': "SELECT id, stmt, itemid, clock - %d, v FROM t_pk" % EPOCH_CLOCK,
}


# --------------------------------------------------------------- LazyFS -----
class Lazy:
    def __init__(self):
        self.proc = None

    def mount(self):
        os.makedirs(DATA, exist_ok=True)
        self.logf = open(LOGS + '/lazyfs.log', 'ab')
        self.proc = subprocess.Popen(
            [LAZYFS, DATA, '--config-path', TOML, '-o', 'allow_other', '-o', 'modules=subdir',
             '-o', 'subdir=' + ROOT, '-f'], stdout=self.logf, stderr=subprocess.STDOUT)
        for _ in range(300):
            if os.path.ismount(DATA) and os.path.exists(FIFO) and os.path.exists(DONE):
                # LazyFS opens the "done" FIFO for writing (blocking) before it
                # reads any command: keep a reader open for the whole mount.
                self.done_fd = os.open(DONE, os.O_RDONLY | os.O_NONBLOCK)
                return
            if self.proc.poll() is not None:
                raise RuntimeError('lazyfs exited at mount')
            time.sleep(0.05)
        raise RuntimeError('lazyfs mount timeout')

    def alive(self):
        return self.proc is not None and self.proc.poll() is None

    def drain(self):
        buf = b''
        while select.select([self.done_fd], [], [], 0)[0]:
            b = os.read(self.done_fd, 4096)
            if not b:
                break
            buf += b
        return buf

    def cmd(self, text, wait_done=False, timeout=30):
        self.drain()
        for _ in range(500):  # ENXIO until LazyFS has the command FIFO open
            try:
                fd = os.open(FIFO, os.O_WRONLY | os.O_NONBLOCK)
                break
            except OSError:
                if not self.alive():
                    return False
                time.sleep(0.02)
        else:
            return False
        os.write(fd, text.encode() + b'\n')
        os.close(fd)
        if not wait_done:
            return True
        buf, end = b'', time.time() + timeout
        while time.time() < end and b'finished' not in buf and self.alive():
            if select.select([self.done_fd], [], [], 0.2)[0]:
                b = os.read(self.done_fd, 4096)
                buf += b
                if not b:
                    time.sleep(0.01)  # no writer right now (EOF)
        return b'finished' in buf

    def umount(self):
        if self.alive():
            self.proc.kill()
            self.proc.wait()
        sh('fusermount3', '-uz', DATA, check=False, stderr=subprocess.DEVNULL)
        os.close(self.done_fd)
        self.proc = None
        self.logf.close()


# -------------------------------------------------------------- mariadbd ----
class Server:
    def __init__(self):
        self.proc = None

    def start(self, timeout=180):
        self.err_off = os.path.getsize(ERRLOG) if os.path.exists(ERRLOG) else 0
        self.proc = subprocess.Popen(['mariadbd', '--defaults-file=' + CNF],
                                     stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        end = time.time() + timeout
        while time.time() < end:
            if self.proc.poll() is not None:
                return False
            try:
                connect(db=None).close()
                return True
            except pymysql.err.OperationalError:
                time.sleep(0.2)
        return False

    def errlog(self):
        try:
            with open(ERRLOG, 'rb') as f:
                f.seek(self.err_off)
                return f.read().decode('utf-8', 'replace')
        except OSError:
            return ''

    def kill9(self):
        if self.proc and self.proc.poll() is None:
            self.proc.kill()
        if self.proc:
            self.proc.wait()

    def shutdown(self):
        try:
            c = connect()
            c.cursor().execute('SHUTDOWN')
            c.close()
        except pymysql.err.Error:
            pass
        try:
            self.proc.wait(120)
        except subprocess.TimeoutExpired:
            self.kill9()


def connect(db=DB, timeout=120):
    return pymysql.connect(unix_socket=SOCK, user='root', database=db, autocommit=True,
                           read_timeout=timeout, write_timeout=timeout, connect_timeout=10)


# ------------------------------------------------------------------ setup ---
def setup(sync_mode):
    if os.path.islink(DATA):
        os.unlink(DATA)
    for d in (ROOT, DATA, SNAP):
        if os.path.ismount(d):
            sh('fusermount3', '-uz', d, check=False)
        shutil.rmtree(d, ignore_errors=True)
    os.makedirs(ROOT)
    os.makedirs(LOGS, exist_ok=True)
    with open(CNF, 'w') as f:
        f.write(CNF_TEXT.format(data=DATA, sock=SOCK, err=ERRLOG, sync=sync_mode))
    with open(TOML, 'w') as f:
        f.write(TOML_TEXT.format(fifo=FIFO, done=DONE))
    plugdir = subprocess.run(['mariadb_config', '--plugindir'], capture_output=True, text=True).stdout.strip() \
        if shutil.which('mariadb_config') else ''
    if not plugdir:
        plugdir = os.path.dirname(subprocess.run(
            "find /usr/lib -name ha_archive.so -path '*plugin*' | head -1", shell=True,
            capture_output=True, text=True).stdout.strip())
    shutil.copy('/repo/build/ha_moosedb.so', plugdir)  # copied at start, never bind-mounted
    # The base datadir is created directly on the backing dir (real fs), so it is
    # fully durable; LazyFS only sees what happens from cycle 1 on.
    os.makedirs(DATA, exist_ok=True)
    os.rmdir(DATA)
    os.symlink(ROOT, DATA)
    sh('mariadb-install-db', '--defaults-file=' + CNF, '--skip-test-db',
       '--auth-root-authentication-method=normal', stdout=subprocess.DEVNULL)
    srv = Server()
    if not srv.start():
        raise RuntimeError('setup: mariadbd did not start:\n' + srv.errlog()[-3000:])
    c = connect(db=None)
    cur = c.cursor()
    cur.execute('CREATE DATABASE ' + DB)
    cur.execute('USE ' + DB)
    for ddl in TABLES.values():
        cur.execute(ddl)
    c.close()
    with open('/repo/storage/moosedb/sql/moosedb_install.sql', 'rb') as f:
        sh('mariadb', '-S', SOCK, '-uroot', DB, stdin=f)
    srv.shutdown()
    os.unlink(DATA)
    os.makedirs(DATA)


# ----------------------------------------------------------------- state ----
class Cycle:
    """Ops of one cycle, as the clients saw them (also mirrored to ops.jsonl)."""

    def __init__(self, k, opslog):
        self.k = k
        self.ins = collections.defaultdict(list)   # tbl -> [stmt, lo, n, status]
        self.dels = collections.defaultdict(list)  # tbl -> [stmt, ids, status]
        self.status = {}                           # stmt -> 'sent'|'ack'|'err'
        self.errors = collections.Counter()        # unexpected errors before the cut
        self.stats = collections.Counter()
        self.durs = collections.defaultdict(list)  # kind -> durations of acked statements
        self.lock = threading.Lock()
        self.opslog = opslog
        self.seq = itertools.count(1)

    def new_stmt(self):
        return self.k * 1_000_000 + next(self.seq)

    def write(self, rec):
        rec['c'] = self.k
        with self.lock:
            self.opslog.write(json.dumps(rec) + '\n')
            self.opslog.flush()


class Crash:
    def __init__(self, phase):
        self.phase = phase
        self.armed = False
        self.go = threading.Event()
        self.target = None
        self.cut = False
        self.lock = threading.Lock()

    def before_send(self, kind, stmt):
        if self.armed and not self.go.is_set() and kind == self.phase:
            with self.lock:
                if not self.go.is_set():
                    self.target = (kind, stmt)
                    self.go.set()


# ------------------------------------------------------------------ load ----
W = [('insert', 30), ('insert_multi', 25), ('insert_big', 1), ('delete_direct', 8),
     ('delete_rows', 8), ('flush', 8), ('optimize', 2), ('compact', 5), ('check', 2), ('select', 7)]
KINDS, WEIGHTS = zip(*W)


def client(ci, cyc, alive, rng, stop, crash, lazy):
    try:
        conn = connect()
    except pymysql.err.Error:
        return
    cur = conn.cursor()
    nxt = cyc.k * 1_000_000 + ci * SLOT
    while not stop.is_set():
        kind = rng.choices(KINDS, WEIGHTS)[0]
        tbl = rng.choice(TNAMES)
        stmt = cyc.new_stmt()
        rec, ids = None, None
        if kind.startswith('insert'):
            n = 1 if kind == 'insert' else rng.randint(2, 60) if kind == 'insert_multi' else rng.randint(3000, 8000)
            n = min(n, (nxt // SLOT + 1) * SLOT - nxt)  # never spill into another client's slot
            if n < 1:
                continue
            lo, nxt = nxt, nxt + n
            sql = 'INSERT INTO %s VALUES %s' % (tbl, ','.join(row_sql(tbl, i, stmt) for i in range(lo, lo + n)))
            rec = [stmt, lo, n, 'sent']
            cyc.ins[tbl].append(rec)
            cyc.write({'k': 'ins', 's': stmt, 't': tbl, 'lo': lo, 'n': n})
        elif kind.startswith('delete'):
            own = alive[(tbl, ci)]
            if not own:
                continue
            x = rng.choice(tuple(own))
            hi = min(x + rng.randint(0, 80), (x // SLOT + 1) * SLOT - 1)
            tags = None if rng.random() < 0.4 else rng.sample(range(7 if tbl == 't_pk' else 5), rng.randint(1, 3))
            tcol = 'clock' if tbl == 't_pk' else 'ts'
            where = '%s BETWEEN %s AND %s' % (tcol, time_lit(tbl, x), time_lit(tbl, hi))
            if tags is not None:
                tcol2 = 'itemid' if tbl == 't_pk' else 'host'
                where += ' AND %s IN (%s)' % (tcol2, ','.join(str(t) if tbl == 't_pk' else "'h%d'" % t for t in tags))
            mod = (1, 0)
            if kind == 'delete_rows':  # a non-TAG column forces the row-by-row path
                mod = (rng.randint(1, 3), 0)
                mod = (mod[0], rng.randrange(mod[0]))
                where += ' AND MOD(id, %d) = %d AND v >= 0' % mod
            ids = frozenset(i for i in own if x <= i <= hi and (tags is None or tag(tbl, i) in
                            [t if tbl == 't_pk' else 'h%d' % t for t in tags]) and i % mod[0] == mod[1])
            sql = 'DELETE FROM %s WHERE %s' % (tbl, where)
            rec = [stmt, ids, 'sent']
            cyc.dels[tbl].append(rec)
            cyc.write({'k': 'del', 's': stmt, 't': tbl, 'ids': sorted(ids), 'sql': sql})
        elif kind == 'flush':
            sql = 'FLUSH TABLES ' + tbl
        elif kind == 'optimize':
            sql = 'OPTIMIZE TABLE ' + tbl
        elif kind == 'compact':
            if rng.random() < 0.5:
                sql = "CALL moosedb_compact('%s', NULL, NULL)" % tbl
            else:
                h = (nxt - rng.randint(0, 20000)) // 3600 * 3600
                sql = "CALL moosedb_compact('%s', %s, %s)" % (tbl, dt_lit(h), dt_lit(h + 7200))
        elif kind == 'check':
            sql = 'CHECK TABLE ' + tbl
        else:
            sql = 'SELECT COUNT(*), MAX(id) FROM ' + tbl
        if kind in ('flush', 'optimize', 'compact', 'check', 'select'):
            cyc.write({'k': kind, 's': stmt, 't': tbl, 'sql': sql})
        crash.before_send(kind, stmt)
        t0 = time.monotonic()
        try:
            cur.execute(sql)
            cur.fetchall()
            while cur.nextset():
                pass
        except pymysql.err.Error as e:
            if stop.is_set() or crash.cut or not lazy.alive():
                break  # the cut (or LazyFS died at its syscall): stays "in flight"
            code = e.args[0] if e.args else 0
            msg = re.sub(r'\d+', 'N', str(e.args[1] if len(e.args) > 1 else e))[:120]
            cyc.errors['%s %s: %s %s' % (kind, tbl, code, msg)] += 1
            if rec is not None:
                rec[-1] = 'err'
                cyc.status[stmt] = 'err'
            cyc.write({'e': stmt, 'err': str(e)[:300]})
            if code in (2006, 2013):
                break
            continue
        cyc.write({'a': stmt})
        cyc.durs[kind].append(time.monotonic() - t0)
        cyc.stats[kind] += 1
        if rec is not None:
            rec[-1] = 'ack'
            cyc.status[stmt] = 'ack'
            if kind.startswith('insert'):
                alive[(tbl, ci)].update(range(rec[1], rec[1] + rec[2]))
            else:
                alive[(tbl, ci)].difference_update(ids)
    try:
        conn.close()
    except Exception:
        pass


def probe(cyc, alive):
    """One acked single-row INSERT per table right after recovery: a table left
    read-only (or that cannot be written) shows up here."""
    bad = []
    try:
        conn = connect()
    except pymysql.err.Error as e:
        return ['probe: connect: %s' % e]
    cur = conn.cursor()
    base = cyc.k * 1_000_000 + PROBE_CLIENT * SLOT
    for j, tbl in enumerate(TNAMES):
        stmt = cyc.new_stmt()
        rec = [stmt, base + j, 1, 'sent']
        cyc.ins[tbl].append(rec)
        cyc.write({'k': 'ins', 's': stmt, 't': tbl, 'lo': base + j, 'n': 1, 'probe': True})
        try:
            cur.execute('INSERT INTO %s VALUES %s' % (tbl, row_sql(tbl, base + j, stmt)))
            rec[-1] = 'ack'
            cyc.write({'a': stmt})
        except pymysql.err.Error as e:
            rec[-1] = 'err'
            bad.append('escrita recusada após recuperação em %s: %s' % (tbl, e))
    conn.close()
    return bad


def run_cycle(cyc, srv, lazy, alive, rng, phase, nclients):
    stop = threading.Event()
    crash = Crash(phase)
    threads = [threading.Thread(target=client, args=(ci, cyc, alive, random.Random(rng.random()), stop, crash, lazy))
               for ci in range(nclients)]
    for t in threads:
        t.start()
    time.sleep(rng.uniform(0.8, 4.0))
    info = {'phase': phase}
    lazy_crashed = False
    if phase in SYS_PHASES:
        op, key, rgx, provoke = SYS_PHASES[phase]
        timing = rng.choice(['before', 'after'])
        info['fault'] = 'lazyfs::crash::timing=%s::op=%s::%s=%s' % (timing, op, key, rgx)
        info['armed'] = lazy.cmd(info['fault'])
        if provoke:
            tbl = rng.choice(TNAMES)
            info['provoke'] = provoke.format(t=tbl)

            def prov():
                try:
                    c = connect(timeout=30)
                    c.cursor().execute(info['provoke'])
                    c.close()
                except Exception:
                    pass
            threading.Thread(target=prov, daemon=True).start()
        end = time.time() + 15
        while time.time() < end and lazy.alive():
            time.sleep(0.01)
        lazy_crashed = not lazy.alive()
        info['hit'] = lazy_crashed
    elif phase == 'random':
        info['hit'] = True
    else:
        crash.armed = True
        info['hit'] = crash.go.wait(20)
        if info['hit']:
            info['target'] = crash.target
            # aim inside the statement: below the median duration seen so far
            ds = sorted(cyc.durs[phase])
            hi = min(STMT_PHASES[phase], 0.8 * ds[len(ds) // 2]) if ds else STMT_PHASES[phase]
            info['delay'] = rng.uniform(0, hi)
            time.sleep(info['delay'])
    # ---- the power cut
    crash.cut = True
    if lazy_crashed:
        srv.kill9()  # LazyFS died at the syscall: its cache (all unsynced data) is gone
    else:
        os.kill(srv.proc.pid, signal.SIGSTOP)
        ok1 = lazy.cmd('lazyfs::clear-cache', wait_done=True)
        srv.kill9()
        ok2 = lazy.cmd('lazyfs::clear-cache', wait_done=True)
        info['clear_cache_ok'] = ok1 and ok2
    stop.set()
    for t in threads:
        t.join(60)
    lazy.umount()
    if 'target' in info:
        st = cyc.status.get(info['target'][1], 'sent')
        info['target_inflight'] = st != 'ack'
    info['inflight'] = sum(1 for t in TNAMES for r in cyc.ins[t] + cyc.dels[t] if r[-1] == 'sent')
    info['stats'] = dict(cyc.stats)
    info['errors_before_cut'] = dict(cyc.errors)
    return info


# ---------------------------------------------------------------- verify ----
LOG_CLASSES = [
    ('autenticação/cripto', re.compile(r'crypt|decrypt|authent|\btag\b|GCM', re.I)),
    ('corrupção/quarentena', re.compile(r'corrupt|quarant|checksum|CRC', re.I)),
    ('read-only', re.compile(r'read.only', re.I)),
    ('WAL/replay', re.compile(r'\bWAL\b|replay|truncat', re.I)),
    ('fsync/IO', re.compile(r'fsync|I/O|errno|Input/output', re.I)),
]


def classify_log(text):
    out = collections.Counter()
    lines = []
    for ln in text.splitlines():
        moose = re.search(r'moose', ln, re.I)
        if not (moose or '[ERROR]' in ln):
            continue
        sev = 'ERROR' if '[ERROR]' in ln else 'Warning' if '[Warning]' in ln else 'Note'
        cls = next((c for c, rx in LOG_CLASSES if rx.search(ln)), 'outro')
        out['%s %s %s' % ('MooseDB' if moose else 'servidor', sev, cls)] += 1
        lines.append(ln[:300])
    return dict(out), lines


def scan_files():
    tmp, corrupt = set(), set()
    for dp, _, fs in os.walk(os.path.join(DATA, DB)):
        for f in fs:
            p = os.path.relpath(os.path.join(dp, f), DATA)
            if f.endswith('.tmp'):
                tmp.add(p)
            if f.endswith('.corrupt'):
                corrupt.add(p)
    return tmp, corrupt


def verify(cyc, truth, id2stmt, srv):
    """Returns (violations, details, new_truth or None when a table could not be read)."""
    viol, det = [], {'tables': {}}
    new_truth, fatal = {}, False
    try:
        conn = connect()
    except pymysql.err.Error as e:
        return ['conexão após restart: %s' % e], det, None
    cur = conn.cursor()
    for tbl in TNAMES:
        d = det['tables'][tbl] = {}
        try:
            cur.execute(DUMP[tbl])
            rows = cur.fetchall()
            cur.execute('SELECT COUNT(*) FROM ' + tbl)
            cnt = cur.fetchone()[0]
            cur.execute('CHECK TABLE ' + tbl)
            chk = cur.fetchall()
        except pymysql.err.Error as e:
            sev = 'GRAVE (tabela cifrada não abre após queda legítima)' if tbl == 't_enc' else 'tabela não abre'
            viol.append('%s: %s: %s' % (tbl, sev, e))
            fatal = True
            continue
        if not chk or chk[-1][3] != 'OK':
            viol.append('%s: CHECK TABLE = %s' % (tbl, [tuple(r) for r in chk]))
        if cnt != len(rows):
            viol.append('%s: COUNT(*)=%d mas SELECT devolveu %d linhas' % (tbl, cnt, len(rows)))
        present = collections.Counter(r[0] for r in rows)
        pset = set(present)
        dup = sorted(i for i, c in present.items() if c > 1)
        mism = []
        for r in rows:
            i = r[0]
            exp = (i, id2stmt.get(i), tag(tbl, i), i, i * 0.5)
            if tuple(r) != exp:
                mism.append((tuple(r), exp))
        expected = set(truth[tbl])
        must_absent, groups = set(), []
        for stmt, lo, n, st in cyc.ins[tbl]:
            ids = set(range(lo, lo + n))
            if st == 'ack':
                expected |= ids
            else:
                groups.append(('INSERT', stmt, ids, st))
        for stmt, ids, st in cyc.dels[tbl]:
            expected -= ids
            if st == 'ack':
                must_absent |= ids
            else:
                groups.append(('DELETE', stmt, set(ids), st))
        uncertain = set().union(*[g[2] for g in groups]) if groups else set()
        lost = sorted(expected - pset - uncertain)
        resurrected = sorted(must_absent & pset)
        unexpected = pset - expected - uncertain - must_absent
        never_sent = sorted(i for i in unexpected if i not in id2stmt)
        came_back = sorted(i for i in unexpected if i in id2stmt)
        partial, applied, not_applied = [], 0, 0
        for kind, stmt, ids, st in groups:
            k = len(ids & pset)
            done = k == 0 if kind == 'DELETE' else k == len(ids)
            none = k == len(ids) if kind == 'DELETE' else k == 0
            if len(ids) == 0:
                continue
            if st == 'err':  # statement that returned an error before the cut
                errk = 'err_%s_%s' % (kind, 'applied' if done else 'none' if none else 'partial')
                d[errk] = d.get(errk, 0) + 1
                continue
            if done:
                applied += 1
            elif none:
                not_applied += 1
            elif st != 'err':  # a statement that FAILED may legitimately stop midway
                partial.append('%s stmt=%d: %d de %d linhas presentes' % (kind, stmt, k, len(ids)))
        d.update(rows=len(rows), inflight_applied=applied, inflight_not_applied=not_applied)
        for name, lst in (('linhas confirmadas perdidas', lost), ('linhas apagadas ressuscitadas', resurrected),
                          ('linhas antigas reaparecidas', came_back), ('linhas nunca enviadas', never_sent),
                          ('linhas duplicadas', dup)):
            if lst:
                viol.append('%s: %d %s, ex.: %s' % (tbl, len(lst), name, lst[:10]))
        if partial:
            viol.append('%s: statement em voo parcial: %s' % (tbl, partial[:5]))
        if mism:
            viol.append('%s: %d linhas com conteúdo errado, ex.: %s' % (tbl, len(mism), mism[:3]))
        new_truth[tbl] = pset
    try:
        cur.execute("SELECT TABLE_NAME, CHUNKS, COMPACTING_CHUNKS, IS_OPEN FROM information_schema.MOOSEDB_TABLES"
                    " WHERE TABLE_SCHEMA=%s", (DB,))
        det['is'] = [list(r) for r in cur.fetchall()]
    except pymysql.err.Error as e:
        det['is'] = str(e)
    conn.close()
    tmp, corrupt = scan_files()
    if tmp:  # background compaction may be mid-write: only flag what stays
        time.sleep(3)
        tmp &= scan_files()[0]
    if tmp:
        viol.append('.tmp sobrando após abertura: %s' % sorted(tmp)[:10])
    if corrupt:
        viol.append('chunk em quarentena: %s' % sorted(corrupt)[:10])
    classes, lines = classify_log(srv.errlog())
    det['log_classes'] = classes
    det['log_lines'] = lines[:40]
    for ln in lines:
        if '[ERROR]' in ln and re.search(r'moose', ln, re.I):
            viol.append('[ERROR] MooseDB no log: %s' % ln[:200])
    return viol, det, (None if fatal else new_truth)


# ------------------------------------------------------------------ main ----
def save_failure(out, k, cyc_info, viol, det, opsfile):
    d = os.path.join(out, 'fail', 'cycle%03d' % k)
    shutil.rmtree(d, ignore_errors=True)
    os.makedirs(d)
    if os.path.isdir(SNAP):
        shutil.copytree(SNAP, os.path.join(d, 'datadir_before_recovery'), symlinks=True)
    if os.path.isdir(os.path.join(ROOT, DB)):
        shutil.copytree(os.path.join(ROOT, DB), os.path.join(d, 'moose_after_recovery'), symlinks=True)
    shutil.copy(ERRLOG, d)
    shutil.copy(opsfile, d)
    with open(os.path.join(d, 'failure.json'), 'w') as f:
        json.dump({'cycle': k, 'crash': cyc_info, 'violations': viol, 'details': det}, f, indent=1, default=str)
    log('  snapshot + logs em %s' % d)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('-n', '--cycles', type=int, default=30)
    ap.add_argument('-s', '--seed', type=int, default=1)
    ap.add_argument('-c', '--clients', type=int, default=4)
    ap.add_argument('-o', '--out', default='/out')
    ap.add_argument('--sync-mode', default='fsync', choices=['fsync', 'write'])
    ap.add_argument('--phases', default=','.join(PHASES), help='comma list, round-robin')
    a = ap.parse_args()
    assert 1 <= a.clients <= 9
    out = os.path.join(a.out, 'seed%d' % a.seed)
    shutil.rmtree(out, ignore_errors=True)
    os.makedirs(out)
    rng = random.Random(a.seed)
    phases = a.phases.split(',')
    rng.shuffle(phases)
    log('setup (seed=%d, cycles=%d, clients=%d, sync=%s)' % (a.seed, a.cycles, a.clients, a.sync_mode))
    setup(a.sync_mode)
    opsfile = os.path.join(out, 'ops.jsonl')
    opslog = open(opsfile, 'a')
    results = open(os.path.join(out, 'results.jsonl'), 'a')
    truth = {t: set() for t in TNAMES}
    id2stmt = {}
    lazy, srv = Lazy(), Server()
    prev, prev_info = None, None
    for k in range(1, a.cycles + 2):
        lazy.mount()
        up = srv.start()
        res = {'seed': a.seed, 'cycle': k - 1}
        if prev is not None:
            for t in TNAMES:
                for stmt, lo, n, _ in prev.ins[t]:
                    for i in range(lo, lo + n):
                        id2stmt[i] = stmt
            if not up:
                viol, det, nt = ['mariadbd não sobe após a queda'], {'log': srv.errlog()[-4000:]}, None
            else:
                viol, det, nt = verify(prev, truth, id2stmt, srv)
            res.update(crash=prev_info, violations=viol, details=det)
            results.write(json.dumps(res, default=str) + '\n')
            results.flush()
            log('cycle %d phase=%s hit=%s inflight=%s -> %s' % (
                k - 1, prev_info['phase'], prev_info.get('hit'), prev_info.get('inflight'),
                'OK' if not viol else 'FALHA %d' % len(viol)))
            for v in viol:
                log('  ! ' + v)
            if viol:
                save_failure(out, k - 1, prev_info, viol, det, opsfile)
            if nt is None:  # unusable datadir: start a fresh one, keep going
                srv.kill9()
                lazy.umount()
                log('  datadir inutilizável: recriando')
                setup(a.sync_mode)
                truth = {t: set() for t in TNAMES}
                if k == a.cycles + 1:
                    break
                lazy.mount()
                up = srv.start()
            else:
                truth = nt
        if k == a.cycles + 1:
            srv.shutdown()
            lazy.umount()
            break
        if not up:
            raise RuntimeError('mariadbd did not start:\n' + srv.errlog()[-3000:])
        cyc = Cycle(k, opslog)
        alive = collections.defaultdict(set)
        for t in TNAMES:
            for i in truth[t]:
                alive[(t, (i // SLOT) % 10)].add(i)
        bad = probe(cyc, alive)
        phase = phases[(k - 1) % len(phases)]
        info = run_cycle(cyc, srv, lazy, alive, random.Random(rng.random()), phase, a.clients)
        info['probe_errors'] = bad
        shutil.rmtree(SNAP, ignore_errors=True)
        shutil.copytree(ROOT, SNAP, symlinks=True)
        prev, prev_info = cyc, info
    results.close()
    summarize(out)


def summarize(out):
    rs = [json.loads(l) for l in open(os.path.join(out, 'results.jsonl'))]
    by_phase = collections.defaultdict(lambda: [0, 0, 0])
    fails = collections.Counter()
    for r in rs:
        p = by_phase[r['crash']['phase']]
        p[0] += 1
        p[1] += bool(r['crash'].get('hit')) and r['crash'].get('target_inflight', True)
        p[2] += bool(r['violations'])
        for v in r['violations']:
            fails[re.sub(r'\d+', 'N', v.split(', ex.')[0])[:120]] += 1
    summ = {'cycles': len(rs), 'failed_cycles': sum(1 for r in rs if r['violations']),
            'by_phase': {k: {'cycles': v[0], 'hit': v[1], 'failed': v[2]} for k, v in by_phase.items()},
            'failures': dict(fails)}
    with open(os.path.join(out, 'summary.json'), 'w') as f:
        json.dump(summ, f, indent=1)
    log('SUMMARY ' + json.dumps(summ))


if __name__ == '__main__':
    main()
