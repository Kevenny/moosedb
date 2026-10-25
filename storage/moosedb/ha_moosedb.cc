/*
  MooseDB storage engine — time-series storage for MariaDB.

  Copyright (c) 2026 Kevenny

  This program is free software; you can redistribute it and/or modify
  it under the terms of the GNU General Public License as published by
  the Free Software Foundation; version 2 of the License.
*/

#define MYSQL_SERVER 1

#include "ha_moosedb.h"
#include "moosedb_gbh.h"

#include <algorithm>
#include <atomic>
#include <cctype>
#include <climits>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <filesystem>
#include <iterator>
#include <system_error>
#include <thread>

#include "my_global.h"
#include "sql_class.h"
#include "item_cmpfunc.h"
#include "key.h"
#include "log.h"
#include "sql_acl.h"
#include "sql_parse.h"
#include "sql_i_s.h"
#include "sql_table.h"
#include <mysql/service_encryption.h>

static handlerton *moosedb_hton;

/* ── Helpers ──────────────────────────────────────────────────────────── */

namespace {

/*
  RAII: while alive, every diagnostic raised on `thd` is swallowed (the server
  raises its own when it evaluates the WHERE) and `flag` is true. The
  destructor pops the handler and restores `flag`, whatever the exit path.
*/
class Quiet_capture
{
  THD *thd_;
  bool &flag_, &flag2_;
  bool saved_, saved2_;
  Dummy_error_handler h_;

public:
  Quiet_capture(THD *thd, bool &flag, bool &flag2)
    : thd_(thd), flag_(flag), flag2_(flag2), saved_(flag), saved2_(flag2)
  {
    flag_= flag2_= true;
    if (thd_)
      thd_->push_internal_handler(&h_);
  }
  ~Quiet_capture()
  {
    if (thd_)
      thd_->pop_internal_handler();
    flag_= saved_;
    flag2_= saved2_;
  }
  Quiet_capture(const Quiet_capture &)= delete;
  Quiet_capture &operator=(const Quiet_capture &)= delete;
};

/*
  A constant that converts to DATETIME with no diagnostic at all: a string
  literal 'YYYY-MM-DD[ HH:MM:SS[.f{1,6}]]' naming a valid calendar day. Anything else
  is left to the server: converting it here could raise (or, through a cached
  conversion, hide) a warning the statement must report exactly once.
*/
bool scan_literal_ok(Item *cst)
{
  /* BETWEEN/comparisons wrap the converted constant in a cache: judge the
     original item it was made from. */
  if (cst->type() == Item::CACHE_ITEM)
  {
    cst= static_cast<Item_cache *>(cst)->get_example();
    if (!cst)
      return false;
  }
  if (cst->type() != Item::CONST_ITEM || cst->cmp_type() != STRING_RESULT)
    return false;
  String tmp;
  String *r= cst->val_str(&tmp);
  if (!r || cst->null_value || r->charset()->mbminlen != 1)
    return false;
  const char *p= r->ptr();
  const size_t n= r->length();
  auto dig= [&](size_t i) { return i < n && p[i] >= '0' && p[i] <= '9'; };
  auto num= [&](size_t i, size_t len) {
    uint v= 0;
    for (size_t k= 0; k < len; k++)
      v= v * 10 + (uint) (p[i + k] - '0');
    return v;
  };
  if (n != 10 && n != 19 && !(n >= 21 && n <= 26))
    return false;
  for (size_t i= 0; i < 10; i++)
    if (i == 4 || i == 7 ? p[i] != '-' : !dig(i))
      return false;
  const uint y= num(0, 4), mo= num(5, 2), d= num(8, 2);
  const uint dim= mo == 2 ? 28u + (((y % 4 == 0 && y % 100 != 0) || y % 400 == 0) ? 1u : 0u)
                          : (mo == 4 || mo == 6 || mo == 9 || mo == 11) ? 30u : 31u;
  if (y < 1000 || mo < 1 || mo > 12 || d < 1 || d > dim)
    return false;
  if (n == 10)
    return true;
  if (p[10] != ' ' || !dig(11) || !dig(12) || p[13] != ':' || !dig(14) ||
      !dig(15) || p[16] != ':' || !dig(17) || !dig(18))
    return false;
  if (num(11, 2) > 23 || num(14, 2) > 59 || num(17, 2) > 59)
    return false;
  if (n == 19)
    return true;
  if (p[19] != '.')
    return false;
  for (size_t i= 20; i < n; i++)
    if (!dig(i))
      return false;
  return true;
}

constexpr longlong MICROS_PER_SEC= 1000000LL;
constexpr longlong MICROS_PER_DAY= 86400LL * MICROS_PER_SEC;

/* Days since 1970-01-01 (proleptic Gregorian). Mirrors the Rust core. */
longlong days_from_civil(longlong y, unsigned m, unsigned d)
{
  y-= m <= 2;
  const longlong era= (y >= 0 ? y : y - 399) / 400;
  const longlong yoe= y - era * 400;
  const longlong mp= m > 2 ? m - 3 : m + 9;
  const longlong doy= (153 * mp + 2) / 5 + d - 1;
  const longlong doe= yoe * 365 + yoe / 4 - yoe / 100 + doy;
  return era * 146097 + doe - 719468;
}

void civil_from_days(longlong z, longlong *y, unsigned *m, unsigned *d)
{
  z+= 719468;
  const longlong era= (z >= 0 ? z : z - 146096) / 146097;
  const longlong doe= z - era * 146097;
  const longlong yoe= (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
  const longlong doy= doe - (365 * yoe + yoe / 4 - yoe / 100);
  const longlong mp= (5 * doy + 2) / 153;
  *d= (unsigned) (doy - (153 * mp + 2) / 5 + 1);
  *m= (unsigned) (mp < 10 ? mp + 3 : mp - 9);
  *y= yoe + era * 400 + (*m <= 2);
}

longlong floor_div(longlong a, longlong b)
{
  return a / b - ((a % b != 0) && ((a < 0) != (b < 0)));
}

longlong time_to_micros(const MYSQL_TIME &lt)
{
  const longlong days= days_from_civil(lt.year, lt.month, lt.day);
  return (((days * 24 + lt.hour) * 60 + lt.minute) * 60 + lt.second) *
         MICROS_PER_SEC + (longlong) lt.second_part;
}

void micros_to_time(longlong us, MYSQL_TIME *lt)
{
  const longlong days= floor_div(us, MICROS_PER_DAY);
  const longlong tod= us - days * MICROS_PER_DAY;
  longlong y;
  unsigned m, d;
  civil_from_days(days, &y, &m, &d);
  memset(lt, 0, sizeof(*lt));
  lt->year= (uint) y;
  lt->month= m;
  lt->day= d;
  lt->hour= (uint) (tod / (3600 * MICROS_PER_SEC));
  lt->minute= (uint) (tod / (60 * MICROS_PER_SEC) % 60);
  lt->second= (uint) (tod / MICROS_PER_SEC % 60);
  lt->second_part= (ulong) (tod % MICROS_PER_SEC);
  lt->time_type= MYSQL_TIMESTAMP_DATETIME;
}

/*
  micros_to_time() with a one-day cache: consecutive rows of a scan usually
  share the day, which saves civil_from_days() (a dozen divisions) per row.
*/
void micros_to_time_cached(longlong us, MYSQL_TIME *lt, longlong *cday,
                           longlong *cy, unsigned *cm, unsigned *cd)
{
  const longlong days= floor_div(us, MICROS_PER_DAY);
  const longlong tod= us - days * MICROS_PER_DAY;
  if (days != *cday)
  {
    civil_from_days(days, cy, cm, cd);
    *cday= days;
  }
  memset(lt, 0, sizeof(*lt));
  lt->year= (uint) *cy;
  lt->month= *cm;
  lt->day= *cd;
  lt->hour= (uint) (tod / (3600 * MICROS_PER_SEC));
  lt->minute= (uint) (tod / (60 * MICROS_PER_SEC) % 60);
  lt->second= (uint) (tod / MICROS_PER_SEC % 60);
  lt->second_part= (ulong) (tod % MICROS_PER_SEC);
  lt->time_type= MYSQL_TIMESTAMP_DATETIME;
}

bool is_tag_comment(const LEX_CSTRING &c)
{
  const char *s= c.str, *e= c.str + c.length;
  while (s < e && my_isspace(system_charset_info, *s)) s++;
  while (e > s && my_isspace(system_charset_info, e[-1])) e--;
  return e - s == 3 && !my_strnncoll(system_charset_info,
                                    (const uchar *) s, 3,
                                    (const uchar *) "TAG", 3);
}

bool is_temporal(const Field *f)
{
  switch (f->real_type()) {
  case MYSQL_TYPE_DATETIME:
  case MYSQL_TYPE_DATETIME2:
  case MYSQL_TYPE_TIMESTAMP:
  case MYSQL_TYPE_TIMESTAMP2:
    return true;
  default:
    return false;
  }
}

/* INT / BIGINT (signed or unsigned): usable as a seconds-since-epoch axis. */
bool is_int_axis(const Field *f)
{
  switch (f->real_type()) {
  case MYSQL_TYPE_LONG:
  case MYSQL_TYPE_LONGLONG:
    return true;
  default:
    return false;
  }
}

/* TINYINT..BIGINT: a TAG of these types is stored as val_str()'s text. */
bool is_int_tag_field(const Field *f)
{
  switch (f->real_type()) {
  case MYSQL_TYPE_TINY:
  case MYSQL_TYPE_SHORT:
  case MYSQL_TYPE_INT24:
  case MYSQL_TYPE_LONG:
  case MYSQL_TYPE_LONGLONG:
    return true;
  default:
    return false;
  }
}

/*
  A collation under which two strings are equal only when their bytes are:
  binary, or a binary-sorted NO PAD one. A UNIQUE key may only hold TAG
  columns of these, since the series (one per distinct byte string) are what
  the uniqueness is checked inside.
*/
bool tag_cs_exact(const CHARSET_INFO *cs)
{
  return cs == &my_charset_bin ||
         ((cs->state & MY_CS_BINSORT) && (cs->state & MY_CS_NOPAD));
}

/*
  Seconds -> µs for a range bound taken from an index key: saturates instead
  of wrapping (a bound beyond what can be stored selects nothing / everything,
  exactly like the unbounded side).
*/
longlong secs_to_micros_saturating(longlong secs, bool field_unsigned)
{
  if (field_unsigned && secs < 0)       /* BIGINT UNSIGNED above LONGLONG_MAX */
    return LONGLONG_MAX;
  longlong us;
  if (__builtin_mul_overflow(secs, MICROS_PER_SEC, &us))
    return secs < 0 ? LONGLONG_MIN : LONGLONG_MAX;
  return us;
}

/* Message of the last Rust error on this thread ("" if none). */
std::string take_last_error()
{
  char *msg= moosedb_last_error();
  std::string s= msg ? msg : "";
  moosedb_free_str(msg);
  return s;
}

/* Replaces every occurrence of `from` in `s` by `to`. */
void replace_all(std::string *s, const std::string &from, const std::string &to)
{
  if (from.empty())
    return;
  for (size_t pos= 0; (pos= s->find(from, pos)) != std::string::npos;
       pos+= to.size())
    s->replace(pos, from.size(), to);
}

/* "db.table" label of a table, never a host path. */
std::string table_label(const TABLE_SHARE *s)
{
  if (!s)
    return "?";
  return std::string(s->db.str ? s->db.str : "?") + "." +
         (s->table_name.str ? s->table_name.str : "?");
}

/* Characters that can be part of a path component: a match must not start
   or end inside one (so "test/t" never matches inside "latest/t2"). */
bool is_path_char(char c)
{
  return isalnum((unsigned char) c) || c == '_' || c == '-' || c == '.' ||
         c == '/' || c == '#' || c == '@';
}

/* Like replace_all, but only for matches delimited by non-path characters
   (or a '/' right after, for a directory prefix). */
void replace_bounded(std::string *s, const std::string &from,
                     const std::string &to)
{
  if (from.empty())
    return;
  for (size_t pos= 0; (pos= s->find(from, pos)) != std::string::npos;)
  {
    const bool left= pos == 0 || !is_path_char((*s)[pos - 1]);
    const size_t end= pos + from.size();
    const bool right= end >= s->size() || (*s)[end] == '/' ||
                      !is_path_char((*s)[end]);
    if (left && right)
    {
      s->replace(pos, from.size(), to);
      pos+= to.size();
    }
    else
      pos++;
  }
}

/* Spellings of the data directory, computed once. Empty when the datadir is
   relative or too short to be told apart from ordinary text ("./", "/"). */
const std::vector<std::string> &datadir_variants()
{
  static const std::vector<std::string> v= [] {
    std::vector<std::string> out;
    std::string home(mysql_real_data_home);
    if (home.size() > 1 && home[0] == '/')
    {
      out.push_back(home);
      std::error_code ec;
      std::filesystem::path abs= std::filesystem::absolute(home, ec);
      if (!ec)
        out.push_back(abs.lexically_normal().string());
      ec.clear();
      std::filesystem::path canon= std::filesystem::canonical(home, ec);
      if (!ec)
        out.push_back((canon / "").string());
      std::sort(out.begin(), out.end(),
                [](const std::string &x, const std::string &y)
                { return x.size() > y.size(); });
      out.erase(std::unique(out.begin(), out.end()), out.end());
    }
    return out;
  }();
  return v;
}

/*
  Removes host paths (the table directory as given by the server, its
  absolute form, and the data directory) from an engine message so that
  clients only see db.table.
*/
std::string scrub_paths(std::string msg, const std::string &name,
                        const std::string &label)
{
  if (!name.empty())
  {
    replace_bounded(&msg, name, label);
    std::error_code ec;
    std::filesystem::path abs= std::filesystem::absolute(name, ec);
    if (!ec)
      replace_bounded(&msg, abs.lexically_normal().string(), label);
    /* "./db/t" -> "db/t", only as a whole path (bounded) */
    if (name.compare(0, 2, "./") == 0)
      replace_bounded(&msg, name.substr(2), label);
  }
  for (const std::string &home : datadir_variants())
    replace_all(&msg, home, "");
  return msg;
}

/* "db.table" label out of a server table path ("./db/table"), for messages
   raised where no TABLE_SHARE is at hand (DROP, RENAME). */
std::string label_from_path(const std::string &name)
{
  std::string n= name;
  while (!n.empty() && n.back() == '/')
    n.pop_back();
  const size_t last= n.rfind('/');
  if (last == std::string::npos)
    return n.empty() ? "?" : n;
  const size_t prev= last ? n.rfind('/', last - 1) : std::string::npos;
  const std::string tbl= n.substr(last + 1);
  const std::string db= prev == std::string::npos
                          ? "" : n.substr(prev + 1, last - prev - 1);
  return db.empty() || db == "." ? tbl : db + "." + tbl;
}

/*
  The one place engine messages become client-visible: takes the pending
  Rust error of this thread and strips every host path from it (the table
  directory in all its spellings and the data directory).
*/
std::string client_error(const std::string &path, const std::string &label)
{
  return scrub_paths(take_last_error(), path, label);
}

/* Engine-private error codes, reported through get_error_message(). */
constexpr int HA_ERR_MOOSEDB_BASE= HA_ERR_LAST + 1000;

} // namespace

/* ── System variables ─────────────────────────────────────────────────── */

static const char *wal_sync_mode_names[]= {"fsync", "write", NullS};
static TYPELIB wal_sync_mode_typelib= CREATE_TYPELIB_FOR(wal_sync_mode_names);
enum { WAL_SYNC_FSYNC= 0, WAL_SYNC_WRITE= 1 };

static ulong srv_wal_sync_mode= WAL_SYNC_FSYNC;
static ulonglong srv_memtable_flush_threshold= 64ULL << 20;
static uint srv_compaction_trigger_chunks= 10;
static uint srv_compaction_threads= 2;
static uint srv_retention_check_interval= 3600;
static double srv_bloom_fpr= 0.01;
static ulonglong srv_chunk_cache_size= 128ULL << 20;
static uint srv_max_open_chunks= 100;
static ulonglong srv_batch_memory_budget= 1ULL << 30;
static uint srv_unique_lock_wait_timeout= 50;
static my_bool srv_recover_damaged_chunks= 0;

/* Mirrors the current values into the Rust core. */
static void push_globals()
{
  TFGlobalSettings s= {};
  s.retention_check_interval_secs= srv_retention_check_interval;
  s.compaction_trigger_chunks= srv_compaction_trigger_chunks;
  s.bloom_filter_false_positive_rate= srv_bloom_fpr;
  s.chunk_cache_bytes= srv_chunk_cache_size;
  s.max_open_chunks= srv_max_open_chunks;
  s.batch_memory_budget_bytes= srv_batch_memory_budget;
  s.recover_damaged_chunks= srv_recover_damaged_chunks;
  if (moosedb_set_globals(&s) != TF_OK)
    sql_print_warning("MooseDB: cannot apply settings: %s",
                      take_last_error().c_str());
}

template <typename T>
static void update_global(THD *, struct st_mysql_sys_var *, void *var,
                          const void *save)
{
  *static_cast<T *>(var)= *static_cast<const T *>(save);
  push_globals();
}

static MYSQL_SYSVAR_ENUM(wal_sync_mode, srv_wal_sync_mode, PLUGIN_VAR_RQCMDARG,
  "WAL durability at statement end: fsync (every committed row survives an "
  "OS crash) or write (rows survive a mysqld crash, not an OS crash)",
  NULL, NULL, WAL_SYNC_FSYNC, &wal_sync_mode_typelib);

static MYSQL_SYSVAR_ULONGLONG(memtable_flush_threshold,
  srv_memtable_flush_threshold, PLUGIN_VAR_RQCMDARG,
  "MemTable size in bytes that triggers a flush to a chunk, for tables "
  "without an explicit MEMTABLE_SIZE. Applies to tables opened afterwards. "
  "At most 96 MiB, so a flushed column block stays below the 256 MiB limit",
  NULL, NULL, 64ULL << 20, 1ULL << 20, 96ULL << 20, 0);

static MYSQL_SYSVAR_UINT(compaction_trigger_chunks,
  srv_compaction_trigger_chunks, PLUGIN_VAR_RQCMDARG,
  "Number of chunks in one time bucket that triggers background compaction",
  NULL, update_global<uint>, 10, 2, 10000, 0);

static MYSQL_SYSVAR_UINT(compaction_threads, srv_compaction_threads,
  PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
  "Background maintenance threads (compaction and retention)",
  NULL, NULL, 2, 1, 64, 0);

static MYSQL_SYSVAR_UINT(retention_check_interval,
  srv_retention_check_interval, PLUGIN_VAR_RQCMDARG,
  "Seconds between two RETENTION_PERIOD sweeps of the same table by the "
  "background threads; 0 disables background sweeps",
  NULL, update_global<uint>, 3600, 0, UINT_MAX, 0);

static MYSQL_SYSVAR_DOUBLE(bloom_filter_false_positive_rate, srv_bloom_fpr,
  PLUGIN_VAR_RQCMDARG,
  "Target false-positive rate of the per-chunk series Bloom filters "
  "(applies to chunks written afterwards)",
  NULL, update_global<double>, 0.01, 0.000001, 0.5, 0);

static MYSQL_SYSVAR_ULONGLONG(chunk_cache_size, srv_chunk_cache_size,
  PLUGIN_VAR_RQCMDARG,
  "Bytes of decoded chunk blocks and decoded series cached in memory (0 "
  "disables the caches). Shrinking it at runtime evicts immediately",
  NULL, update_global<ulonglong>, 128ULL << 20, 0, ULONGLONG_MAX, 0);

static MYSQL_SYSVAR_UINT(max_open_chunks, srv_max_open_chunks,
  PLUGIN_VAR_RQCMDARG,
  "Maximum chunk files kept open between reads",
  NULL, update_global<uint>, 100, 1, 1000000, 0);

static MYSQL_SYSVAR_ULONGLONG(batch_memory_budget, srv_batch_memory_budget,
  PLUGIN_VAR_RQCMDARG,
  "Bytes the row buffers of all open statement batches may hold together; "
  "above it, batches holding at least 1 MiB spill to disk early",
  NULL, update_global<ulonglong>, 1ULL << 30, 16ULL << 20, 1ULL << 40, 0);

static MYSQL_SYSVAR_BOOL(recover_damaged_chunks, srv_recover_damaged_chunks,
  PLUGIN_VAR_OPCMDARG,
  "Recovery switch, OFF by default: when a chunk listed in the authenticated "
  "MANIFEST of an encrypted table is damaged, open the table without that "
  "chunk (set aside as .corrupt, logged loudly) instead of refusing. The data "
  "of that chunk is lost; restoring the file from a backup is preferable. "
  "Turn it OFF again afterwards",
  NULL, update_global<my_bool>, 0);

static MYSQL_SYSVAR_UINT(unique_lock_wait_timeout, srv_unique_lock_wait_timeout,
  PLUGIN_VAR_RQCMDARG,
  "Seconds an INSERT into a table with a UNIQUE/PRIMARY key waits for another "
  "statement that is inserting into the same series before failing with a "
  "lock wait timeout (0 = fail at once)",
  NULL, NULL, 50, 0, 86400, 0);

/*
  Aggregation done by the engine (group_by_handler, moosedb_gbh.cc), only for
  queries it recognises (moosedb_gbh_debug shows the verdict); others run the
  normal way. With the pushdown OFF none of these has any effect.
*/
static MYSQL_THDVAR_BOOL(group_by_pushdown, PLUGIN_VAR_RQCMDARG,
  "Let the engine compute GROUP BY aggregates of queries on MooseDB tables "
  "(COUNT/SUM/AVG/MIN/MAX); queries it cannot handle run the normal way",
  NULL, NULL, 0);

static MYSQL_THDVAR_BOOL(gbh_debug, PLUGIN_VAR_RQCMDARG,
  "Report, as a note per query, whether the GROUP BY pushdown recognises it "
  "(\"gbh: accepted\" or \"gbh: refused: <reason>\"); for tests",
  NULL, NULL, 0);

static MYSQL_THDVAR_ULONGLONG(agg_max_memory, PLUGIN_VAR_RQCMDARG,
  "Bytes one pushed-down GROUP BY may use for its groups; above it the "
  "statement fails with an error",
  NULL, NULL, 1ULL << 30, 16ULL << 20, 1ULL << 40, 0);

static uint default_agg_threads()
{
  return std::clamp<uint>(std::thread::hardware_concurrency(), 1, 4);
}

static uint srv_agg_threads= 1;
static MYSQL_SYSVAR_UINT(agg_threads, srv_agg_threads, PLUGIN_VAR_RQCMDARG,
  "Threads one pushed-down GROUP BY uses to scan chunks (default: the number "
  "of CPUs, at most 4)",
  NULL, NULL, default_agg_threads(), 1, 64, 0);

bool moosedb_group_by_pushdown_enabled(THD *thd)
{
  return thd && THDVAR(thd, group_by_pushdown);
}

bool moosedb_gbh_debug_enabled(THD *thd)
{
  return thd && THDVAR(thd, gbh_debug);
}


/* Queries whose GROUP BY the engine computed itself (group_by_handler). */
static ulonglong srv_gbh_queries= 0;
void moosedb_gbh_count()
{
  __atomic_fetch_add(&srv_gbh_queries, 1, __ATOMIC_RELAXED);
}

/* Cumulative time of the filter-dictionary phase and of the aggregation proper, and the dictionary values evaluated. */
static ulonglong srv_gbh_dict_us= 0, srv_gbh_agg_us= 0, srv_gbh_dict_values= 0, srv_gbh_eval_us= 0, srv_gbh_post_us= 0;
void moosedb_gbh_phase(ulonglong dict_us, ulonglong agg_us, ulonglong dict_values, ulonglong eval_us, ulonglong post_us)
{
  __atomic_fetch_add(&srv_gbh_dict_us, dict_us, __ATOMIC_RELAXED);
  __atomic_fetch_add(&srv_gbh_agg_us, agg_us, __ATOMIC_RELAXED);
  __atomic_fetch_add(&srv_gbh_dict_values, dict_values, __ATOMIC_RELAXED);
  __atomic_fetch_add(&srv_gbh_eval_us, eval_us, __ATOMIC_RELAXED);
  __atomic_fetch_add(&srv_gbh_post_us, post_us, __ATOMIC_RELAXED);
}

/* Shortcuts taken: exact-value filters sent to the core, and queries whose top-N the core did. */
static ulonglong srv_gbh_exact_filters= 0, srv_gbh_topn= 0;
void moosedb_gbh_shortcuts(ulonglong exact, ulonglong topn)
{
  __atomic_fetch_add(&srv_gbh_exact_filters, exact, __ATOMIC_RELAXED);
  __atomic_fetch_add(&srv_gbh_topn, topn, __ATOMIC_RELAXED);
}

/* Zone-map / Bloom pruning counters of the core (Moosedb_prune_*). */
static int show_prune_stats(THD *, SHOW_VAR *var, void *buff,
                            system_status_var *, enum_var_type)
{
  struct out_t
  {
    ulonglong v[7];
    SHOW_VAR vars[8];
  };
  static_assert(sizeof(out_t) <= SHOW_VAR_FUNC_BUFF_SIZE, "status buffer");
  if (!var || !buff)
    return 1;
  out_t *o= static_cast<out_t *>(buff);
  TFPruneStats st{};
  if (moosedb_prune_stats(&st) != TF_OK)
  {
    take_last_error();
    st= TFPruneStats{};
  }
  const ulonglong vals[7]= {st.entries_tested, st.pruned_by_zone,
                            st.pruned_by_bloom, st.rows_skipped,
                            st.blocks_skipped, st.blocks_read,
                            st.block_read_ns};
  static const char *const names[7]= {
    "entries_tested", "zone_pruned", "bloom_pruned", "rows_skipped",
    "blocks_skipped", "blocks_read", "block_read_ns"};
  for (int i= 0; i < 7; i++)
  {
    o->v[i]= vals[i];
    o->vars[i]= {names[i], (char *) &o->v[i], SHOW_LONGLONG};
  }
  o->vars[7]= {NullS, NullS, SHOW_LONG};
  var->type= SHOW_ARRAY;
  var->value= (char *) o->vars;
  return 0;
}

static SHOW_VAR moosedb_status_variables[]= {
  {"prune", (char *) &show_prune_stats, SHOW_FUNC},
  {"gbh_queries", (char *) &srv_gbh_queries, SHOW_LONGLONG},
  {"gbh_dict_us", (char *) &srv_gbh_dict_us, SHOW_LONGLONG},
  {"gbh_agg_us", (char *) &srv_gbh_agg_us, SHOW_LONGLONG},
  {"gbh_dict_values", (char *) &srv_gbh_dict_values, SHOW_LONGLONG},
  {"gbh_eval_us", (char *) &srv_gbh_eval_us, SHOW_LONGLONG},
  {"gbh_post_us", (char *) &srv_gbh_post_us, SHOW_LONGLONG},
  {"gbh_exact_filters", (char *) &srv_gbh_exact_filters, SHOW_LONGLONG},
  {"gbh_topn", (char *) &srv_gbh_topn, SHOW_LONGLONG},
  {NullS, NullS, SHOW_LONG}
};
static struct st_mysql_sys_var *moosedb_system_variables[]= {
  MYSQL_SYSVAR(group_by_pushdown),
  MYSQL_SYSVAR(gbh_debug),
  MYSQL_SYSVAR(agg_max_memory),
  MYSQL_SYSVAR(agg_threads),
  MYSQL_SYSVAR(wal_sync_mode),
  MYSQL_SYSVAR(memtable_flush_threshold),
  MYSQL_SYSVAR(compaction_trigger_chunks),
  MYSQL_SYSVAR(compaction_threads),
  MYSQL_SYSVAR(retention_check_interval),
  MYSQL_SYSVAR(bloom_filter_false_positive_rate),
  MYSQL_SYSVAR(chunk_cache_size),
  MYSQL_SYSVAR(max_open_chunks),
  MYSQL_SYSVAR(batch_memory_budget),
  MYSQL_SYSVAR(recover_damaged_chunks),
  MYSQL_SYSVAR(unique_lock_wait_timeout),
  NULL
};

/* ── Table options ────────────────────────────────────────────────────── */

ha_create_table_option moosedb_table_option_list[]=
{
  HA_TOPTION_STRING("CHUNK_INTERVAL", chunk_interval),
  HA_TOPTION_STRING("RETENTION_PERIOD", retention_period),
  HA_TOPTION_STRING("COMPRESSION", compression),
  HA_TOPTION_NUMBER("COMPRESSION_LEVEL", compression_level, 3, 1, 19, 1),
  HA_TOPTION_STRING("HOT_THRESHOLD", hot_threshold),
  HA_TOPTION_NUMBER("MEMTABLE_SIZE", memtable_size, 0, 0, 1ULL << 40, 1),
  HA_TOPTION_STRING("TIMESTAMP_COLUMN", timestamp_column),
  HA_TOPTION_BOOL("ENCRYPTION", encryption, 0),
  HA_TOPTION_NUMBER("ENCRYPTION_KEY_ID", encryption_key_id, 1, 1, UINT_MAX32, 1),
  HA_TOPTION_END
};

/* ── Encryption keys ──────────────────────────────────────────────────── */

/* Called by the Rust core (any thread) to fetch keys from the server's key
   management plugin. version 0 = latest. */
static int32_t moosedb_key_callback(uint32_t key_id, uint32_t version,
                                     uint32_t *out_version, uint8_t *out_key)
{
  uint v= version ? version : encryption_key_get_latest_version(key_id);
  if (v == ENCRYPTION_KEY_VERSION_INVALID)
    return 1;
  uint len= 32;
  if (encryption_key_get(key_id, v, out_key, &len))
    return 1;
  if (len != 32)
    return 2;                 /* AES-256 needs a 256-bit key */
  *out_version= v;
  return 0;
}

/* ── Layout ───────────────────────────────────────────────────────────── */

int tf_layout::tag_position(uint field_index) const
{
  int pos= 0;
  for (const tf_column &c : columns)
  {
    if (c.conv != tf_conv::TAG)
      continue;
    if (c.field_index == field_index)
      return pos;
    pos++;
  }
  return -1;
}

namespace {

/*
  Builds the column layout of a table and validates that its definition is
  something MooseDB can store. On error, a message is written to `err`.
*/
bool build_layout(TABLE_SHARE *s, tf_layout *out, std::string *err)
{
  tf_layout l;
  const ha_table_option_struct *opt= s->option_struct;

  Field *ts_field= nullptr;
  if (opt && opt->timestamp_column && opt->timestamp_column[0])
  {
    for (uint i= 0; i < s->fields && !ts_field; i++)
      if (!my_strcasecmp(system_charset_info, s->field[i]->field_name.str,
                         opt->timestamp_column))
        ts_field= s->field[i];
    if (!ts_field)
    {
      *err= std::string("TIMESTAMP_COLUMN '") + opt->timestamp_column +
            "' does not exist";
      return true;
    }
  }
  else
  {
    /* Default: the column of the first index on a temporal column. */
    for (uint k= 0; k < s->keys && !ts_field; k++)
    {
      Field *f= s->key_info[k].key_part[0].field;
      if (f && is_temporal(f))
        ts_field= f;
    }
  }
  if (!ts_field)
  {
    *err= "a MooseDB table needs a DATETIME/TIMESTAMP column with an index "
          "(INDEX ts_idx (ts)) or TIMESTAMP_COLUMN = '<column>'";
    return true;
  }
  /*
    An INT/BIGINT axis (seconds since 1970 UTC) is accepted only when named
    explicitly by TIMESTAMP_COLUMN: a plain integer index must never silently
    turn an existing table definition into a time axis.
  */
  if (!(is_temporal(ts_field) || is_int_axis(ts_field)) ||
      !ts_field->stored_in_db())
  {
    *err= std::string("timestamp column '") + ts_field->field_name.str +
          "' must be a stored DATETIME, TIMESTAMP, INT or BIGINT column";
    return true;
  }
  if (ts_field->real_maybe_null())
  {
    *err= std::string("timestamp column '") + ts_field->field_name.str +
          "' must be NOT NULL";
    return true;
  }

  /*
    Indexes: the timestamp column alone, and composite indexes
    (tag_1, ..., tag_k [, timestamp]) over TAG columns (see index_flags()).

    One UNIQUE / PRIMARY KEY is accepted, in the form
    (every TAG column of the table, the timestamp column [, 1-2 integer
    NOT NULL columns]) -- the series plus the time (plus a tie-breaker such as
    Zabbix's ns) identify a row, which the core can check without any index
    structure (see unique.rs). Without TAG columns only UNIQUE (timestamp).
    TAG columns of a UNIQUE key must compare byte for byte (integers, or
    strings with a binary NO PAD collation), because the series that the key
    values select are compared byte for byte too.
  */
  size_t n_tag_cols= 0;
  for (uint i= 0; i < s->fields; i++)
    if (s->field[i]->stored_in_db() && is_tag_comment(s->field[i]->comment))
      n_tag_cols++;
  l.indexes.assign(s->keys, tf_index_def());
  for (uint k= 0; k < s->keys; k++)
  {
    const KEY &key= s->key_info[k];
    const uint np= key.user_defined_key_parts;
    tf_index_def &def= l.indexes[k];
    const std::string head= std::string("index '") + key.name.str + "' ";
    const bool unique= (key.flags & HA_NOSAME) != 0;
    const char *const unique_form=
      ". A MooseDB UNIQUE/PRIMARY key is (every TAG column, the timestamp "
      "column [, up to 2 integer NOT NULL columns]), e.g. PRIMARY KEY "
      "(itemid, clock, ns)";
    if (unique)
    {
      def.unique= true;
      if (l.unique_key != MAX_KEY)
      {
        *err= head + "is not supported: a MooseDB table takes at most one "
              "UNIQUE/PRIMARY key";
        return true;
      }
      l.unique_key= k;
    }
    if (np == 1 && key.key_part[0].field == ts_field)
    {
      def.simple_ts= true;
      if (key.key_part[0].key_part_flag & HA_REVERSE_SORT)
      {
        *err= head + "is not supported: descending index parts";
        return true;
      }
      if (unique && n_tag_cols != 0)
      {
        *err= head + "is not supported: the table has TAG columns, so its "
              "UNIQUE/PRIMARY key must list all of them before the timestamp" +
              unique_form;
        return true;
      }
    }
    else
    {
      if (key.flags & (HA_FULLTEXT | HA_SPATIAL))
      {
        *err= head + "is not supported: only ordinary indexes";
        return true;
      }
      for (uint p= 0; p < np; p++)
      {
        const KEY_PART_INFO &kp= key.key_part[p];
        const Field *f= kp.field;
        const bool last= p + 1 == np;
        const std::string col= std::string("column '") +
                               (f ? f->field_name.str : "?") + "'";
        std::string why;
        if (!f || !f->stored_in_db())
          why= col + " is not a stored column";
        else if (def.has_time)
        {
          /* a part after the timestamp: only a UNIQUE key has them */
          if (!unique)
            why= col + " follows the timestamp, which must be the last part";
          else if (is_tag_comment(f->comment))
            why= "TAG " + col + " comes after the timestamp (list the TAG "
                 "columns first)";
          else if (f->real_maybe_null())
            why= col + " must be NOT NULL in a UNIQUE key";
          else if (!is_int_tag_field(f))
            why= col + " must be an integer column (TINYINT..BIGINT) to "
                 "follow the timestamp in a UNIQUE key";
          else if (def.extra_fields.size() >= 2)
            why= "at most 2 columns may follow the timestamp";
        }
        else if (f == ts_field)
          why= (last || unique) ? ""
                                : col + " (the timestamp) must be the last part";
        else if (!is_tag_comment(f->comment))
          why= col + " is not a TAG column or the timestamp column";
        else if (f->real_maybe_null())
          why= "TAG " + col + " must be NOT NULL to be indexed";
        else if (!is_int_tag_field(f) &&
                 f->real_type() != MYSQL_TYPE_VARCHAR &&
                 f->real_type() != MYSQL_TYPE_STRING)
          why= "TAG " + col + " must be an integer, CHAR or VARCHAR to be "
               "indexed";
        else if (unique && !is_int_tag_field(f) && !tag_cs_exact(f->charset()))
          why= "TAG " + col + " needs a binary NO PAD collation (e.g. "
               "utf8mb4_nopad_bin, or BINARY/VARBINARY) in a UNIQUE key, "
               "because rows of series that only differ by case or trailing "
               "spaces would not be detected as duplicates";
        if (why.empty() && f && kp.length != f->key_length())
          why= col + ": index prefixes are not supported";
        if (why.empty() && (kp.key_part_flag & HA_REVERSE_SORT))
          why= col + ": descending index parts are not supported";
        if (why.empty())
          for (uint q= 0; q < p; q++)
            if (key.key_part[q].field == f)
              why= col + " appears twice";
        if (!why.empty())
        {
          *err= head + "is not supported: " + why + ". MooseDB indexes the "
                "timestamp column alone, or TAG columns optionally followed "
                "by the timestamp (INDEX (itemid, clock))" +
                (unique ? std::string(unique_form) : std::string());
          return true;
        }
        if (def.has_time)
          def.extra_fields.push_back(f->field_index);
        else if (f == ts_field)
          def.has_time= true;
        else
          def.tag_fields.push_back(f->field_index);
      }
      if (unique)
      {
        std::string why;
        if (!def.has_time)
          why= "must include the timestamp column after the TAG columns";
        else if (def.tag_fields.empty())
          why= "needs TAG columns (a table without any only takes UNIQUE "
               "on the timestamp column alone)";
        else if (def.tag_fields.size() != n_tag_cols)
          why= "must list every TAG column of the table (the series identity)";
        if (!why.empty())
        {
          *err= head + "is not supported: " + why + unique_form;
          return true;
        }
      }
    }
    if (def.simple_ts)
      l.ts_key= k;
  }
  /* A UNIQUE key (which contains the timestamp) can stand in for the index on
     the timestamp column alone. */
  if (l.ts_key == MAX_KEY && l.unique_key == MAX_KEY)
  {
    *err= std::string("missing index on the timestamp column: add INDEX (") +
          ts_field->field_name.str + ")";
    return true;
  }

  for (uint i= 0; i < s->fields; i++)
  {
    Field *f= s->field[i];
    if (!f->stored_in_db())
      continue;
    if (f->flags & AUTO_INCREMENT_FLAG)
    {
      *err= std::string("AUTO_INCREMENT column '") + f->field_name.str +
            "' is not supported";
      return true;
    }
    tf_column c{i, TF_COL_VARCHAR, tf_conv::STRING};
    if (f == ts_field)
      l.ts_column= (uint) l.columns.size();
    if (is_tag_comment(f->comment))
    {
      if (f == ts_field)
      {
        *err= "the timestamp column cannot be a TAG";
        return true;
      }
      c.type= TF_COL_TAG;
      c.conv= tf_conv::TAG;
      l.columns.push_back(c);
      continue;
    }
    if (f == ts_field && !is_temporal(f))
    {
      /* integer axis: seconds since 1970 UTC, stored as µs (secs * 1e6) */
      c.type= TF_COL_TIMESTAMP;
      c.conv= tf_conv::INT_SECS;
      l.columns.push_back(c);
      continue;
    }
    switch (f->real_type()) {
    case MYSQL_TYPE_DATETIME:
    case MYSQL_TYPE_DATETIME2:
      c.type= TF_COL_TIMESTAMP; c.conv= tf_conv::DATETIME; break;
    case MYSQL_TYPE_TIMESTAMP:
    case MYSQL_TYPE_TIMESTAMP2:
      c.type= TF_COL_TIMESTAMP; c.conv= tf_conv::TIMESTAMP; break;
    case MYSQL_TYPE_TINY:
    case MYSQL_TYPE_SHORT:
    case MYSQL_TYPE_INT24:
    case MYSQL_TYPE_LONG:
    case MYSQL_TYPE_LONGLONG:
    case MYSQL_TYPE_YEAR:
    case MYSQL_TYPE_BIT:
      c.type= TF_COL_INT64; c.conv= tf_conv::INT; break;
    case MYSQL_TYPE_FLOAT:
      c.type= TF_COL_FLOAT32; c.conv= tf_conv::FLOAT; break;
    case MYSQL_TYPE_DOUBLE:
      c.type= TF_COL_FLOAT64; c.conv= tf_conv::DOUBLE; break;
    case MYSQL_TYPE_NEWDECIMAL:
      c.type= TF_COL_DECIMAL; c.conv= tf_conv::DECIMAL; break;
    case MYSQL_TYPE_GEOMETRY:
      *err= std::string("column '") + f->field_name.str +
            "': spatial types are not supported";
      return true;
    default:
      /* VARCHAR, CHAR, TEXT/BLOB, DATE, TIME, ENUM, SET, JSON, UUID, ... */
      break;
    }
    l.columns.push_back(c);
  }
  for (tf_index_def &d : l.indexes)
    for (const uint fi : d.tag_fields)
    {
      const int pos= l.tag_position(fi);
      if (pos < 0)
      {
        *err= "an indexed column is not a stored TAG column";
        return true;
      }
      d.tag_pos.push_back(pos);
    }
  if (l.unique_key != MAX_KEY)
    for (const uint fi : l.indexes[l.unique_key].extra_fields)
    {
      uint pos= 0;
      while (pos < l.columns.size() && l.columns[pos].field_index != fi)
        pos++;
      if (pos >= l.columns.size())
      {
        *err= "a UNIQUE key column is not a stored column";
        return true;
      }
      l.unique_extra_cols.push_back(pos);
    }
  *out= std::move(l);
  return false;
}

/* Owns the strings a TFTableConfig points into. */
struct tf_config_holder
{
  std::vector<const char *> names;
  std::vector<uint8_t> types;
  std::vector<uint32_t> unique_extra;
  TFTableConfig cfg{};

  tf_config_holder(TABLE_SHARE *s, const tf_layout &l)
  {
    const ha_table_option_struct *opt= s->option_struct;
    for (const tf_column &c : l.columns)
    {
      names.push_back(s->field[c.field_index]->field_name.str);
      types.push_back((uint8_t) c.type);
    }
    cfg.data_dir= nullptr;
    cfg.chunk_interval= opt ? opt->chunk_interval : nullptr;
    cfg.retention_period= opt ? opt->retention_period : nullptr;
    cfg.compression= opt ? opt->compression : nullptr;
    cfg.compression_level= opt ? (uint8_t) opt->compression_level : 0;
    cfg.hot_threshold= opt ? opt->hot_threshold : nullptr;
    cfg.memtable_size_bytes= opt && opt->memtable_size
                               ? opt->memtable_size
                               : srv_memtable_flush_threshold;
    cfg.ts_column_index= l.ts_column;
    cfg.column_count= (uint32_t) l.columns.size();
    cfg.column_names= names.data();
    cfg.column_types= types.data();
    cfg.encryption_key_id= opt && opt->encryption
                             ? (uint32_t) opt->encryption_key_id : 0;
    unique_extra= l.unique_extra_cols;
    cfg.unique= l.unique_key != MAX_KEY;
    cfg.unique_extra_count= (uint32_t) unique_extra.size();
    cfg.unique_extra_cols= unique_extra.data();
  }
};

} // namespace

/* ── Share ────────────────────────────────────────────────────────────── */

MooseDB_share::MooseDB_share()
{
  thr_lock_init(&lock);
}

MooseDB_share::~MooseDB_share()
{
  /*
    The last handler of this TABLE_SHARE is gone (FLUSH TABLES, DROP,
    shutdown, table cache eviction): seal the MemTable so data does not
    linger only in the WAL.
  */
  if (table)
  {
    /*
      DROP TABLE (and DROP TEMPORARY TABLE, end of session, REBUILD
      PARTITION) removes the table directory before the last handler is
      closed: there is nothing left to flush into.
    */
    std::error_code ec;
    const bool gone= !path.empty() && !std::filesystem::exists(path, ec) &&
                     !ec;
    if (!gone && moosedb_flush(table.get()) != TF_OK)
      sql_print_warning("MooseDB: flush on close failed for %s: %s",
                        display.c_str(),
                        scrub_paths(take_last_error(), path, display).c_str());
  }
  table.reset();
  thr_lock_delete(&lock);
}

/* ── Handler ──────────────────────────────────────────────────────────── */

ha_moosedb::ha_moosedb(handlerton *hton, TABLE_SHARE *table_arg)
  : handler(hton, table_arg)
{
  ref_length= TF_POSITION_LEN;
}

void tf_batch_closer::operator()(MooseDBBatch *b) const noexcept
{
  /* Never drop rows the server already acknowledged: commit, then warn. */
  if (!b)
    return;
  const bool ok= moosedb_batch_commit(b, false) == TF_OK;
  try
  {
    if (ok)
      sql_print_warning("MooseDB: statement batch was still open on "
                        "cleanup; committed");
    else
      sql_print_warning("MooseDB: statement batch commit failed on cleanup: "
                        "%s", take_last_error().c_str());
  }
  catch (...)
  {
  }
}

ha_moosedb::~ha_moosedb()= default;

int ha_moosedb::map_status(TFStatus status, const char *name)
{
  if (status == TF_OK)
    return 0;
  /* Every message that can reach a client goes through here: no host path. */
  if (share_ && !share_->path.empty())
    last_error_= client_error(share_->path, share_->display);
  else if (name && name[0])
    last_error_= client_error(name, label_from_path(name));
  else
    last_error_= client_error("", "");
  switch (status) {
  case TF_ERR_FULL:      return HA_ERR_RECORD_FILE_FULL;
  case TF_ERR_READONLY:  return HA_ERR_TABLE_READONLY;
  case TF_ERR_CORRUPT:   return HA_ERR_CRASHED_ON_USAGE;
  case TF_ERR_OOM:       return HA_ERR_OUT_OF_MEM;
  case TF_ERR_NOT_FOUND: return HA_ERR_NO_SUCH_TABLE;
  case TF_ERR_CRYPTO:    return HA_ERR_DECRYPTION_FAILED;
  case TF_ERR_DUPLICATE: return HA_ERR_FOUND_DUPP_KEY;
  case TF_ERR_DEADLOCK:  return HA_ERR_LOCK_DEADLOCK;
  case TF_ERR_BUSY:      return HA_ERR_LOCK_WAIT_TIMEOUT;
  default:               return HA_ERR_MOOSEDB_BASE + (int) status;
  }
}

bool ha_moosedb::get_error_message(int error, String *buf)
{
  if (!buf)
    return false;
  if (error == HA_ERR_DECRYPTION_FAILED)
  {
    /* last_error_ was scrubbed by map_status(): no host path in it. */
    const std::string msg= last_error_.empty()
      ? std::string("wrong or unavailable encryption key") : last_error_;
    buf->copy(msg.c_str(), msg.length(), system_charset_info);
  }
  else if (error >= HA_ERR_MOOSEDB_BASE && !last_error_.empty())
    buf->copy(last_error_.c_str(), last_error_.length(), system_charset_info);
  return false;
}

bool ha_moosedb::moosedb_is_timestamp_column(const Field *field) const
{
  if (!share_ || !field)
    return false;
  const tf_layout &l= share_->layout;
  return l.ts_column < l.columns.size() &&
         l.columns[l.ts_column].field_index == field->field_index;
}

bool ha_moosedb::moosedb_column_conv(const Field *field, tf_conv *out) const
{
  if (!share_ || !field || !out)
    return false;
  for (const tf_column &c : share_->layout.columns)
    if (c.field_index == field->field_index)
    {
      *out= c.conv;
      return true;
    }
  return false;
}

bool ha_moosedb::moosedb_gbh_where(Item *where,
                                   std::vector<tf_tag_predicate> *preds,
                                   longlong *lo, longlong *hi)
{
  if (!where || !preds || !lo || !hi)
    return false;
  capture_str_time_= true;
  const bool ok= capture_delete_cond(where, preds, lo, hi);
  capture_str_time_= false;
  return ok;
}

void ha_moosedb::moosedb_gbh_value_preds(Item *where,
                                         std::vector<tf_val_pred> *out)
{
  if (!where || !out)
    return;
  std::vector<tf_val_pred> all;
  collect_value_predicates(where, &all);
  /* TF_PRED_BYTES is dropped on purpose: the gbh already sends exact string sets as TF_AGG_FILTER_VALUES or as the dictionary mask, which the core turns into the same test. */
  for (tf_val_pred &p : all)
    if (p.kind == TF_PRED_INT || p.kind == TF_PRED_FLOAT)
      out->push_back(std::move(p));
}

bool ha_moosedb::moosedb_gbh_series(const std::vector<tf_tag_predicate> &preds,
                                    std::vector<uint64_t> *out)
{
  if (!out)
    return false;
  std::vector<const tf_tag_predicate *> ptrs;
  for (const tf_tag_predicate &p : preds)
    ptrs.push_back(&p);
  return resolve_series(ptrs, out);
}

std::string ha_moosedb::moosedb_gbh_error() const
{
  return share_ ? client_error(share_->path, share_->display)
                : client_error("", "");
}

void moosedb_micros_to_time(longlong us, MYSQL_TIME *lt)
{
  if (lt)
    micros_to_time(us, lt);
}

ulonglong moosedb_agg_max_memory_of(THD *thd)
{
  return thd ? THDVAR(thd, agg_max_memory) : (1ULL << 30);
}

uint moosedb_agg_threads_value() { return srv_agg_threads; }

ha_moosedb *moosedb_handler_of(handler *file)
{
  return (file && file->ht == moosedb_hton) ? static_cast<ha_moosedb *>(file)
                                            : nullptr;
}

MooseDB_share *ha_moosedb::get_share()
{
  lock_shared_ha_data();
  auto *s= static_cast<MooseDB_share *>(get_ha_share_ptr());
  if (!s)
  {
    s= new (std::nothrow) MooseDB_share;
    if (s)
      set_ha_share_ptr(static_cast<Handler_share *>(s));
  }
  unlock_shared_ha_data();
  return s;
}

int ha_moosedb::create(const char *name, TABLE *table_arg, HA_CREATE_INFO *)
{
  DBUG_ENTER("ha_moosedb::create");
  assert(table_arg && name);
  if (!table_arg || !table_arg->s || !name)
    DBUG_RETURN(HA_ERR_INTERNAL_ERROR);

  tf_layout layout;
  std::string err;
  if (build_layout(table_arg->s, &layout, &err))
  {
    my_printf_error(ER_ILLEGAL_HA_CREATE_OPTION, "MooseDB: %s", MYF(0),
                    err.c_str());
    DBUG_RETURN(HA_WRONG_CREATE_OPTION);
  }
  tf_config_holder cfg(table_arg->s, layout);
  TFStatus st= moosedb_table_create(name, &cfg.cfg);
  if (st != TF_OK)
  {
    std::string msg= client_error(name, table_label(table_arg->s));
    my_printf_error(ER_ILLEGAL_HA_CREATE_OPTION, "MooseDB: %s", MYF(0),
                    msg.c_str());
    DBUG_RETURN(HA_WRONG_CREATE_OPTION);
  }
  DBUG_RETURN(0);
}

int ha_moosedb::open(const char *name, int, uint)
{
  DBUG_ENTER("ha_moosedb::open");
  DBUG_ASSERT(table && name);
  if (!table || !table->s || !name)
    DBUG_RETURN(HA_ERR_INTERNAL_ERROR);
  if (!(share_= get_share()))
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);

  lock_shared_ha_data();
  int error= 0;
  if (!share_->table)
  {
    share_->path= name;
    share_->display= table_label(table->s);
    std::string err;
    if (build_layout(table->s, &share_->layout, &err))
    {
      last_error_= err;
      error= HA_ERR_MOOSEDB_BASE + TF_ERR_INVALID_ARG;
    }
    else
    {
      tf_config_holder cfg(table->s, share_->layout);
      MooseDBTable *t= nullptr;
      error= map_status(moosedb_table_open(name, &cfg.cfg, &t));
      share_->table.reset(t);
      if (error)
      {
        sql_print_error("MooseDB: cannot open %s: %s",
                        share_->display.c_str(), last_error_.c_str());
      }
    }
  }
  unlock_shared_ha_data();
  if (error)
    DBUG_RETURN(error);

  thr_lock_data_init(&share_->lock, &lock_data_, NULL);
  str_bufs_.resize(share_->layout.columns.size());
  values_.resize(share_->layout.columns.size());
  DBUG_RETURN(0);
}

int ha_moosedb::close()
{
  DBUG_ENTER("ha_moosedb::close");
  scan_.reset();
  snapshots_.clear();
  int error= 0;
  if (batch_ || del_)
  {
    sql_print_warning("MooseDB: statement batch still open at close; "
                      "committing it");
    error= commit_batch();
  }
  DBUG_RETURN(error);
}

int ha_moosedb::delete_table(const char *name)
{
  DBUG_ENTER("ha_moosedb::delete_table");
  TFStatus st= moosedb_table_drop(name);
  if (st == TF_ERR_NOT_FOUND)
  {
    take_last_error();
    DBUG_RETURN(ENOENT);
  }
  DBUG_RETURN(map_status(st, name));
}

int ha_moosedb::rename_table(const char *from, const char *to)
{
  DBUG_ENTER("ha_moosedb::rename_table");
  TFStatus st= moosedb_table_rename(from, to);
  if (st == TF_ERR_NOT_FOUND)
  {
    take_last_error();
    DBUG_RETURN(ENOENT);
  }
  DBUG_RETURN(map_status(st, from));
}

/* ── Row conversion ───────────────────────────────────────────────────── */

/*
  Converts the record in `buf` into values_ (the TFRow handed to the core).
  Shared by write_row() and delete_row(): a row to delete is found in the
  table by its content, so it must be converted exactly as it was stored.
*/
int ha_moosedb::record_to_row(const uchar *buf, TFRow *out)
{
  assert(table);
  if (!table || !buf || !share_ || !share_->table)
    return HA_ERR_INTERNAL_ERROR;

  const tf_layout &l= share_->layout;
  if (values_.size() < l.columns.size() || str_bufs_.size() < l.columns.size())
    return HA_ERR_INTERNAL_ERROR;
  const my_ptrdiff_t diff= buf - table->record[0];
  MY_BITMAP *old_map= dbug_tmp_use_all_columns(table, &table->read_set);
  int error= 0;

  for (size_t i= 0; i < l.columns.size() && !error; i++)
  {
    const tf_column &c= l.columns[i];
    if (c.field_index >= table->s->fields || !table->field[c.field_index] ||
        i >= values_.size() || i >= str_bufs_.size())
    {
      error= HA_ERR_INTERNAL_ERROR;
      break;
    }
    Field *f= table->field[c.field_index];
    TFValue &v= values_[i];
    v.kind= (uint8_t) c.type;
    v.data.int_val= 0;
    f->move_field_offset(diff);
    v.is_null= f->is_null();
    if (!v.is_null)
    {
      switch (c.conv) {
      case tf_conv::DATETIME:
      {
        MYSQL_TIME lt;
        if (f->get_date(&lt, date_mode_t(0)) || lt.month == 0 || lt.day == 0)
        {
          last_error_= std::string("column '") + f->field_name.str +
                       "': zero or partial dates cannot be stored";
          error= HA_ERR_MOOSEDB_BASE + TF_ERR_INVALID_ARG;
          break;
        }
        v.data.ts_us= time_to_micros(lt);
        break;
      }
      case tf_conv::TIMESTAMP:
      {
        ulong usec= 0;
        const my_time_t secs= f->get_timestamp(&usec);
        v.data.ts_us= (longlong) secs * MICROS_PER_SEC + (longlong) usec;
        break;
      }
      case tf_conv::INT_SECS:
      {
        const longlong secs= f->val_int();
        longlong us= 0;
        /* a BIGINT UNSIGNED above LONGLONG_MAX reads back negative */
        if ((f->is_unsigned() && secs < 0) ||
            __builtin_mul_overflow(secs, MICROS_PER_SEC, &us))
        {
          last_error_= std::string("column '") + f->field_name.str +
                       "': time value outside the supported range "
                       "(+/-9223372036854 seconds since 1970)";
          error= HA_ERR_MOOSEDB_BASE + TF_ERR_INVALID_ARG;
          break;
        }
        v.data.ts_us= us;
        break;
      }
      case tf_conv::INT:
        v.data.int_val= f->val_int();
        break;
      case tf_conv::FLOAT:
        v.data.float32_val= (float) f->val_real();
        break;
      case tf_conv::DOUBLE:
        v.data.float_val= f->val_real();
        break;
      case tf_conv::DECIMAL:
      case tf_conv::STRING:
      case tf_conv::TAG:
      {
        String *s= f->val_str(&str_bufs_[i], &str_bufs_[i]);
        v.data.str_val.ptr= s ? s->ptr() : "";
        v.data.str_val.len= s ? (uint32_t) s->length() : 0;
        break;
      }
      }
    }
    f->move_field_offset(-diff);
  }
  dbug_tmp_restore_column_map(&table->read_set, old_map);
  if (error)
    return error;
  *out= TFRow{(uint32_t) values_.size(), values_.data()};
  return 0;
}

int ha_moosedb::write_row(const uchar *buf)
{
  DBUG_ENTER("ha_moosedb::write_row");
  TFRow row{0, nullptr};
  int error= record_to_row(buf, &row);
  if (error)
    DBUG_RETURN(error);
  /*
    Rows of one statement go into a batch: invisible to other sessions until
    the statement ends, and lost as a whole if the server crashes before.
    The batch is committed by end_bulk_insert(), external_lock(F_UNLCK) or
    reset(), whichever comes first (none of them discards rows).

    Only under plain LOCK TABLES is there no per-statement external_lock, and
    the server calls start_bulk_insert() for multi-row statements there. So a
    row written outside bulk insert with no open batch is a lone single-row
    INSERT: write it directly and sync it right away (moosedb_write_row alone
    does not sync the WAL). In prelocked modes (triggers, stored functions,
    CALL) the server never calls start_bulk_insert(), so always use a batch.
  */
  THD *thd= ha_thd();
  if (!thd)
    DBUG_RETURN(HA_ERR_INTERNAL_ERROR);
  /* a bulk flag left over from an earlier statement is stale */
  if (in_bulk_insert_ && bulk_query_id_ != thd->query_id)
    in_bulk_insert_= false;
  const bool unique= share_->layout.unique_key != MAX_KEY;
  const bool lone= !batch_ && !in_bulk_insert_ &&
                   thd->locked_tables_mode == LTM_LOCK_TABLES;
  if (lone && !unique)
  {
    if (!(error= map_status(moosedb_write_row(share_->table.get(), &row))))
      error= map_status(moosedb_sync_wal(share_->table.get(),
                                         srv_wal_sync_mode == WAL_SYNC_FSYNC));
    DBUG_RETURN(error);
  }
  if (!batch_)
  {
    MooseDBBatch *b= nullptr;
    if ((error= map_status(moosedb_batch_begin(share_->table.get(), &b))))
      DBUG_RETURN(error);
    if (!b)
      DBUG_RETURN(HA_ERR_INTERNAL_ERROR);
    batch_.reset(b);
    /* unique_checks=0: the statement stores rows without checking the key */
    if (unique && (thd->variables.option_bits & OPTION_RELAXED_UNIQUE_CHECKS) &&
        moosedb_batch_set_unique_checks(batch_.get(), false) != TF_OK)
      take_last_error();
  }
  error= write_batch_row(row);
  if (error == HA_ERR_FOUND_DUPP_KEY)
    dup_key_= share_->layout.unique_key;
  if (lone)
  {
    /* UNIQUE table under LOCK TABLES: a one-row statement, committed now */
    const int ce= commit_batch();
    if (!error)
      error= ce;
  }
  DBUG_RETURN(error);
}

/*
  Adds one row to the statement batch. On a table with a UNIQUE key the core
  may report that another statement holds the row's series (TF_ERR_BUSY after
  a short wait): nothing changed, so the same row is offered again until the
  series is free, the statement is killed or the wait times out.
*/
int ha_moosedb::write_batch_row(const TFRow &row)
{
  THD *thd= ha_thd();
  ulonglong waited_from= 0;
  bool waiting= false;
  int error= 0;
  for (;;)
  {
    const TFStatus st= moosedb_batch_write(batch_.get(), &row);
    if (st != TF_ERR_BUSY)
    {
      error= map_status(st);
      break;
    }
    take_last_error();
    if (!waiting)
    {
      waiting= true;
      waited_from= microsecond_interval_timer();
      if (thd)
        thd_wait_begin(thd, THD_WAIT_ROW_LOCK);
    }
    if (thd && thd_killed(thd))
    {
      error= HA_ERR_QUERY_INTERRUPTED;
      break;
    }
    if (microsecond_interval_timer() - waited_from >=
        (ulonglong) srv_unique_lock_wait_timeout * 1000000ULL)
    {
      last_error_= "another statement is inserting into the same series";
      error= HA_ERR_LOCK_WAIT_TIMEOUT;
      break;
    }
  }
  if (waiting && thd)
    thd_wait_end(thd);
  return error;
}

int ha_moosedb::fill_record(uchar *buf, const TFRow &row)
{
  assert(table);
  if (!table || !share_)
    return HA_ERR_INTERNAL_ERROR;
  const tf_layout &l= share_->layout;
  if (row.col_count != l.columns.size() || !row.values)
  {
    last_error_= "row shape does not match the table definition";
    return HA_ERR_MOOSEDB_BASE + TF_ERR_CORRUPT;
  }
  // keep the unused bits of the null bytes as the table defines them (see fill_from_batch)
  if (table->s->null_bytes && table->s->default_values)
    memcpy(buf, table->s->default_values, table->s->null_bytes);
  else
    memset(buf, 0, table->s->null_bytes);
  const my_ptrdiff_t diff= buf - table->record[0];
  MY_BITMAP *old_map= dbug_tmp_use_all_columns(table, &table->write_set);

  for (size_t i= 0; i < l.columns.size(); i++)
  {
    const tf_column &c= l.columns[i];
    const TFValue &v= row.values[i];
    if (c.field_index >= table->s->fields || !table->field[c.field_index])
    {
      dbug_tmp_restore_column_map(&table->write_set, old_map);
      return HA_ERR_INTERNAL_ERROR;
    }
    Field *f= table->field[c.field_index];
    f->move_field_offset(diff);
    if (v.is_null)
    {
      f->set_null();
      f->reset();
    }
    else
    {
      f->set_notnull();
      switch (c.conv) {
      case tf_conv::DATETIME:
      {
        MYSQL_TIME lt;
        micros_to_time(v.data.ts_us, &lt);
        f->store_time_dec(&lt, f->decimals());
        break;
      }
      case tf_conv::TIMESTAMP:
      {
        const longlong us= v.data.ts_us;
        const longlong secs= floor_div(us, MICROS_PER_SEC);
        f->store_timestamp((my_time_t) secs, (ulong) (us - secs * MICROS_PER_SEC));
        break;
      }
      case tf_conv::INT_SECS:
        f->store(floor_div(v.data.ts_us, MICROS_PER_SEC), f->is_unsigned());
        break;
      case tf_conv::INT:
        f->store(v.data.int_val, f->is_unsigned());
        break;
      case tf_conv::FLOAT:
        f->store((double) v.data.float32_val);
        break;
      case tf_conv::DOUBLE:
        f->store(v.data.float_val);
        break;
      case tf_conv::DECIMAL:
      case tf_conv::STRING:
      case tf_conv::TAG:
        f->store(v.data.str_val.ptr ? v.data.str_val.ptr : "",
                 v.data.str_val.len, f->charset());
        break;
      }
    }
    f->move_field_offset(-diff);
  }
  dbug_tmp_restore_column_map(&table->write_set, old_map);
  return 0;
}

/*
  Fills the record from row `row` of the current batch. Only the columns the
  scan was asked for (`active_cols_`) are written; the null bits of the
  others come from `null_template_` (unrequested nullable fields read NULL).
*/
int ha_moosedb::fill_from_batch(uchar *buf, uint32_t row)
{
  assert(table);
  if (!table || !share_ || !buf)
    return HA_ERR_INTERNAL_ERROR;
  const tf_layout &l= share_->layout;
  if (row >= bat_rows_ || !bat_.columns || bat_.col_count != l.columns.size())
    return HA_ERR_INTERNAL_ERROR;
  const size_t nb= table->s->null_bytes;
  if (null_template_.size() == nb)
    memcpy(buf, null_template_.data(), nb);
  else if (nb && table->s->default_values)
    memcpy(buf, table->s->default_values, nb);
  else
    memset(buf, 0, nb);
  const my_ptrdiff_t diff= buf - table->record[0];
  if (buf != tag_buf_)
  {
    std::fill(tag_last_ok_.begin(), tag_last_ok_.end(), 0);
    tag_buf_= buf;
  }
  MY_BITMAP *old_map= dbug_tmp_use_all_columns(table, &table->write_set);
  int rc= 0;

  for (const uint32_t ci : active_cols_)
  {
    if (ci >= l.columns.size() || ci >= tag_last_ok_.size() ||
        ci >= tag_last_start_.size() || ci >= tag_last_len_.size() || ci >= tag_snap_.size())
    {
      rc= HA_ERR_INTERNAL_ERROR;
      break;
    }
    const tf_column &c= l.columns[ci];
    const TFColumnBatch &cb= bat_.columns[ci];
    if (c.field_index >= table->s->fields || !table->field[c.field_index])
    {
      rc= HA_ERR_INTERNAL_ERROR;
      break;
    }
    Field *f= table->field[c.field_index];
    f->move_field_offset(diff);
    if (cb.nulls && cb.nulls[row])
    {
      f->set_null();
      f->reset();
      tag_last_ok_[ci]= 0;
      f->move_field_offset(-diff);
      continue;
    }
    f->set_notnull();
    switch (c.conv) {
    case tf_conv::DATETIME:
    case tf_conv::TIMESTAMP:
    case tf_conv::INT_SECS:
    case tf_conv::INT:
    {
      if (!cb.values)
      {
        rc= HA_ERR_INTERNAL_ERROR;
        break;
      }
      const int64_t x= static_cast<const int64_t *>(cb.values)[row];
      if (c.conv == tf_conv::DATETIME)
      {
        MYSQL_TIME lt;
        micros_to_time_cached(x, &lt, &cached_day_, &cached_y_, &cached_m_,
                              &cached_d_);
        f->store_time_dec(&lt, f->decimals());
      }
      else if (c.conv == tf_conv::TIMESTAMP)
      {
        const longlong secs= floor_div(x, MICROS_PER_SEC);
        f->store_timestamp((my_time_t) secs, (ulong) (x - secs * MICROS_PER_SEC));
      }
      else if (c.conv == tf_conv::INT_SECS)
        f->store(floor_div(x, MICROS_PER_SEC), f->is_unsigned());
      else
        f->store(x, f->is_unsigned());
      break;
    }
    case tf_conv::FLOAT:
      if (!cb.values)
        rc= HA_ERR_INTERNAL_ERROR;
      else
        f->store((double) static_cast<const float *>(cb.values)[row]);
      break;
    case tf_conv::DOUBLE:
      if (!cb.values)
        rc= HA_ERR_INTERNAL_ERROR;
      else
        f->store(static_cast<const double *>(cb.values)[row]);
      break;
    case tf_conv::DECIMAL:
    case tf_conv::STRING:
    case tf_conv::TAG:
    {
      if (!cb.starts || !cb.lens)
      {
        rc= HA_ERR_INTERNAL_ERROR;
        break;
      }
      const uint32_t st= cb.starts[row], len= cb.lens[row];
      if (len != 0 && !cb.data)
      {
        rc= HA_ERR_INTERNAL_ERROR;
        break;
      }
      /*
        TAG rows of one series run share `starts`. The store is skipped only
        when the Field in *this* record buffer still holds exactly the bytes
        the previous store produced (checked against a snapshot of them), so
        a server that overwrote the buffer in between (outer-join NULL row,
        restore_record, ...) always gets a fresh store.
      */
      const enum_field_types rt= f->real_type();
      /*
        Integer TAG columns hold the canonical decimal text of their series; the
        store parses it. Rows of a run repeat the same text, so they take the
        same memo as strings (the snapshot is the field's fixed-width image).
      */
      const bool fixed_int= rt == MYSQL_TYPE_TINY || rt == MYSQL_TYPE_SHORT ||
                            rt == MYSQL_TYPE_INT24 || rt == MYSQL_TYPE_LONG ||
                            rt == MYSQL_TYPE_LONGLONG;
      const bool memo= c.conv == tf_conv::TAG &&
                       (rt == MYSQL_TYPE_VARCHAR || rt == MYSQL_TYPE_STRING || fixed_int) &&
                       f->pack_length() <= 1024;
      if (memo && tag_last_ok_[ci] && tag_last_start_[ci] == st && tag_last_len_[ci] == len &&
          tag_snap_[ci].size() == f->pack_length() &&
          memcmp(tag_snap_[ci].data(), f->ptr, tag_snap_[ci].size()) == 0)
        break;
      f->store(len ? (const char *) cb.data + st : "", len, f->charset());
      if (memo)
      {
        tag_snap_[ci].assign((const char *) f->ptr, f->pack_length());
        tag_last_start_[ci]= st;
        tag_last_len_[ci]= len;
        tag_last_ok_[ci]= 1;
      }
      break;
    }
    }
    f->move_field_offset(-diff);
    if (rc)
      break;
  }
  dbug_tmp_restore_column_map(&table->write_set, old_map);
  return rc;
}

/* ── Statement end / durability ───────────────────────────────────────── */

void tf_delete_closer::operator()(MooseDBDelete *d) const noexcept
{
  moosedb_delete_abort(d);
}

/*
  Applies the rows collected by delete_row(). The handle is released also on
  error. Like an INSERT batch this runs at the end of the statement, whether
  it succeeded or not: rows the server already counted (and binlogged) as
  deleted must really go.
*/
int ha_moosedb::commit_delete()
{
  if (!del_)
    return 0;
  MooseDBDelete *d= del_.release();
  uint64_t n= 0;
  const uint64_t expected= del_rows_;
  del_rows_= 0;
  int error= map_status(moosedb_delete_commit(d, &n));
  if (error)
    sql_print_error("MooseDB: DELETE commit failed: %s", last_error_.c_str());
  else if (n < expected)
  {
    /* Rows the server counted as deleted that the table no longer held (a
       concurrent DELETE took them first): not an error, but never silent. */
    THD *thd= ha_thd();
    if (thd)
      push_warning_printf(thd, Sql_condition::WARN_LEVEL_WARN, ER_UNKNOWN_ERROR,
                          "MooseDB: %llu of %llu rows to delete were already gone",
                          (unsigned long long) (expected - n),
                          (unsigned long long) expected);
  }
  return error;
}

int ha_moosedb::delete_row(const uchar *buf)
{
  DBUG_ENTER("ha_moosedb::delete_row");
  TFRow row{0, nullptr};
  int error= record_to_row(buf, &row);
  if (error)
    DBUG_RETURN(error);
  if (!del_)
  {
    MooseDBDelete *d= nullptr;
    if ((error= map_status(moosedb_delete_begin(
           share_->table.get(), srv_wal_sync_mode == WAL_SYNC_FSYNC, &d))))
      DBUG_RETURN(error);
    if (!d)
      DBUG_RETURN(HA_ERR_INTERNAL_ERROR);
    del_.reset(d);
  }
  error= map_status(moosedb_delete_add(del_.get(), &row));
  if (!error)
    del_rows_++;
  DBUG_RETURN(error);
}

int ha_moosedb::commit_batch()
{
  int del_error= commit_delete();
  if (!batch_)
    return del_error;
  /* commit always releases the handle, also on error */
  MooseDBBatch *b= batch_.release();
  int error= map_status(
    moosedb_batch_commit(b, srv_wal_sync_mode == WAL_SYNC_FSYNC));
  if (error)
  {
    sql_print_error("MooseDB: statement commit failed: %s",
                    last_error_.c_str());
  }
  return error ? error : del_error;
}

void ha_moosedb::start_bulk_insert(ha_rows, uint)
{
  in_bulk_insert_= true;
  THD *thd= ha_thd();
  bulk_query_id_= thd ? thd->query_id : 0;
}

int ha_moosedb::end_bulk_insert()
{
  in_bulk_insert_= false;
  /*
    Replication applier: Write_rows_log_event calls start/end_bulk_insert once
    per row EVENT. Keep the batch open across the events of one statement; it
    is committed (and synced) by external_lock(F_UNLCK) at STMT_END_F, which
    runs (slave_close_thread_tables) before the relay log position is advanced.
  */
  THD *thd= ha_thd();
  if (thd && thd->slave_thread)
    return 0;
  int error= commit_batch();
  /* the server ignores our return value here and reads my_errno */
  if (error)
    my_errno= error;
  return error;
}

int ha_moosedb::external_lock(THD *, int lock_type)
{
  DBUG_ENTER("ha_moosedb::external_lock");
  /*
    F_UNLCK = statement end. MooseDB is non-transactional, so this is the
    point where the client's OK is about to be sent: commit the statement
    batch (also when the statement failed: already-inserted rows are
    binlogged and stay).
  */
  if (lock_type == F_UNLCK)
    in_bulk_insert_= false;
  DBUG_RETURN(lock_type == F_UNLCK ? commit_batch() : 0);
}

/*
  A DELETE with a WHERE runs next to INSERTs (the housekeeper must not stop the
  syncer): the core serializes deletes and inserts and a delete only touches
  rows that existed when it committed. A DELETE without WHERE becomes TRUNCATE
  (delete_all_rows), which keeps the exclusive lock.
*/
static bool delete_needs_exclusive(THD *thd, int cmd)
{
  if (cmd != SQLCOM_DELETE)
    return false;
  const SELECT_LEX *sl= thd->lex ? thd->lex->first_select_lex() : nullptr;
  return !sl || !sl->where;
}

THR_LOCK_DATA **ha_moosedb::store_lock(THD *thd, THR_LOCK_DATA **to,
                                        enum thr_lock_type lock_type)
{
  if (lock_type != TL_IGNORE && lock_data_.type == TL_UNLOCK)
  {
    /*
      Like InnoDB: let INSERTs run concurrently with each other and with
      SELECTs. The core serializes appends internally, and scans read
      immutable snapshots, so row positions stay valid under concurrent
      flushes and compactions. Statements that rebuild or empty the table
      keep the exclusive lock.
    */
    const int cmd= thd_sql_command(thd);
    if (lock_type >= TL_WRITE_CONCURRENT_INSERT && lock_type <= TL_WRITE &&
        !thd_in_lock_tables(thd) && cmd != SQLCOM_TRUNCATE &&
        cmd != SQLCOM_OPTIMIZE && !delete_needs_exclusive(thd, cmd) &&
        cmd != SQLCOM_CREATE_TABLE && cmd != SQLCOM_ALTER_TABLE)
      lock_type= TL_WRITE_ALLOW_WRITE;
    else if (lock_type == TL_READ_NO_INSERT && !thd_in_lock_tables(thd))
      lock_type= TL_READ;
    lock_data_.type= lock_type;
  }
  *to++= &lock_data_;
  return to;
}

/* ── Maintenance ──────────────────────────────────────────────────────── */

int ha_moosedb::delete_all_rows()
{
  DBUG_ENTER("ha_moosedb::delete_all_rows");
  if (!share_ || !share_->table)
    DBUG_RETURN(HA_ERR_INTERNAL_ERROR);
  /*
    A DELETE with a WHERE runs next to INSERTs (store_lock). If the server
    still turns it into delete_all_rows() (the WHERE folded to a constant true
    only after the lock was chosen), a truncate here would discard rows other
    sessions are inserting: refuse, and the server deletes row by row.
  */
  {
    THD *thd= ha_thd();
    if (thd && thd_sql_command(thd) == SQLCOM_DELETE &&
        lock_data_.type == TL_WRITE_ALLOW_WRITE)
      DBUG_RETURN(HA_ERR_WRONG_COMMAND);
  }
  scan_.reset();
  DBUG_RETURN(map_status(moosedb_truncate(share_->table.get())));
}

int ha_moosedb::truncate()
{
  return delete_all_rows();
}

int ha_moosedb::moosedb_force_flush()
{
  if (!share_ || !share_->table)
    return HA_ERR_INTERNAL_ERROR;
  return map_status(moosedb_flush(share_->table.get()));
}

int ha_moosedb::moosedb_compact_chunks(longlong from_us, longlong to_us)
{
  if (!share_ || !share_->table)
    return HA_ERR_INTERNAL_ERROR;
  return map_status(moosedb_compact(share_->table.get(), from_us, to_us));
}

int ha_moosedb::check(THD *thd, HA_CHECK_OPT *)
{
  DBUG_ENTER("ha_moosedb::check");
  if (!share_ || !share_->table)
    DBUG_RETURN(HA_ADMIN_INTERNAL_ERROR);
  uint32_t bad= 0;
  if (int error= map_status(moosedb_check(share_->table.get(), &bad)))
    DBUG_RETURN(error);
  if (bad)
  {
    std::string msg= client_error(share_->path, share_->display);
    push_warning_printf(thd, Sql_condition::WARN_LEVEL_WARN, ER_NOT_KEYFILE,
                        "MooseDB: %u corrupt chunk(s): %s", bad, msg.c_str());
    sql_print_error("MooseDB: CHECK TABLE %s found %u corrupt chunk(s): %s",
                    share_->display.c_str(), bad, msg.c_str());
    DBUG_RETURN(HA_ADMIN_CORRUPT);
  }
  DBUG_RETURN(HA_ADMIN_OK);
}

int ha_moosedb::optimize(THD *thd, HA_CHECK_OPT *)
{
  DBUG_ENTER("ha_moosedb::optimize");
  if (!share_ || !share_->table)
    DBUG_RETURN(HA_ADMIN_INTERNAL_ERROR);
  /* Seal the MemTable, expire old chunks, merge and re-encode the rest. */
  int error= moosedb_force_flush();
  /*
    Retention deletes data: moosedb_apply_retention requires DELETE, while
    OPTIMIZE only needs SELECT+INSERT, so apply it only when the user could
    have run the procedure. Otherwise just flush and compact.
  */
  bool retention_skipped= false;
  if (!error)
  {
    bool may_delete= true;
    if (thd && table && table->s && table->s->tmp_table == NO_TMP_TABLE)
    {
      LEX_CSTRING db_l= table->s->db;
      LEX_CSTRING tbl_l= table->s->table_name;
      TABLE_LIST tl;
      tl.init_one_table(&db_l, &tbl_l, nullptr, TL_READ);
      may_delete= !check_table_access(thd, DELETE_ACL, &tl, false, 1, true);
    }
    if (may_delete)
      error= map_status(moosedb_apply_retention(share_->table.get()));
    else
      retention_skipped= true;
  }
  if (!error)
    error= moosedb_compact_chunks(LONGLONG_MIN, LONGLONG_MAX);
  if (error)
  {
    push_warning_printf(thd, Sql_condition::WARN_LEVEL_WARN, ER_UNKNOWN_ERROR,
                        "MooseDB: %s", last_error_.c_str());
    DBUG_RETURN(HA_ADMIN_FAILED);
  }
  if (retention_skipped)
    push_warning_printf(thd, Sql_condition::WARN_LEVEL_NOTE, ER_UNKNOWN_ERROR,
                        "MooseDB: retention skipped: DELETE privilege "
                        "required");
  DBUG_RETURN(HA_ADMIN_OK);
}

/* ── Condition pushdown ───────────────────────────────────────────────── */

namespace {

const Item_field *field_of(Item *item, TABLE *table)
{
  Item *r= item->real_item();
  if (r->type() != Item::FIELD_ITEM)
    return nullptr;
  auto *f= static_cast<const Item_field *>(r);
  return f->field && f->field->table == table ? f : nullptr;
}

/*
  Evaluates a constant string operand into `cs`'s character set. Returns
  false when the item is not a cheap string constant (not pushable).
*/
bool const_string(Item *item, CHARSET_INFO *cs, std::string *out,
                  bool *is_null)
{
  if (!item->const_item() || item->is_expensive() ||
      item->cmp_type() != STRING_RESULT)
    return false;
  StringBuffer<MAX_FIELD_WIDTH> buf;
  String *s= item->val_str(&buf);
  if (!s || item->null_value)
  {
    *is_null= true;
    return true;
  }
  if (my_charset_same(s->charset(), cs))
    out->assign(s->ptr(), s->length());
  else
  {
    String conv;
    uint errors;
    if (conv.copy(s->ptr(), s->length(), s->charset(), cs, &errors))
      return false;
    out->assign(conv.ptr(), conv.length());
  }
  return true;
}

/*
  Numeric pushdown for integer TAG columns.

  A series of such a column carries the canonical decimal text of the value
  (Field::val_str: no padding, no '+', '-' only for negatives, unsigned
  columns printed unsigned). A comparison `col = K` is turned into "the
  series text equals the canonical text of K" -- but only where that is
  provably what the server's own comparison decides, because the server
  evaluates the whole WHERE again and a series dropped here is gone for good:

  * INT constant: the server compares exactly (signed/unsigned aware).
  * DECIMAL constant: exact too. A fractional value (123.5) equals no integer
    column value; an integral one (123.0) is the integer itself.
  * REAL constant: compared as DOUBLE. Safe only while |K| < 2^53 (every
    integer there is a double, so (double) c == K  <=>  c == K); a fractional
    double equals no integer; a bigger one is not pushed (c = 2^53 + 1 would
    compare equal to 2^53).
  * String constant: the server compares it as DOUBLE (or as an exact integer
    when it converts cleanly); either way a *clean* integer text -- optional
    '-', no leading zeros, no sign '+', no spaces, no exponent, not "-0" --
    with |K| < 2^53 behaves like the integer K. Anything else ('0123', ' 12',
    '12abc', '1e3', '', 'abc', binary strings such as x'3132') is not pushed:
    the server would compare the number it parses, not the text.
  * A value outside the column's range matches no series.
  * ZEROFILL columns are never pushed (their stored text is padded).
*/
enum class num_const { PUSH, NO_MATCH, IS_NULL, REFUSE };

constexpr uint64_t EXACT_DOUBLE_LIMIT= 1ULL << 53;

/* Does -mag / +mag fit the integer Field `f`? (neg implies mag != 0) */
bool int_fits_field(const Field *f, bool neg, uint64_t mag)
{
  const unsigned bits= (unsigned) f->pack_length() * 8;
  if (bits == 0 || bits > 64)
    return false;
  if (f->is_unsigned())
    return !neg && (bits == 64 || mag <= ((uint64_t) 1 << bits) - 1);
  const uint64_t lim= (uint64_t) 1 << (bits - 1);
  return neg ? mag <= lim : mag < lim;
}

std::string canonical_int_text(bool neg, uint64_t mag)
{
  char buf[32];
  snprintf(buf, sizeof(buf), "%s%llu", neg ? "-" : "",
           (unsigned long long) mag);
  return buf;
}

/* Digits [from, to) -> value; false on overflow of uint64_t. */
bool digits_to_u64(const char *p, size_t from, size_t to, uint64_t *out)
{
  uint64_t v= 0;
  for (size_t i= from; i < to; i++)
  {
    const unsigned d= (unsigned) (p[i] - '0');
    if (v > (UINT64_MAX - d) / 10)
      return false;
    v= v * 10 + d;
  }
  *out= v;
  return true;
}

num_const numeric_const(Item *item, const Field *fld, std::string *text,
                       bool *big)
{
  assert(fld);
  assert(big);
  if (!fld || !big)
    return num_const::REFUSE;
  /* set when the value is >= 2^53 in magnitude (or overflows): see add() */
  *big= false;
  if (item->type() == Item::NULL_ITEM)
    return num_const::IS_NULL;
  if (!item->const_item() || item->is_expensive())
    return num_const::REFUSE;

  bool neg= false;
  uint64_t mag= 0;
  switch (item->cmp_type()) {
  case INT_RESULT:
  {
    const longlong v= item->val_int();
    if (item->null_value)
      return num_const::IS_NULL;
    if (item->unsigned_flag || v >= 0)
      mag= (uint64_t) v;
    else
    {
      neg= true;
      mag= (uint64_t) 0 - (uint64_t) v;
    }
    *big= mag >= EXACT_DOUBLE_LIMIT;
    break;
  }
  case DECIMAL_RESULT:
  {
    StringBuffer<MAX_FIELD_WIDTH> buf;
    String *s= item->val_str(&buf);
    if (!s || item->null_value)
      return num_const::IS_NULL;
    const char *p= s->ptr();
    const size_t n= s->length();
    size_t i= 0;
    if (i < n && p[i] == '-')
      neg= true, i++;
    const size_t int_from= i;
    while (i < n && p[i] >= '0' && p[i] <= '9')
      i++;
    const size_t int_to= i;
    size_t frac_from= i, frac_to= i;
    if (i < n && p[i] == '.')
    {
      frac_from= ++i;
      while (i < n && p[i] >= '0' && p[i] <= '9')
        i++;
      frac_to= i;
    }
    if (i != n || (int_to == int_from && frac_to == frac_from))
      return num_const::REFUSE;
    const bool fits= digits_to_u64(p, int_from, int_to, &mag);
    *big= !fits || mag >= EXACT_DOUBLE_LIMIT;
    for (size_t k= frac_from; k < frac_to; k++)
      if (p[k] != '0')
        return num_const::NO_MATCH;       /* exact fraction: equals no integer */
    if (!fits)
      return num_const::NO_MATCH;         /* beyond any column, exact compare */
    break;
  }
  case REAL_RESULT:
  {
    const double d= item->val_real();
    if (item->null_value)
      return num_const::IS_NULL;
    if (!std::isfinite(d) || std::fabs(d) >= (double) EXACT_DOUBLE_LIMIT)
      return num_const::REFUSE;
    if (d != std::trunc(d))
      return num_const::NO_MATCH;
    neg= d < 0;
    mag= (uint64_t) std::fabs(d);
    break;
  }
  case STRING_RESULT:
  {
    if (item->collation.collation == &my_charset_bin)
      return num_const::REFUSE;
    StringBuffer<MAX_FIELD_WIDTH> buf;
    String *s= item->val_str(&buf);
    if (!s || item->null_value)
      return num_const::IS_NULL;
    if (s->charset()->mbminlen != 1)
      return num_const::REFUSE;
    const char *p= s->ptr();
    const size_t n= s->length();
    size_t i= 0;
    if (i < n && p[i] == '-')
      neg= true, i++;
    const size_t from= i;
    if (n - from == 0 || n - from > 16)        /* 16 digits < 2^53 ~ 9.0e15 */
      return num_const::REFUSE;
    for (size_t k= from; k < n; k++)
      if (p[k] < '0' || p[k] > '9')
        return num_const::REFUSE;
    if (p[from] == '0' && (n - from > 1 || neg))   /* '0123', '-0' */
      return num_const::REFUSE;
    if (!digits_to_u64(p, from, n, &mag) || mag >= EXACT_DOUBLE_LIMIT)
      return num_const::REFUSE;
    break;
  }
  default:
    return num_const::REFUSE;
  }
  if (mag == 0)
    neg= false;
  if (!int_fits_field(fld, neg, mag))
    return num_const::NO_MATCH;
  *text= canonical_int_text(neg, mag);
  return num_const::PUSH;
}

} // namespace

void ha_moosedb::collect_tag_predicates(const Item *cond,
                                         std::vector<tf_tag_predicate> *out)
{
  Item *item= const_cast<Item *>(cond);
  if (item->type() == Item::COND_ITEM)
  {
    auto *c= static_cast<Item_cond *>(item);
    if (c->functype() != Item_func::COND_AND_FUNC)
      return;
    List_iterator_fast<Item> it(*c->argument_list());
    while (Item *arg= it++)
      collect_tag_predicates(arg, out);
    return;
  }
  if (item->type() != Item::FUNC_ITEM)
    return;
  auto *fn= static_cast<Item_func *>(item);

  /* column = one of `consts`, compared under `cmp_cs` */
  auto add= [&](const Item_field *f, CHARSET_INFO *cmp_cs, Item **consts,
                uint n) {
    const int pos= share_->layout.tag_position(f->field->field_index);
    if (pos < 0)
      return;
    if (is_int_tag_field(f->field))
    {
      /* Integer TAG: compare canonical decimal text, byte for byte. */
      if (f->field->flags & ZEROFILL_FLAG)
        return;
      tf_tag_predicate p{pos, &my_charset_bin, {}};
      p.values.reserve(n);
      /*
        The server may compare a whole IN list with one aggregated type. If
        any constant is DOUBLE or string (or the types are otherwise not just
        INT/DECIMAL), the comparison can be inexact, and then a constant of
        magnitude >= 2^53 can equal column values that differ from it (2^53
        and 2^53+1 are the same double). So: only INT/DECIMAL-only lists may
        carry such magnitudes; otherwise any big constant refuses the whole
        predicate.
      */
      bool exact= true;
      for (uint i= 0; i < n; i++)
        if (consts[i]->type() != Item::NULL_ITEM &&
            consts[i]->cmp_type() != INT_RESULT &&
            consts[i]->cmp_type() != DECIMAL_RESULT)
          exact= false;
      for (uint i= 0; i < n; i++)
      {
        std::string v;
        bool big= false;
        const num_const r= numeric_const(consts[i], f->field, &v, &big);
        if (big && !exact)
          return;
        switch (r) {
        case num_const::PUSH:
          p.values.push_back(std::move(v));
          break;
        case num_const::NO_MATCH:     /* equals no value of the column */
        case num_const::IS_NULL:      /* "col = NULL" never matches    */
          break;
        case num_const::REFUSE:
          return;                     /* the server decides this one   */
        }
      }
      out->push_back(std::move(p));
      return;
    }
    if (f->field->cmp_type() != STRING_RESULT)
      return;
    tf_tag_predicate p{pos, cmp_cs ? cmp_cs : f->field->charset(), {}};
    for (uint i= 0; i < n; i++)
    {
      std::string v;
      bool is_null= false;
      if (!const_string(consts[i], p.cs, &v, &is_null))
        return;
      if (!is_null)            /* "col = NULL" never matches */
        p.values.push_back(std::move(v));
    }
    out->push_back(std::move(p));
  };

  switch (fn->functype()) {
  case Item_func::EQ_FUNC:
  {
    if (fn->argument_count() != 2)
      return;
    Item **a= fn->arguments();
    for (int side= 0; side < 2; side++)
      if (const Item_field *f= field_of(a[side], table))
        add(f, static_cast<Item_bool_rowready_func2 *>(fn)->compare_collation(),
            &a[1 - side], 1);
    return;
  }
  case Item_func::MULT_EQUAL_FUNC:
  {
    auto *eq= static_cast<Item_equal *>(fn);
    Item *c= eq->get_const();
    if (!c)
      return;
    /*
      For an integer TAG, every member must itself be an integer column: with
      a DOUBLE, DECIMAL or string member the equality is evaluated under
      another comparison type (e.g. DOUBLE, where 2^53 and 2^53+1 are equal),
      not exactly. String TAGs are not affected by this.
    */
    bool all_int= true;
    {
      Item_equal_fields_iterator chk(*eq);
      while (Item *m= chk++)
      {
        Item *r= m->real_item();
        const Field *mf= r->type() == Item::FIELD_ITEM
                           ? static_cast<Item_field *>(r)->field : nullptr;
        if (!mf || !is_int_tag_field(mf))
          all_int= false;
      }
    }
    Item_equal_fields_iterator it(*eq);
    while (Item *fi= it++)
      if (const Item_field *f= field_of(fi, table))
      {
        if (is_int_tag_field(f->field) && !all_int)
          continue;
        add(f, eq->compare_collation(), &c, 1);
      }
    return;
  }
  case Item_func::IN_FUNC:
  {
    auto *in= static_cast<Item_func_in *>(fn);
    if (in->negated || fn->argument_count() < 2)
      return;
    if (const Item_field *f= field_of(fn->arguments()[0], table))
      add(f, in->compare_collation(), fn->arguments() + 1,
          fn->argument_count() - 1);
    return;
  }
  default:
    return;
  }
}

/* ── Value predicates (zone-map / Bloom pruning hints) ────────────────── */

namespace {

enum class vp_op { EQ, GE, LE, BETWEEN };

/* floor/ceil of an exact or small-real constant as int64 (saturating). */
bool int_bounds(Item *c, bool any_real, int64_t *fl, int64_t *ce)
{
  if (!c || !fl || !ce || c->type() == Item::NULL_ITEM || !c->const_item() ||
      c->is_expensive())
    return false;
  const __int128 lim= (__int128) EXACT_DOUBLE_LIMIT;
  __int128 f= 0, g= 0;
  switch (c->cmp_type()) {
  case INT_RESULT:
  {
    const longlong v= c->val_int();
    if (c->null_value || (c->unsigned_flag && v < 0))
      return false;
    f= g= v;
    break;
  }
  case DECIMAL_RESULT:
  {
    StringBuffer<MAX_FIELD_WIDTH> buf;
    String *s= c->val_str(&buf);
    if (!s || c->null_value)
      return false;
    const char *p= s->ptr();
    const size_t n= s->length();
    size_t i= 0;
    bool neg= false;
    if (i < n && p[i] == '-')
      neg= true, i++;
    const size_t from= i;
    while (i < n && p[i] >= '0' && p[i] <= '9')
      i++;
    const size_t to= i;
    bool frac= false;
    if (i < n && p[i] == '.')
    {
      i++;
      while (i < n && p[i] >= '0' && p[i] <= '9')
      {
        if (p[i] != '0')
          frac= true;
        i++;
      }
    }
    if (i != n || to == from)
      return false;
    uint64_t mag= 0;
    if (!digits_to_u64(p, from, to, &mag))
      mag= UINT64_MAX;             /* saturates below */
    const __int128 m= neg ? -(__int128) mag : (__int128) mag;
    f= frac && neg ? m - 1 : m;
    g= frac && !neg ? m + 1 : m;
    break;
  }
  case REAL_RESULT:
  {
    const double d= c->val_real();
    if (c->null_value || !std::isfinite(d) ||
        std::fabs(d) >= (double) EXACT_DOUBLE_LIMIT)
      return false;
    f= (__int128) std::floor(d);
    g= (__int128) std::ceil(d);
    break;
  }
  default:
    return false;                  /* strings, temporal: the server decides */
  }
  /* a REAL sibling makes the server compare as DOUBLE: exact only below 2^53 */
  if (any_real && (f >= lim || f <= -lim || g >= lim || g <= -lim))
    return false;
  auto clamp= [](__int128 x) -> int64_t {
    return x > (__int128) INT64_MAX ? INT64_MAX
         : x < (__int128) INT64_MIN ? INT64_MIN : (int64_t) x;
  };
  *fl= clamp(f);
  *ce= clamp(g);
  return true;
}

bool double_value(Item *c, double *out)
{
  if (!c || !out || c->type() == Item::NULL_ITEM || !c->const_item() ||
      c->is_expensive())
    return false;
  const Item_result t= c->cmp_type();
  if (t != INT_RESULT && t != DECIMAL_RESULT && t != REAL_RESULT)
    return false;
  const double d= c->val_real();
  if (c->null_value || std::isnan(d))
    return false;
  *out= d;
  return true;
}

} // namespace

bool ha_moosedb::moosedb_gbh_const_double(Item *c, double *out)
{
  return double_value(c, out);
}

void ha_moosedb::collect_value_predicates(const Item *cond,
                                          std::vector<tf_val_pred> *out)
{
  assert(out);
  if (!cond || !out || !share_ || !table)
    return;
  Item *item= const_cast<Item *>(cond);
  if (item->type() == Item::COND_ITEM)
  {
    auto *c= static_cast<Item_cond *>(item);
    if (c->functype() != Item_func::COND_AND_FUNC)
      return;
    List_iterator_fast<Item> it(*c->argument_list());
    while (Item *arg= it++)
      collect_value_predicates(arg, out);
    return;
  }
  if (item->type() != Item::FUNC_ITEM)
    return;
  auto *fn= static_cast<Item_func *>(item);

  /* `f` (op) the constants `c[0..n)`; BETWEEN has n == 2 (lo, hi). */
  auto add= [&](const Item_field *f, vp_op op, Item **c, uint n,
                CHARSET_INFO *cmp_cs) {
    if (!f || !f->field || !c || n == 0)
      return;
    const tf_layout &l= share_->layout;
    size_t ci= 0;
    while (ci < l.columns.size() &&
           l.columns[ci].field_index != f->field->field_index)
      ci++;
    if (ci >= l.columns.size() || ci == l.ts_column)
      return;
    const tf_conv conv= l.columns[ci].conv;
    tf_val_pred p;
    p.col= (uint32_t) ci;
    /* NULL constants never match: dropped from lists, fatal elsewhere */
    std::vector<Item *> k;
    for (uint i= 0; i < n; i++)
    {
      if (!c[i])
        return;
      if (c[i]->type() == Item::NULL_ITEM)
      {
        if (op == vp_op::EQ && n > 1)
          continue;
        return;
      }
      k.push_back(c[i]);
    }
    if (k.empty())
      return;
    if (conv == tf_conv::INT)
    {
      switch (f->field->real_type()) {
      case MYSQL_TYPE_TINY: case MYSQL_TYPE_SHORT: case MYSQL_TYPE_INT24:
      case MYSQL_TYPE_LONG:
        break;
      case MYSQL_TYPE_LONGLONG:
        if (f->field->is_unsigned())
          return;                       /* stored wrapped as signed */
        break;
      default:
        return;                         /* YEAR, BIT */
      }
      bool any_real= false;
      for (Item *x : k)
        if (x->cmp_type() == REAL_RESULT)
          any_real= true;
      p.kind= TF_PRED_INT;
      int64_t mn= INT64_MAX, mx= INT64_MIN;
      std::vector<std::pair<int64_t, int64_t>> b;
      for (Item *x : k)
      {
        int64_t fl= 0, ce= 0;
        if (!int_bounds(x, any_real, &fl, &ce))
          return;
        b.emplace_back(fl, ce);
        mn= std::min(mn, fl);
        mx= std::max(mx, ce);
      }
      switch (op) {
      case vp_op::EQ: p.ilo= mn; p.ihi= mx; break;
      case vp_op::GE: p.ilo= b[0].first; break;
      case vp_op::LE: p.ihi= b[0].second; break;
      case vp_op::BETWEEN:
        if (b.size() != 2)
          return;
        p.ilo= b[0].first;
        p.ihi= b[1].second;
        break;
      }
    }
    else if (conv == tf_conv::DOUBLE || conv == tf_conv::FLOAT)
    {
      // FLOAT(M,D)/DOUBLE(M,D) compare with an epsilon on the server
      // (compare_real_fixed), so exact bounds could prune a matching row.
      if (f->field->decimals() != NOT_FIXED_DEC)
        return;
      p.kind= TF_PRED_FLOAT;
      p.flo= -HUGE_VAL;
      p.fhi= HUGE_VAL;
      std::vector<double> v;
      for (Item *x : k)
      {
        double d= 0;
        if (!double_value(x, &d))
          return;
        v.push_back(d);
      }
      const double mn= *std::min_element(v.begin(), v.end());
      const double mx= *std::max_element(v.begin(), v.end());
      switch (op) {
      case vp_op::EQ: p.flo= mn; p.fhi= mx; break;
      case vp_op::GE: p.flo= v[0]; break;
      case vp_op::LE: p.fhi= v[0]; break;
      case vp_op::BETWEEN:
        if (v.size() != 2)
          return;
        p.flo= v[0];
        p.fhi= v[1];
        break;
      }
    }
    else if (conv == tf_conv::STRING && op == vp_op::EQ)
    {
      /*
        Only a byte comparison: the comparison collation is the column's own
        (not just the same charset), binary NO PAD, or binary PAD SPACE in
        charsets whose space is the single byte 0x20. DECIMAL columns are not
        pushed at all.
      */
      CHARSET_INFO *fcs= f->field->charset();
      if (!fcs || !cmp_cs || cmp_cs->number != fcs->number ||
          f->field->real_type() != MYSQL_TYPE_VARCHAR ||
          !(fcs == &my_charset_bin || (fcs->state & MY_CS_BINSORT)))
        return;
      if (fcs == &my_charset_bin || (fcs->state & MY_CS_NOPAD))
        p.pad_space= 0;
      else
      {
        const char *nm= fcs->cs_name.str;
        if (fcs->mbminlen != 1 || !nm ||
            !(strcmp(nm, "utf8mb4") == 0 || strcmp(nm, "utf8mb3") == 0 ||
              strcmp(nm, "latin1") == 0 || strcmp(nm, "ascii") == 0))
          return;
        p.pad_space= 1;
      }
      p.kind= TF_PRED_BYTES;
      for (Item *x : k)
      {
        std::string v;
        bool is_null= false;
        if (!const_string(x, fcs, &v, &is_null))
          return;
        if (!is_null)
          p.values.push_back(std::move(v));
      }
    }
    else
      return;
    out->push_back(std::move(p));
  };

  switch (fn->functype()) {
  case Item_func::EQ_FUNC:
  case Item_func::GE_FUNC:
  case Item_func::GT_FUNC:
  case Item_func::LE_FUNC:
  case Item_func::LT_FUNC:
  {
    if (fn->argument_count() != 2)
      return;
    Item **a= fn->arguments();
    CHARSET_INFO *cs= fn->functype() == Item_func::EQ_FUNC
        ? static_cast<Item_bool_rowready_func2 *>(fn)->compare_collation()
        : nullptr;
    for (int side= 0; side < 2; side++)
      if (const Item_field *f= field_of(a[side], table))
      {
        /* `col op K` as is; `K op col` mirrors the operator */
        vp_op op= vp_op::EQ;
        switch (fn->functype()) {
        case Item_func::GE_FUNC:
        case Item_func::GT_FUNC:
          op= side == 0 ? vp_op::GE : vp_op::LE;
          break;
        case Item_func::LE_FUNC:
        case Item_func::LT_FUNC:
          op= side == 0 ? vp_op::LE : vp_op::GE;
          break;
        default:
          break;
        }
        add(f, op, &a[1 - side], 1, cs);
        return;
      }
    return;
  }
  case Item_func::BETWEEN:
  {
    auto *b= static_cast<Item_func_between *>(fn);
    if (b->negated || fn->argument_count() != 3)
      return;
    if (const Item_field *f= field_of(fn->arguments()[0], table))
      add(f, vp_op::BETWEEN, fn->arguments() + 1, 2, nullptr);
    return;
  }
  case Item_func::IN_FUNC:
  {
    auto *in= static_cast<Item_func_in *>(fn);
    if (in->negated || fn->argument_count() < 2)
      return;
    if (const Item_field *f= field_of(fn->arguments()[0], table))
      add(f, vp_op::EQ, fn->arguments() + 1, fn->argument_count() - 1,
          in->compare_collation());
    return;
  }
  default:
    return;
  }
}

const COND *ha_moosedb::cond_push(const COND *cond)
{
  std::vector<tf_tag_predicate> preds;
  if (share_ && cond)
    collect_tag_predicates(cond, &preds);
  pushed_.push_back(std::move(preds));
  {
    std::vector<tf_val_pred> vp;
    if (share_ && cond && table)
      collect_value_predicates(cond, &vp);
    pushed_val_.push_back(std::move(vp));
  }
  pushed_valid_= false;
  /*
    Time bounds of the WHERE for the unordered scan (rnd_init): every
    top-level conjunct capture_delete_cond() reproduces exactly narrows
    [lo, hi]; the others are ignored. The server still evaluates the whole
    WHERE, so a wider range is only slower, never wrong.
  */
  {
    longlong tlo= LONGLONG_MIN, thi= LONGLONG_MAX;
    if (share_ && table && cond)
    {
      /*
        capture_str_time_ is true only inside this scope (a string constant
        compared as DATETIME is a bound too): a SUPERSET is enough here. The
        direct DELETE below runs without the server re-checking rows and
        needs exactness, so it uses the flag off (see capture_delete_cond).
        Converting a constant may raise a note; the server raises its own.
      */
      Quiet_capture guard(ha_thd(), capture_str_time_, capture_for_scan_);
      auto one= [&](Item *c) {
        std::vector<tf_tag_predicate> scratch;
        longlong l= tlo, h= thi;
        if (capture_delete_cond(c, &scratch, &l, &h))
        {
          tlo= l;
          thi= h;
        }
      };
      Item *it= const_cast<Item *>(cond);
      if (it->type() == Item::COND_ITEM &&
          static_cast<Item_cond *>(it)->functype() == Item_func::COND_AND_FUNC)
      {
        List_iterator_fast<Item> li(*static_cast<Item_cond *>(it)->argument_list());
        while (Item *arg= li++)
          one(arg);
      }
      else
        one(it);
    }
    /* Keep pushed_ and pushed_time_ paired: no early return above this line. */
    pushed_time_.emplace_back(tlo, thi);
  }
  /*
    A DELETE whose whole WHERE is TAG equalities and time bounds can be done by
    the engine alone (direct delete). The server asks for that by calling
    cond_push on the target table of the DELETE and treating a NULL return as
    "the engine takes the whole condition" (sql_delete.cc); it then calls
    direct_delete_rows_init(), which may still refuse, and the server goes on
    with its own evaluation. Anything else (a SELECT, a table that is only
    read by the DELETE, a condition not fully captured) returns the condition:
    the server evaluates all of it.
  */
  ddel_cap_valid_= false;
  THD *thd= ha_thd();
  if (cond && share_ && table && thd && thd_sql_command(thd) == SQLCOM_DELETE &&
      table->reginfo.lock_type >= TL_FIRST_WRITE)
  {
    std::vector<tf_tag_predicate> p;
    longlong lo= LONGLONG_MIN, hi= LONGLONG_MAX;
    if (capture_delete_cond(const_cast<Item *>(cond), &p, &lo, &hi))
    {
      ddel_cap_preds_= std::move(p);
      ddel_cap_lo_= lo;
      ddel_cap_hi_= hi;
      ddel_cap_item_= cond;
      ddel_cap_valid_= true;
      return nullptr;
    }
  }
  /* The server keeps evaluating the full condition: we only prune series. */
  return cond;
}

void ha_moosedb::cond_pop()
{
  if (!pushed_.empty())
    pushed_.pop_back();
  if (!pushed_val_.empty())
    pushed_val_.pop_back();
  if (!pushed_time_.empty())
    pushed_time_.pop_back();
  pushed_valid_= false;
}

int ha_moosedb::reset()
{
  /*
    Statement-end safety net (runs per statement under LOCK TABLES and per
    stored-routine statement). Never commit in the middle of a bulk insert of
    the same statement.
  */
  int error= 0;
  THD *thd= ha_thd();
  if (in_bulk_insert_ && thd && bulk_query_id_ != thd->query_id)
    in_bulk_insert_= false;   /* stale: end_bulk_insert() was never called */
  if (!in_bulk_insert_ && (batch_ || del_))
  {
    error= commit_batch();
    /* ha_reset()'s return value is ignored by the server: report it here */
    if (error)
      print_error(error, MYF(0));
  }
  ddel_cap_valid_= false;
  ddel_cap_item_= nullptr;
  pushed_.clear();
  pushed_val_.clear();
  pushed_time_.clear();
  pushed_valid_= false;
  scan_.reset();
  reset_batch_state();
  snapshots_.clear();
  idx_req_= idx_request();
  idx_ids_.clear();
  scan_snapshot_saved_= false;
  return error;
}

/*
  Series matching every pushed TAG predicate. Returns false when nothing
  was pushed (no series restriction).
*/
bool ha_moosedb::pushed_series(const std::vector<uint64_t> **ids)
{
  *ids= &pushed_ids_;
  if (pushed_valid_)
    return pushed_filter_;
  pushed_valid_= true;
  pushed_ids_.clear();
  std::vector<const tf_tag_predicate *> preds;
  for (const auto &level : pushed_)
    for (const tf_tag_predicate &p : level)
      preds.push_back(&p);
  pushed_filter_= !preds.empty();
  if (!pushed_filter_)
    return false;
  if (!resolve_series(preds, &pushed_ids_))
  {
    pushed_ids_.clear();
    pushed_filter_= false;
    return false;
  }
  return true;
}

/*
  Series satisfying every predicate (sorted ids). false = could not be
  resolved (the caller decides: a pushdown falls back to an unfiltered scan,
  an index lookup cannot).
*/
bool ha_moosedb::resolve_series(
    const std::vector<const tf_tag_predicate *> &preds,
    std::vector<uint64_t> *out)
{
  out->clear();
  if (!share_ || !share_->table)
    return false;
  assert(table);
  if (!table)
    return false;
  /* Character sets of the TAG columns, in TAG order. */
  std::vector<CHARSET_INFO *> tag_cs;
  for (const tf_column &c : share_->layout.columns)
    if (c.conv == tf_conv::TAG)
    {
      if (c.field_index >= table->s->fields || !table->field[c.field_index])
        return false;
      /* Integer TAGs are matched on their decimal text, byte for byte. */
      tag_cs.push_back(is_int_tag_field(table->field[c.field_index])
                         ? &my_charset_bin
                         : table->field[c.field_index]->charset());
    }
  for (const tf_tag_predicate *p : preds)
    if (!p || p->tag_pos < 0 || (size_t) p->tag_pos >= tag_cs.size() || !p->cs)
      return false;

  if (pushed_series_cached(preds, tag_cs, out))
    return true;

  /* Fallback: compare every series (also used past the cache limits). */
  MooseDBSeriesList *list= nullptr;
  if (moosedb_series_list(share_->table.get(), &list) != TF_OK)
  {
    sql_print_warning("MooseDB: TAG lookup unavailable: %s",
                      take_last_error().c_str());
    return false;
  }
  out->clear();
  const bool ok= pushed_series_scan(list, preds, tag_cs, out);
  moosedb_series_list_close(list);
  return ok;
}

namespace {

/* Series value as seen under the predicate's collation (same conversion the
   comparison path always applied). */
struct tag_view
{
  const char *ptr;
  size_t len;
};

tag_view tag_in_collation(const TFValue &v, CHARSET_INFO *from,
                          CHARSET_INFO *to, String *conv)
{
  const TFStr s= v.data.str_val;
  tag_view r{s.ptr ? s.ptr : "", s.len};
  uint errors;
  if (!my_charset_same(from, to) && !conv->copy(r.ptr, r.len, from, to, &errors))
  {
    r.ptr= conv->ptr();
    r.len= conv->length();
  }
  return r;
}

uint64_t tag_hash(CHARSET_INFO *cs, const char *ptr, size_t len)
{
  ulong nr1= 1, nr2= 4;
  cs->hash_sort((const uchar *) ptr, len, &nr1, &nr2);
  return ((uint64_t) nr1 * 0x9E3779B97F4A7C15ULL) ^ (uint64_t) nr2;
}

constexpr size_t TAG_CACHE_MAX_INDEXES= 32;
/* Per-table heap cap of the TAG index; above it the direct comparison is
   used. A group costs its value plus ~96 bytes (string, id vector, hash
   bucket entry); each further series of the group costs one id. */
constexpr size_t TAG_CACHE_MAX_BYTES= 256ULL << 20;
constexpr size_t TAG_CACHE_GROUP_OVERHEAD= 96;

} // namespace

/* Reference implementation: one strnncollsp per series and value. */
bool ha_moosedb::pushed_series_scan(
    MooseDBSeriesList *list, const std::vector<const tf_tag_predicate *> &preds,
    const std::vector<CHARSET_INFO *> &tag_cs, std::vector<uint64_t> *out)
{
  const uint64_t n= moosedb_series_list_len(list);
  String conv;
  for (uint64_t i= 0; i < n; i++)
  {
    uint64_t id= 0;
    TFRow tags{0, nullptr};
    if (moosedb_series_list_get(list, i, &id, &tags) != TF_OK)
      continue;
    bool match= true;
    for (const tf_tag_predicate *p : preds)
    {
      if ((uint) p->tag_pos >= tags.col_count || !tags.values ||
          tags.values[p->tag_pos].is_null)
      {
        match= false;
        break;
      }
      const tag_view v= tag_in_collation(tags.values[p->tag_pos],
                                         tag_cs[p->tag_pos], p->cs, &conv);
      bool any= false;
      for (const std::string &val : p->values)
        if (!p->cs->strnncollsp((const uchar *) v.ptr, v.len,
                                (const uchar *) val.data(), val.size()))
        {
          any= true;
          break;
        }
      if (!any)
      {
        match= false;
        break;
      }
    }
    if (match)
      out->push_back(id);
  }
  std::sort(out->begin(), out->end());
  return true;
}

/*
  Indexed implementation. Per (TAG column, collation) the series are grouped
  by value; the series list is append-only (registration order) except for
  TRUNCATE, so the cache only indexes the new tail. A predicate then costs
  O(values), not O(series x values).

  Equality is exactly strnncollsp's: candidates come from hash_sort buckets
  (the hash the server itself uses for GROUP BY / hash joins, consistent with
  the collation's equality, PAD SPACE / NO PAD and _ci included) and every
  candidate is confirmed with strnncollsp.

  The list's epoch identifies the log behind it: the same epoch guarantees the
  current list extends the indexed one (same series at the same indexes); a
  new epoch (TRUNCATE, table reopened) drops the cache. The version only says
  "something changed" -- it is a process-wide counter, so its deltas must not
  be compared with length deltas.
*/
bool ha_moosedb::pushed_series_cached(
    const std::vector<const tf_tag_predicate *> &preds,
    const std::vector<CHARSET_INFO *> &tag_cs, std::vector<uint64_t> *out)
{
  for (const tf_tag_predicate *p : preds)
    if (!p->cs->coll || !p->cs->coll->hash_sort)
      return false;

  tf_tag_cache &cache= share_->tag_cache;
  std::lock_guard<std::mutex> guard(cache.mutex);

  uint64_t cur= 0;
  if (moosedb_series_version(share_->table.get(), &cur) != TF_OK)
    return false;
  bool missing= false;
  for (const tf_tag_predicate *p : preds)
    if (!cache.indexes.count({p->tag_pos, p->cs}))
      missing= true;
  if (!cache.valid || cache.version != cur || missing)
  {
    MooseDBSeriesList *list= nullptr;
    if (moosedb_series_list(share_->table.get(), &list) != TF_OK)
      return false;
    const uint64_t lv= moosedb_series_list_version(list);
    const uint64_t epoch= moosedb_series_list_epoch(list);
    const uint64_t n= moosedb_series_list_len(list);
    const bool appended= cache.valid && epoch != 0 && epoch == cache.epoch &&
                         n >= cache.len;
    if (!appended)
    {
      cache.indexes.clear();
      cache.bytes= 0;
    }
    for (const tf_tag_predicate *p : preds)
    {
      const std::pair<int, const CHARSET_INFO *> key{p->tag_pos, p->cs};
      if (!cache.indexes.count(key) &&
          cache.indexes.size() >= TAG_CACHE_MAX_INDEXES)
      {
        /* Indexes stay consistent with each other only if all advance. */
        cache.valid= false;
        moosedb_series_list_close(list);
        return false;
      }
      cache.indexes[key];
    }
    /* Every index (old ones: the new tail; new ones: everything). */
    String conv;
    for (auto &kv : cache.indexes)
    {
      tf_tag_index &ix= kv.second;
      CHARSET_INFO *cs= const_cast<CHARSET_INFO *>(kv.first.second);
      const int pos= kv.first.first;
      for (uint64_t i= ix.built_len; i < n; i++)
      {
        uint64_t id= 0;
        TFRow tags{0, nullptr};
        if (moosedb_series_list_get(list, i, &id, &tags) != TF_OK)
        {
          /* Skipping it would lose the series until the next epoch. */
          take_last_error();
          cache.indexes.clear();
          cache.bytes= 0;
          cache.valid= false;
          moosedb_series_list_close(list);
          return false;
        }
        if ((uint) pos >= tags.col_count || !tags.values ||
            tags.values[pos].is_null)
          continue;
        const tag_view v= tag_in_collation(tags.values[pos], tag_cs[pos], cs,
                                           &conv);
        std::vector<uint32_t> &bucket= ix.buckets[tag_hash(cs, v.ptr, v.len)];
        bool found= false;
        for (uint32_t g : bucket)
        {
          if (g >= ix.groups.size())
            continue;
          tf_tag_group &grp= ix.groups[g];
          if (!cs->strnncollsp((const uchar *) grp.value.data(),
                               grp.value.size(), (const uchar *) v.ptr, v.len))
          {
            grp.ids.push_back(id);
            cache.bytes+= sizeof(uint64_t);
            found= true;
            break;
          }
        }
        if (!found)
        {
          bucket.push_back((uint32_t) ix.groups.size());
          ix.groups.push_back({std::string(v.ptr, v.len), {id}});
          cache.bytes+= TAG_CACHE_GROUP_OVERHEAD + v.len;
        }
        if (cache.bytes > TAG_CACHE_MAX_BYTES)
        {
          /* Too many distinct series to index: use the direct comparison. */
          cache.indexes.clear();
          cache.bytes= 0;
          cache.valid= false;
          moosedb_series_list_close(list);
          return false;
        }
      }
      ix.built_len= n;
    }
    cache.valid= true;
    cache.version= lv;
    cache.epoch= epoch;
    cache.len= n;
    moosedb_series_list_close(list);
  }

  bool first= true;
  std::vector<uint64_t> result;
  for (const tf_tag_predicate *p : preds)
  {
    const auto it= cache.indexes.find({p->tag_pos, (const CHARSET_INFO *) p->cs});
    if (it == cache.indexes.end())
      return false;
    const tf_tag_index &ix= it->second;
    std::vector<uint32_t> hit;
    for (const std::string &val : p->values)
    {
      const auto b= ix.buckets.find(tag_hash(p->cs, val.data(), val.size()));
      if (b == ix.buckets.end())
        continue;
      for (uint32_t g : b->second)
        if (g < ix.groups.size() &&
            !p->cs->strnncollsp((const uchar *) ix.groups[g].value.data(),
                                ix.groups[g].value.size(),
                                (const uchar *) val.data(), val.size()))
          hit.push_back(g);
    }
    std::sort(hit.begin(), hit.end());
    hit.erase(std::unique(hit.begin(), hit.end()), hit.end());
    std::vector<uint64_t> ids;
    for (uint32_t g : hit)
      ids.insert(ids.end(), ix.groups[g].ids.begin(), ix.groups[g].ids.end());
    std::sort(ids.begin(), ids.end());
    if (first)
    {
      result.swap(ids);
      first= false;
    }
    else
    {
      std::vector<uint64_t> both;
      std::set_intersection(result.begin(), result.end(), ids.begin(),
                            ids.end(), std::back_inserter(both));
      result.swap(both);
    }
    if (result.empty())
      break;
  }
  *out= std::move(result);
  return true;
}

/* ── Direct DELETE ────────────────────────────────────────────────────── */

/*
  Does `item` -- one conjunct of a WHERE -- say exactly "TAG column equals one
  of some constants" or "the time column compares with a constant"? Then it is
  absorbed into `preds` (series) or [lo, hi] (time) and true is returned; false
  means the condition has something the engine cannot reproduce exactly (the
  server evaluates the WHERE row by row then). A direct delete runs without the
  server re-checking rows, so unlike the scan pushdown nothing may be
  approximated here.
*/
bool ha_moosedb::capture_delete_cond(Item *item,
                                     std::vector<tf_tag_predicate> *preds,
                                     longlong *lo, longlong *hi)
{
  if (!item || !share_ || !table)
    return false;
  if (item->type() == Item::COND_ITEM)
  {
    auto *c= static_cast<Item_cond *>(item);
    if (c->functype() != Item_func::COND_AND_FUNC)
      return false;
    List_iterator_fast<Item> it(*c->argument_list());
    while (Item *arg= it++)
      if (!capture_delete_cond(arg, preds, lo, hi))
        return false;
    return true;
  }
  if (item->const_item())
  {
    if (item->is_expensive())
      return false;
    const bool v= item->val_bool();
    return !item->null_value && v;
  }
  if (item->type() != Item::FUNC_ITEM)
    return false;
  auto *fn= static_cast<Item_func *>(item);
  const tf_layout &l= share_->layout;
  if (l.ts_column >= l.columns.size())
    return false;
  const tf_column &tc= l.columns[l.ts_column];

  /* Time bound: column OP constant, constant on either side. */
  /* The comparison itself is DATETIME (the constant may be a string): GROUP BY pushdown only. */
  auto time_cmp= [&](Item_func *f) -> bool {
    if (!capture_str_time_)
      return false;
    switch (f->functype()) {
    case Item_func::LT_FUNC: case Item_func::LE_FUNC: case Item_func::GT_FUNC:
    case Item_func::GE_FUNC: case Item_func::EQ_FUNC:
      return static_cast<Item_bool_func2 *>(f)->compare_type_handler()->cmp_type() == TIME_RESULT;
    case Item_func::BETWEEN:
      /* (datetime, string, string) compares as datetime */
      return f->argument_count() == 3 && f->arguments()[0]->cmp_type() == TIME_RESULT &&
             f->arguments()[1]->cmp_type() == STRING_RESULT &&
             f->arguments()[2]->cmp_type() == STRING_RESULT;
    default:
      return false;
    }
  };
  auto time_bound= [&](Item *fld, Item *cst, Item_func::Functype op, bool cmp_time) -> bool {
    const Item_field *f= field_of(fld, table);
    if (!f || f->field->field_index != tc.field_index || !cst->const_item() ||
        cst->is_expensive())
      return false;
    __int128 key;                    /* the constant, in micros of the axis */
    __int128 step;                   /* smallest distance between stored values */
    if (tc.conv == tf_conv::INT_SECS)
    {
      if (cst->cmp_type() != INT_RESULT)
        return false;
      const longlong v= cst->val_int();
      if (cst->null_value)
        return false;
      key= (cst->unsigned_flag && v < 0) ? (__int128) (ulonglong) v : (__int128) v;
      key*= MICROS_PER_SEC;
      step= MICROS_PER_SEC;
    }
    else if (tc.conv == tf_conv::DATETIME)
    {
      THD *thd= ha_thd();
      MYSQL_TIME lt;
      /* For rnd_init only clean string literals are converted (see scan_literal_ok). */
      if (capture_for_scan_ && !scan_literal_ok(cst))
        return false;
      if (!thd || (cst->cmp_type() != TIME_RESULT && !cmp_time) ||
          cst->get_date(thd, &lt, Datetime::Options_cmp(thd)) /* the comparator's own flags and rounding */ || cst->null_value ||
          lt.month == 0 || lt.day == 0 || lt.neg ||
          (lt.time_type != MYSQL_TIMESTAMP_DATETIME && lt.time_type != MYSQL_TIMESTAMP_DATE))
        return false;
      /* ALLOW_INVALID_DATES lets '2026-02-30' through the conversion; the
         server orders it by its packed value, which time_to_micros() does not
         reproduce: leave such constants to the server. */
      {
        const uint dim= lt.month == 2
            ? 28u + (((lt.year % 4 == 0 && lt.year % 100 != 0) || lt.year % 400 == 0) ? 1u : 0u)
            : ((lt.month == 4 || lt.month == 6 || lt.month == 9 || lt.month == 11) ? 30u : 31u);
        if (lt.month > 12 || lt.day > dim)
          return false;
      }
      key= time_to_micros(lt);
      step= 1;
    }
    else
      return false;                  /* TIMESTAMP axis: left to the server */
    const __int128 mn= LONGLONG_MIN, mx= LONGLONG_MAX;
    auto clamp= [&](__int128 v) { return (longlong) (v < mn ? mn : v > mx ? mx : v); };
    switch (op) {
    case Item_func::LT_FUNC: *hi= std::min(*hi, clamp(key - step)); break;
    case Item_func::LE_FUNC: *hi= std::min(*hi, clamp(key)); break;
    case Item_func::GT_FUNC: *lo= std::max(*lo, clamp(key + step)); break;
    case Item_func::GE_FUNC: *lo= std::max(*lo, clamp(key)); break;
    case Item_func::EQ_FUNC:
      *lo= std::max(*lo, clamp(key));
      *hi= std::min(*hi, clamp(key));
      break;
    default: return false;
    }
    return true;
  };
  auto flip= [](Item_func::Functype op) {
    switch (op) {
    case Item_func::LT_FUNC: return Item_func::GT_FUNC;
    case Item_func::LE_FUNC: return Item_func::GE_FUNC;
    case Item_func::GT_FUNC: return Item_func::LT_FUNC;
    case Item_func::GE_FUNC: return Item_func::LE_FUNC;
    default: return op;
    }
  };

  switch (fn->functype()) {
  case Item_func::LT_FUNC:
  case Item_func::LE_FUNC:
  case Item_func::GT_FUNC:
  case Item_func::GE_FUNC:
  case Item_func::EQ_FUNC:
  {
    if (fn->argument_count() != 2)
      return false;
    Item **a= fn->arguments();
    if (fn->functype() == Item_func::EQ_FUNC)
    {
      /* A TAG equality? (collect_tag_predicates pushes only exact ones.) */
      const size_t before= preds->size();
      collect_tag_predicates(item, preds);
      if (preds->size() > before)
        return true;
    }
    const bool ct= time_cmp(fn);
    return time_bound(a[0], a[1], fn->functype(), ct) ||
           time_bound(a[1], a[0], flip(fn->functype()), ct);
  }
  case Item_func::IN_FUNC:
  {
    const size_t before= preds->size();
    collect_tag_predicates(item, preds);
    return preds->size() > before;
  }
  case Item_func::MULT_EQUAL_FUNC:
  {
    auto *eq= static_cast<Item_equal *>(fn);
    {
      Item_equal_fields_iterator chk(*eq);
      while (Item *m= chk++)
      {
        const Item_field *f= field_of(m, table);
        if (!f || share_->layout.tag_position(f->field->field_index) < 0)
          return false;
      }
    }
    const size_t before= preds->size();
    collect_tag_predicates(item, preds);
    return preds->size() > before;
  }
  case Item_func::BETWEEN:
  {
    auto *b= static_cast<Item_func_between *>(fn);
    if (b->negated || fn->argument_count() != 3)
      return false;
    Item **a= fn->arguments();
    longlong l0= *lo, h0= *hi;
    const bool ct= time_cmp(fn);
    if (time_bound(a[0], a[1], Item_func::GE_FUNC, ct) &&
        time_bound(a[0], a[2], Item_func::LE_FUNC, ct))
      return true;
    *lo= l0;
    *hi= h0;
    return false;
  }
  default:
    return false;
  }
}

/*
  A DELETE the engine can do as one range: only TAG equalities / IN lists and
  bounds on the time column in the WHERE, no ORDER BY, and either no LIMIT or
  a LIMIT the range does not reach. Everything else (any other WHERE, a LIMIT
  that would cut the range, binary logging in ROW format, triggers, RETURNING)
  makes the server delete row by row through delete_row(), which is always
  correct, just slower.
*/
int ha_moosedb::direct_delete_rows_init()
{
  DBUG_ENTER("ha_moosedb::direct_delete_rows_init");
  ddel_ready_= false;
  THD *thd= ha_thd();
  if (!thd || !table || !share_ || !share_->table || !thd->lex)
    DBUG_RETURN(HA_ERR_WRONG_COMMAND);
  SELECT_LEX *sl= thd->lex->first_select_lex();
  if (!sl || sl->order_list.elements)
    DBUG_RETURN(HA_ERR_WRONG_COMMAND);
  std::vector<tf_tag_predicate> preds;
  longlong lo= LONGLONG_MIN, hi= LONGLONG_MAX;
  /*
    Fail closed: with a WHERE in the statement the plan is the one cond_push
    captured, and only if it is the very condition the server handed over.
    Without a captured plan a WHERE never becomes a delete of the whole table.
  */
  if (sl->where)
  {
    if (!ddel_cap_valid_ || !pushed_cond || pushed_cond != ddel_cap_item_)
      DBUG_RETURN(HA_ERR_WRONG_COMMAND);
    preds= ddel_cap_preds_;
    lo= ddel_cap_lo_;
    hi= ddel_cap_hi_;
  }
  else if (pushed_cond)
    DBUG_RETURN(HA_ERR_WRONG_COMMAND);
  ddel_all_series_= preds.empty();
  ddel_ids_.clear();
  if (!preds.empty())
  {
    std::vector<const tf_tag_predicate *> ptrs;
    for (const tf_tag_predicate &p : preds)
      ptrs.push_back(&p);
    if (!resolve_series(ptrs, &ddel_ids_))
      DBUG_RETURN(HA_ERR_WRONG_COMMAND);
    /* The log entry names every series by its tags (8 MiB at most): a huge
       selection goes row by row instead of failing after the server said OK. */
    if (ddel_ids_.size() > 20000)
      DBUG_RETURN(HA_ERR_WRONG_COMMAND);
    {
      /* bytes the log entry needs: every series by its tag values, at their widest */
      size_t per_series= 8;
      for (const tf_column &c : share_->layout.columns)
        if (c.conv == tf_conv::TAG && c.field_index < table->s->fields &&
            table->field[c.field_index])
          per_series+= table->field[c.field_index]->pack_length() + 6;
      if ((double) ddel_ids_.size() * (double) per_series > 4.0 * 1024 * 1024)
        DBUG_RETURN(HA_ERR_WRONG_COMMAND);
    }
  }
  ddel_lo_= lo;
  ddel_hi_= hi;
  ddel_limit_= thd->lex->unit.lim.get_select_limit();
  if (ddel_limit_ != HA_POS_ERROR)
  {
    /* With a LIMIT only a range that stays under it can be deleted at once. */
    uint64_t n= 0;
    if (map_status(moosedb_count_range(share_->table.get(),
                                       ddel_all_series_ ? nullptr : ddel_ids_.data(),
                                       ddel_all_series_ ? -1 : (int64_t) ddel_ids_.size(),
                                       lo, hi, &n)) ||
        n > ddel_limit_)
      DBUG_RETURN(HA_ERR_WRONG_COMMAND);
  }
  ddel_ready_= true;
  DBUG_RETURN(0);
}

int ha_moosedb::direct_delete_rows(ha_rows *deleted)
{
  DBUG_ENTER("ha_moosedb::direct_delete_rows");
  *deleted= 0;
  if (!ddel_ready_ || !share_ || !share_->table)
  {
    print_error(HA_ERR_INTERNAL_ERROR, MYF(0));
    DBUG_RETURN(HA_ERR_INTERNAL_ERROR);
  }
  ddel_ready_= false;
  scan_.reset();
  uint64_t rows= 0;
  bool exceeds= false;
  int error= map_status(moosedb_delete_range(
      share_->table.get(), ddel_all_series_ ? nullptr : ddel_ids_.data(),
      ddel_all_series_ ? -1 : (int64_t) ddel_ids_.size(), ddel_lo_, ddel_hi_,
      ddel_limit_ == HA_POS_ERROR ? -1 : (int64_t) std::min<ha_rows>(ddel_limit_, (ha_rows) INT64_MAX),
      srv_wal_sync_mode == WAL_SYNC_FSYNC, &rows, &exceeds));
  /*
    sql_delete.cc does not report an error of ha_direct_delete_rows(): it only
    returns failure, and the client would get a plain OK with 0 rows. The
    handler has to print it itself (print_error) -- fail closed.
  */
  if (!error && exceeds)
  {
    last_error_= "more rows match the DELETE than its LIMIT allows: rows were "
                 "inserted while it ran; run it again";
    error= HA_ERR_LOCK_WAIT_TIMEOUT;
  }
  if (error)
  {
    print_error(error, MYF(0));
    DBUG_RETURN(error);
  }
  *deleted= (ha_rows) rows;
  DBUG_RETURN(0);
}

/* ── Scans ────────────────────────────────────────────────────────────── */

void ha_moosedb::reset_batch_state()
{
  bat_= TFBatch{0, 0, nullptr, nullptr};
  bat_rows_= 0;
  bat_pos_= 0;
  bat_dir_= 0;
  bat_hint_= 16;
  bat_skip_= 0;
  bat_ended_= false;
  bitmaps_dirty_= false;
  degraded_= false;
  degraded_snap_.reset();
  active_cols_.clear();
  tag_buf_= nullptr;
}

/*
  Decides which columns the scan decodes (proj_mask_ / proj_all_). The mask
  follows table->read_set; everything else the server might read without
  declaring it asks for all columns:
   - statements other than SELECT / INSERT..SELECT / REPLACE..SELECT (UPDATE,
     DELETE, CREATE..SELECT, ALTER, CHECK, ...), and no THD or no read_set;
   - the table has virtual columns (their inputs are computed by the server).
  Index scans always include the timestamp: the server compares the row's
  key against end_range (handler::compare_key) reading it from the record.
  If read_set grows after the scan opened (column_bitmaps_signal), rows are
  fetched whole instead (degraded_), see read_from_scan().
*/
void ha_moosedb::build_projection(bool index_scan)
{
  proj_all_= true;
  proj_mask_.clear();
  if (!table || !share_ || !table->read_set || table->vfield)
    return;
  THD *thd= ha_thd();
  /* the replication applier reads rows to compare them with a before-image: whole rows */
  if (!thd || thd->slave_thread)
    return;
  switch (thd_sql_command(thd)) {
  case SQLCOM_SELECT:
  case SQLCOM_INSERT_SELECT:
  case SQLCOM_REPLACE_SELECT:
    break;
  default:
    return;
  }
  const tf_layout &l= share_->layout;
  std::vector<uint8_t> mask(l.columns.size(), 0);
  bool any_off= false;
  /*
    The server compares a row's key parts with end_range / the ref key
    (compare_key, key_cmp_if_same) reading them from the record, so every
    part of the index being scanned must be decoded, requested or not.
  */
  std::vector<uint8_t> key_field(table->s->fields, 0);
  if (index_scan && active_index < table->s->keys && table->key_info)
  {
    const KEY &ki= table->key_info[active_index];
    for (uint p= 0; ki.key_part && p < ki.user_defined_key_parts; p++)
      if (ki.key_part[p].field && ki.key_part[p].field->field_index < key_field.size())
        key_field[ki.key_part[p].field->field_index]= 1;
  }
  for (size_t i= 0; i < l.columns.size(); i++)
  {
    const uint fi= l.columns[i].field_index;
    if (fi >= table->s->fields)
      return;
    if (bitmap_is_set(table->read_set, fi) ||
        (index_scan && i == l.ts_column) || key_field[fi])
      mask[i]= 1;
    else
      any_off= true;
  }
  if (!any_off || mask.empty())
    return;
  proj_mask_= std::move(mask);
  proj_all_= false;
}

/* Switches to whole-row reads if read_set asks for an undecoded column. */
void ha_moosedb::recheck_projection()
{
  bitmaps_dirty_= false;
  if (proj_all_ || degraded_)
    return;
  if (!table || !table->read_set || !share_)
  {
    degraded_= true;
    return;
  }
  const tf_layout &l= share_->layout;
  for (size_t i= 0; i < l.columns.size() && i < proj_mask_.size(); i++)
    if (!proj_mask_[i] && l.columns[i].field_index < table->s->fields &&
        bitmap_is_set(table->read_set, l.columns[i].field_index))
    {
      degraded_= true;
      if (bat_pos_ > 0 && !warned_degraded_)
      {
        warned_degraded_= true;
        sql_print_warning("MooseDB: read_set grew after rows were served from "
                          "a projected scan of %s; falling back to whole rows",
                          share_ ? share_->display.c_str() : "?");
      }
      return;
    }
}

void ha_moosedb::column_bitmaps_signal(bool mark_for_update)
{
  bitmaps_dirty_= true;
  handler::column_bitmaps_signal(mark_for_update);
}

int ha_moosedb::open_scan(longlong lo, longlong hi, bool sorted,
                          bool index_scan)
{
  scan_.reset();
  scan_snapshot_saved_= false;
  reset_batch_state();
  if (!share_ || !share_->table)
    return HA_ERR_INTERNAL_ERROR;
  build_projection(index_scan);
  if (index_scan && idx_req_.active)
  {
    /*
      Composite-index read: the series set is part of the *meaning* of the
      read (the key's tags), so unlike a pushdown it can never be dropped --
      if the series version keeps moving there is no unfiltered fallback.
      Same protocol as below: resolve the ids, open the scan (which takes the
      snapshot), and redo both if a series appeared in between.
    */
    for (int attempt= 0; attempt < 64; attempt++)
    {
      uint64_t v0= 0, v1= 0;
      if (moosedb_series_version(share_->table.get(), &v0) != TF_OK)
      {
        last_error_= take_last_error();
        return HA_ERR_INTERNAL_ERROR;
      }
      const std::vector<uint64_t> *ids= nullptr;
      if (!series_for_index(&ids) || !ids)
      {
        last_error_= "cannot resolve the TAG values of the index key";
        return HA_ERR_INTERNAL_ERROR;
      }
      MooseDBScan *s= nullptr;
      if (int error= map_status(moosedb_scan_open_projected(
              share_->table.get(), lo, hi, ids->data(),
              (int64_t) ids->size(), sorted,
              proj_all_ ? nullptr : proj_mask_.data(),
              proj_all_ ? 0 : (uint32_t) proj_mask_.size(), &s)))
        return error;
      tf_scan_ptr opened(s);
      if (moosedb_series_version(share_->table.get(), &v1) == TF_OK && v1 == v0)
      {
        scan_= std::move(opened);
        return 0;
      }
      take_last_error();
    }
    last_error_= "the set of series kept changing while the index was read";
    return HA_ERR_LOCK_WAIT_TIMEOUT;
  }
  /*
    The ids of a TAG filter and the scan are two separate calls: a statement
    that commits between them with a brand-new series would be seen only
    partially. The series version read before resolving the ids must still
    be current once the scan (which snapshots the data) is open; otherwise
    resolve again and reopen. A false alarm only costs a retry.
  */
  /*
    If the version cannot be read or keeps moving (16 tries), fall back to an
    unfiltered scan: the server evaluates the whole WHERE anyway, so that is
    always correct -- only slower -- and never partially visible.
  */
  int error= 0;
  MooseDBScan *s= nullptr;
  bool unfiltered= false;
  /*
    Value predicates (zone maps / Blooms) are a hint: the server re-evaluates
    the whole WHERE. The buffers behind the TFColPred array live in `bufs`
    until the scan is open.
  */
  struct pred_bufs
  {
    std::vector<uint64_t> starts;
    std::vector<uint32_t> lens;
    std::vector<uint8_t> data;
  };
  std::vector<pred_bufs> bufs;
  std::vector<TFColPred> cp;
  {
    size_t total= 0;
    for (const auto &level : pushed_val_)
      total+= level.size();
    bufs.resize(total);
    cp.reserve(total);
    size_t bi= 0;
    for (const auto &level : pushed_val_)
      for (const tf_val_pred &p : level)
      {
        if (bi >= bufs.size())
          break;
        pred_bufs &b= bufs[bi++];
        TFColPred c{};
        c.col= p.col;
        c.kind= p.kind;
        c.pad_space= p.pad_space;
        c.int_lo= p.ilo;
        c.int_hi= p.ihi;
        c.float_lo= p.flo;
        c.float_hi= p.fhi;
        if (p.kind == TF_PRED_BYTES)
        {
          for (const std::string &v : p.values)
          {
            b.starts.push_back((uint64_t) b.data.size());
            b.lens.push_back((uint32_t) v.size());
            b.data.insert(b.data.end(), v.begin(), v.end());
          }
          c.value_starts= b.starts.data();
          c.value_lens= b.lens.data();
          c.value_count= (uint64_t) b.starts.size();
          c.value_data= b.data.data();
          c.value_data_len= (uint64_t) b.data.size();
        }
        cp.push_back(c);
      }
  }
  for (int attempt= 0; attempt <= 16; attempt++)
  {
    uint64_t v0= 0;
    if (attempt == 16 ||
        (!pushed_.empty() &&
         moosedb_series_version(share_->table.get(), &v0) != TF_OK))
    {
      take_last_error();
      unfiltered= true;
    }
    if (pushed_valid_ && pushed_version_ != v0)
      pushed_valid_= false;
    const std::vector<uint64_t> *ids= nullptr;
    const bool filtered= !unfiltered && pushed_series(&ids);
    if (filtered)
      pushed_version_= v0;
    s= nullptr;
    if (cp.empty())
      error= map_status(moosedb_scan_open_projected(
          share_->table.get(), lo, hi, filtered ? ids->data() : nullptr,
          filtered ? (int64_t) ids->size() : -1, sorted,
          proj_all_ ? nullptr : proj_mask_.data(),
          proj_all_ ? 0 : (uint32_t) proj_mask_.size(), &s));
    else
      error= map_status(moosedb_scan_open_pruned(
          share_->table.get(), lo, hi, filtered ? ids->data() : nullptr,
          filtered ? (int64_t) ids->size() : -1, sorted,
          proj_all_ ? nullptr : proj_mask_.data(),
          proj_all_ ? 0 : (uint32_t) proj_mask_.size(), cp.data(),
          (uint32_t) cp.size(), &s));
    if (error || !filtered)
      break;
    uint64_t v1= 0;
    const bool v1_ok=
        moosedb_series_version(share_->table.get(), &v1) == TF_OK;
    if (!v1_ok)
    {
      take_last_error();
      unfiltered= true;
    }
    else if (v1 == v0)
      break;
    moosedb_scan_close(s);
    s= nullptr;
    pushed_valid_= false;
    if (unfiltered)
      attempt= 15;   /* next iteration opens the unfiltered scan */
  }
  scan_.reset(s);
  return error;
}

int ha_moosedb::rnd_init(bool scan)
{
  DBUG_ENTER("ha_moosedb::rnd_init");
  /* scan == false: only rnd_pos() follows; positions resolve through the
     snapshots kept by position(). */
  if (!scan)
    DBUG_RETURN(0);
  /* Unordered anyway: bound the scan by the time range of the pushed WHERE
     (prunes chunks and rows, see cond_push). */
  longlong lo= LONGLONG_MIN, hi= LONGLONG_MAX;
  for (const auto &t : pushed_time_)
  {
    lo= std::max(lo, t.first);
    hi= std::min(hi, t.second);
  }
  DBUG_RETURN(open_scan(lo, hi, false, false));
}

/*
  Reads the next batch (returns 0), or -1 at the end of the scan. After a
  direction switch `bat_skip_` rows are dropped first: the core cursor sits
  on the last row of the previous batch, so the rows between it and the row
  last served (read ahead, already decoded) would otherwise be repeated.
*/
int ha_moosedb::refill_batch(bool backward)
{
  if (!scan_ || !share_)
    return HA_ERR_INTERNAL_ERROR;
  const tf_layout &l= share_->layout;
  for (;;)
  {
    const uint32_t want= std::min<uint32_t>(bat_hint_ + bat_skip_, 65536);
    TFBatch b{0, 0, nullptr, nullptr};
    bool end= false;
    bat_rows_= 0;
    bat_pos_= 0;
    active_cols_.clear();
    TFStatus st= backward
        ? moosedb_scan_prev_batch(scan_.get(), want, &b, &end)
        : moosedb_scan_next_batch(scan_.get(), want, &b, &end);
    if (st != TF_OK)
    {
      bat_dir_= 0;
      bat_skip_= 0;
      return map_status(st);
    }
    if (b.row_count == 0)
    {
      bat_skip_= 0;
      bat_ended_= true;
      return -1;
    }
    if (!b.columns || !b.positions || b.col_count != l.columns.size())
    {
      bat_dir_= 0;
      bat_skip_= 0;
      last_error_= "batch shape does not match the table definition";
      return HA_ERR_MOOSEDB_BASE + TF_ERR_CORRUPT;
    }
    /* Validate the batch against the layout and the projection. */
    for (size_t i= 0; i < l.columns.size(); i++)
    {
      const TFColumnBatch &cb= b.columns[i];
      const bool wanted= proj_all_ || (i < proj_mask_.size() && proj_mask_[i]);
      if (!cb.present)
      {
        if (wanted)
          goto bad_batch;
        continue;
      }
      if (cb.kind != (uint8_t) l.columns[i].type)
        goto bad_batch;
      switch (l.columns[i].conv) {
      case tf_conv::DECIMAL:
      case tf_conv::STRING:
      case tf_conv::TAG:
        if (!cb.starts || !cb.lens || (!cb.data && cb.data_len))
          goto bad_batch;
        for (uint32_t r= 0; r < b.row_count; r++)
          if ((uint64_t) cb.starts[r] + (uint64_t) cb.lens[r] > cb.data_len)
            goto bad_batch;
        break;
      default:
        if (!cb.values)
          goto bad_batch;
        break;
      }
    }
    goto batch_ok;
  bad_batch:
    bat_dir_= 0;
    bat_skip_= 0;
    last_error_= "batch does not match the table definition or projection";
    return HA_ERR_MOOSEDB_BASE + TF_ERR_CORRUPT;
  batch_ok:
    uint32_t take= 0;
    if (bat_skip_)
    {
      take= std::min(bat_skip_, b.row_count);
      bat_skip_-= take;
      if (take == b.row_count)
        continue;               /* still dropping: read the next batch */
    }
    bat_ended_= false;
    bat_= b;
    bat_rows_= b.row_count;
    bat_pos_= take;
    break;
  }
  if (bat_hint_ < 1024)
    bat_hint_= std::min<uint32_t>(bat_hint_ * 2, 1024);

  /* Per-batch column plan; the TAG store-skip memo is per batch (starts). */
  const size_t n= l.columns.size();
  tag_last_ok_.assign(n, 0);
  tag_last_start_.assign(n, 0);
  tag_last_len_.assign(n, 0);
  tag_snap_.resize(n);
  const size_t nb= table ? table->s->null_bytes : 0;
  // The unused bits of the null bytes keep their default value (the replication applier
  // compares whole null bytes with the before-image of the master).
  null_template_.assign(nb, 0);
  if (nb && table->s->default_values)
    memcpy(null_template_.data(), table->s->default_values, nb);
  for (size_t i= 0; i < n; i++)
  {
    if (bat_.columns[i].present)
    {
      active_cols_.push_back((uint32_t) i);
      continue;
    }
    /* unrequested: make a nullable field read NULL */
    const uint fi= l.columns[i].field_index;
    if (table && fi < table->s->fields && table->field[fi])
    {
      const Field *f= table->field[fi];
      if (f->null_ptr && table->record[0])
      {
        const my_ptrdiff_t off= f->null_ptr - table->record[0];
        if (off >= 0 && (size_t) off < nb)
          null_template_[(size_t) off]|= f->null_bit;
      }
    }
  }
  return 0;
}

int ha_moosedb::read_from_scan(uchar *buf, bool backward, int end_error)
{
  if (!scan_ || !buf || !table)
    return HA_ERR_INTERNAL_ERROR;
  const int dir= backward ? 2 : 1;
  if (bat_dir_ != 0 && bat_dir_ != dir)
  {
    /* Rows read ahead but not served: n - bat_pos_ (0 once the scan ended). */
    bat_skip_= bat_rows_ > bat_pos_ ? bat_rows_ - bat_pos_ : 0;
    bat_rows_= 0;
    bat_pos_= 0;
    if (bat_ended_)
    {
      /*
        The core cursor is not reliable after an empty (EOF/BOF) batch:
        reposition explicitly. Forward EOF -> backward starts from the end;
        backward BOF -> forward uses moosedb_scan_seek_start (same snapshot).
      */
      bat_skip_= 0;
      if (int error= map_status(backward ? moosedb_scan_seek_end(scan_.get())
                                         : moosedb_scan_seek_start(scan_.get())))
        return error;
      bat_ended_= false;
    }
  }
  else if (bat_ended_ && bat_dir_ == dir)
    return end_error;   /* the core cursor is not usable after an empty batch */
  bat_dir_= dir;
  if (bitmaps_dirty_)
    recheck_projection();
  if (bat_pos_ >= bat_rows_)
  {
    const int r= refill_batch(backward);
    if (r < 0)
      return end_error;
    if (r)
      return r;
  }
  if (bat_pos_ >= bat_rows_)
    return HA_ERR_INTERNAL_ERROR;
  const uint32_t row= bat_pos_++;
  if (!degraded_)
    return fill_from_batch(buf, row);

  /* read_set outgrew the projection: serve whole rows by position */
  if (!degraded_snap_)
  {
    MooseDBSnapshot *snap= nullptr;
    if (int error= map_status(moosedb_scan_snapshot(scan_.get(), &snap)))
      return error;
    degraded_snap_.reset(snap);
  }
  TFRow full{0, nullptr};
  if (int error= map_status(moosedb_snapshot_fetch(
          degraded_snap_.get(), bat_.positions + (size_t) row * TF_POSITION_LEN,
          &full)))
    return error;
  return fill_record(buf, full);
}

int ha_moosedb::rnd_next(uchar *buf)
{
  DBUG_ENTER("ha_moosedb::rnd_next");
  DBUG_RETURN(read_from_scan(buf, false, HA_ERR_END_OF_FILE));
}

int ha_moosedb::rnd_end()
{
  DBUG_ENTER("ha_moosedb::rnd_end");
  scan_.reset();
  reset_batch_state();
  DBUG_RETURN(0);
}

void ha_moosedb::position(const uchar *)
{
  DBUG_ENTER("ha_moosedb::position");
  if (scan_ && bat_dir_ != 0 && bat_pos_ > 0 && bat_pos_ <= bat_rows_ &&
      bat_.positions)
    memcpy(ref, bat_.positions + (size_t) (bat_pos_ - 1) * TF_POSITION_LEN,
           TF_POSITION_LEN);
  else if (!scan_ || moosedb_scan_position(scan_.get(), ref) != TF_OK)
  {
    take_last_error();
    memset(ref, 0xff, ref_length);
    DBUG_VOID_RETURN;
  }
  if (!scan_snapshot_saved_)
  {
    /* Keep the scan's snapshot so rnd_pos() can resolve this position even
       after the scan is closed and the table has changed. Consecutive
       scans over unchanged data share one snapshot. */
    MooseDBSnapshot *snap= nullptr;
    if (moosedb_scan_snapshot(scan_.get(), &snap) == TF_OK)
    {
      tf_snapshot_ptr p(snap);
      if (snapshots_.empty() ||
          moosedb_snapshot_version(snapshots_.back().get()) !=
          moosedb_snapshot_version(snap))
        snapshots_.push_back(std::move(p));
    }
    else
      take_last_error();
    scan_snapshot_saved_= true;
  }
  DBUG_VOID_RETURN;
}

int ha_moosedb::rnd_pos(uchar *buf, uchar *pos)
{
  DBUG_ENTER("ha_moosedb::rnd_pos");
  if (!share_ || !share_->table || !pos)
    DBUG_RETURN(HA_ERR_INTERNAL_ERROR);
  TFRow row{0, nullptr};
  for (auto it= snapshots_.rbegin(); it != snapshots_.rend(); ++it)
  {
    TFStatus st= moosedb_snapshot_fetch(it->get(), pos, &row);
    if (st == TF_OK)
      DBUG_RETURN(fill_record(buf, row));
    if (st != TF_ERR_NOT_FOUND)
      DBUG_RETURN(map_status(st));
    take_last_error();
  }
  /* Position produced elsewhere (e.g. by a cloned handler): resolve it
     against the current contents. */
  MooseDBScan *s= nullptr;
  if (int error= map_status(moosedb_scan_open_filtered(
          share_->table.get(), 1, 0, nullptr, -1, false, &s)))
    DBUG_RETURN(error);
  tf_scan_ptr tmp(s);
  MooseDBSnapshot *snap= nullptr;
  if (int error= map_status(moosedb_scan_snapshot(tmp.get(), &snap)))
    DBUG_RETURN(error);
  snapshots_.emplace_back(snap);
  TFStatus st= moosedb_snapshot_fetch(snap, pos, &row);
  if (st == TF_ERR_NOT_FOUND)
  {
    take_last_error();
    DBUG_RETURN(HA_ERR_RECORD_DELETED);
  }
  if (int error= map_status(st))
    DBUG_RETURN(error);
  DBUG_RETURN(fill_record(buf, row));
}

/* ── Index scans ──────────────────────────────────────────────────────── */

const tf_index_def *ha_moosedb::index_def(uint idx) const
{
  if (!share_)
    return nullptr;
  const tf_layout &l= share_->layout;
  return idx < l.indexes.size() ? &l.indexes[idx] : nullptr;
}

int ha_moosedb::index_init(uint idx, bool)
{
  DBUG_ENTER("ha_moosedb::index_init");
  active_index= idx;
  scan_.reset();
  reset_batch_state();
  idx_req_= idx_request();
  DBUG_RETURN(0);
}

int ha_moosedb::index_end()
{
  DBUG_ENTER("ha_moosedb::index_end");
  scan_.reset();
  reset_batch_state();
  idx_req_= idx_request();
  active_index= MAX_KEY;
  DBUG_RETURN(0);
}

/*
  Time value of key part `part` of `key` (a key image of index `idx`), in µs.
  Parts before it are restored too (key_restore reads the key from its start).
*/
int ha_moosedb::key_to_ts(uint idx, const uchar *key, uint part, longlong *out)
{
  assert(table && key);
  if (!table || !key || !out || !table->key_info || idx >= table->s->keys)
    return HA_ERR_INTERNAL_ERROR;
  KEY *ki= &table->key_info[idx];
  if (!ki->key_part || part >= ki->user_defined_key_parts)
    return HA_ERR_INTERNAL_ERROR;
  Field *f= ki->key_part[part].field;
  if (!f)
    return HA_ERR_INTERNAL_ERROR;
  uint len= 0;
  for (uint p= 0; p <= part; p++)
    len+= ki->key_part[p].store_length;
  /* Decode through the field itself, using record[1] as scratch space. */
  key_restore(table->record[1], key, ki, len);
  const my_ptrdiff_t diff= table->record[1] - table->record[0];
  f->move_field_offset(diff);
  longlong ts;
  if (f->real_type() == MYSQL_TYPE_TIMESTAMP ||
      f->real_type() == MYSQL_TYPE_TIMESTAMP2)
  {
    ulong usec= 0;
    ts= (longlong) f->get_timestamp(&usec) * MICROS_PER_SEC + (longlong) usec;
  }
  else if (is_int_axis(f))
  {
    /* integer axis: key image -> seconds -> µs (saturating, see above) */
    ts= secs_to_micros_saturating(f->val_int(), f->is_unsigned());
  }
  else
  {
    MYSQL_TIME lt;
    if (f->get_date(&lt, date_mode_t(0)) || lt.month == 0 || lt.day == 0)
      ts= LONGLONG_MIN;   /* zero date sorts before everything */
    else
      ts= time_to_micros(lt);
  }
  f->move_field_offset(-diff);
  *out= ts;
  return 0;
}

int ha_moosedb::open_range_scan(longlong lo, longlong hi, bool backward,
                                 uchar *buf, int not_found_error)
{
  if (int error= open_scan(lo, hi, true, true))
    return error;
  if (backward)
    if (int error= map_status(moosedb_scan_seek_end(scan_.get())))
      return error;
  return read_from_scan(buf, backward, not_found_error);
}

/*
  ── Composite indexes (TAG_1 .. TAG_k [, time]) ──

  The engine keeps no index structure: an index read is "which series match
  the TAG equalities of the key" (O(1) per value through the TAG cache) plus
  a time range, served by the same k-way merge as the time index. The order
  the server may rely on is therefore only the one this gives for free:

    * all k tags fixed (ref, eq_range, MIN/MAX of the time part): rows come
      in time order, ascending or descending;
    * fewer tags fixed with KEY_EXACT (ref on a prefix): all matching series
      merged in time order -- index_flags() does not announce an order on a
      TAG part, so the server never expects (tag_j, time) order from it.

  A read that does depend on the order BETWEEN tag values -- MIN/MAX(tag)
  (opt_sum announces those through index_flags(idx, part, 0)), index_first,
  index_last, a KEY_OR_NEXT/PREV on a partial tag prefix -- is "ordered":
  the series whose remaining tag tuple is the extreme (under the column's
  own comparison: numeric for integer TAGs, the collation for strings) are
  scanned, and index_next/prev move to the neighbouring tuple when they run
  out. Equality of tags is always the column collation's, never a byte
  compare. Ranges on a TAG part (itemid > 5) are not announced
  (HA_READ_RANGE), so the optimizer never builds one.
*/

/*
  TAG equalities of the first `ntags` parts of an index key, as predicates on
  the canonical text the series store (the text write_row() derived from the
  same field type).
*/
int ha_moosedb::key_tag_predicates(uint idx, const uchar *key, uint ntags,
                                   std::vector<tf_tag_predicate> *out)
{
  assert(table && key && out);
  const tf_index_def *def= index_def(idx);
  if (!table || !key || !out || !def || !table->key_info ||
      idx >= table->s->keys || def->tag_pos.size() < ntags)
    return HA_ERR_INTERNAL_ERROR;
  KEY *ki= &table->key_info[idx];
  if (!ki->key_part || ntags > ki->user_defined_key_parts)
    return HA_ERR_INTERNAL_ERROR;
  uint len= 0;
  for (uint p= 0; p < ntags; p++)
    len+= ki->key_part[p].store_length;
  key_restore(table->record[1], key, ki, len);
  const my_ptrdiff_t diff= table->record[1] - table->record[0];
  MY_BITMAP *old_map= dbug_tmp_use_all_columns(table, &table->read_set);
  int rc= 0;
  for (uint p= 0; p < ntags && !rc; p++)
  {
    Field *f= ki->key_part[p].field;
    if (!f)
    {
      rc= HA_ERR_INTERNAL_ERROR;
      break;
    }
    f->move_field_offset(diff);
    StringBuffer<128> sb;
    String *v= f->val_str(&sb);
    tf_tag_predicate pr{def->tag_pos[p],
                        is_int_tag_field(f) ? &my_charset_bin : f->charset(),
                        {}};
    if (v)
      pr.values.emplace_back(v->ptr(), v->length());
    else
      rc= HA_ERR_INTERNAL_ERROR;
    f->move_field_offset(-diff);
    if (!rc)
      out->push_back(std::move(pr));
  }
  dbug_tmp_restore_column_map(&table->read_set, old_map);
  return rc;
}

namespace {

/* Order of two stored TAG texts under the semantics of the column. */
int cmp_tag_text(const Field *f, const std::string &a, const std::string &b)
{
  if (is_int_tag_field(f))
  {
    if (f->is_unsigned())
    {
      const unsigned long long x= strtoull(a.c_str(), nullptr, 10);
      const unsigned long long y= strtoull(b.c_str(), nullptr, 10);
      return x < y ? -1 : (x > y ? 1 : 0);
    }
    const long long x= strtoll(a.c_str(), nullptr, 10);
    const long long y= strtoll(b.c_str(), nullptr, 10);
    return x < y ? -1 : (x > y ? 1 : 0);
  }
  return f->charset()->strnncollsp((const uchar *) a.data(), a.size(),
                                   (const uchar *) b.data(), b.size());
}

/* Lexicographic order of the first `n` parts of two tag tuples. */
int cmp_tuple_n(const std::vector<const Field *> &fld,
                const std::vector<std::string> &a,
                const std::vector<std::string> &b, size_t n)
{
  for (size_t i= 0; i < n && i < fld.size() && i < a.size() && i < b.size();
       i++)
    if (int c= cmp_tag_text(fld[i], a[i], b[i]))
      return c;
  return 0;
}

/*
  Heap all sorted tuple indexes of the process (every table, every key) may
  hold together, estimated honestly (each String as its header + bytes + a
  malloc rounding, each entry's vectors), and charged twice while one is being
  built (the unsorted rows and the merged entries coexist). Over it the
  ordered read fails with an error -- it never returns a wrong order.
*/
constexpr size_t TUPLE_INDEX_GLOBAL_MAX_BYTES= 512ULL << 20;
std::atomic<size_t> g_tuple_index_bytes{0};

/* Holds a share of the budget until it goes out of scope. */
struct tuple_budget
{
  size_t held= 0;
  bool take(size_t n)
  {
    if (g_tuple_index_bytes.fetch_add(n) + n > TUPLE_INDEX_GLOBAL_MAX_BYTES)
    {
      g_tuple_index_bytes.fetch_sub(n);
      return false;
    }
    held+= n;
    return true;
  }
  ~tuple_budget() { g_tuple_index_bytes.fetch_sub(held); }
};

} // namespace

tf_tuple_index::~tf_tuple_index()
{
  g_tuple_index_bytes.fetch_sub(bytes);
}

/*
  Ordered view of the series of a composite index (see composite_read()):
  every series' tag tuple, sorted like the index, tuple-equal series merged
  into one entry. Rebuilt O(S log S) when the series version moves, shared by
  all handlers of the table; nullptr when it would not fit the global budget.
  The (long) build runs outside the share mutex, so other handlers keep
  working; two handlers may build the same version, the budget covers both.
  The version compared is moosedb_series_version(), the very counter that
  moosedb_series_list_version() reports for the list the index is built from.
*/
std::shared_ptr<const tf_tuple_index> ha_moosedb::tuple_index(uint idx)
{
  const tf_index_def *def= index_def(idx);
  if (!def || def->simple_ts || !table || !table->key_info ||
      idx >= table->s->keys || !share_ || !share_->table)
    return nullptr;
  KEY *ki= &table->key_info[idx];
  const size_t k= def->tag_pos.size();
  std::vector<const Field *> fld;
  for (size_t p= 0; p < k; p++)
  {
    if (!ki->key_part || p >= ki->user_defined_key_parts ||
        !ki->key_part[p].field)
      return nullptr;
    fld.push_back(ki->key_part[p].field);
  }
  uint64_t cur= 0;
  if (moosedb_series_version(share_->table.get(), &cur) != TF_OK)
  {
    take_last_error();
    return nullptr;
  }
  {
    std::lock_guard<std::mutex> guard(share_->tuple_mutex);
    const auto found= share_->tuples.find(idx);
    if (found != share_->tuples.end() && found->second &&
        found->second->version == cur)
      return found->second;
  }

  MooseDBSeriesList *list= nullptr;
  if (moosedb_series_list(share_->table.get(), &list) != TF_OK)
  {
    take_last_error();
    return nullptr;
  }
  struct row
  {
    std::vector<std::string> t;
    uint64_t id;
  };
  const uint64_t n= moosedb_series_list_len(list);
  const uint64_t lv= moosedb_series_list_version(list);
  std::vector<row> rows;
  tuple_budget budget;
  size_t est= 0;                     /* one copy of the data */
  bool ok= true;
  for (uint64_t i= 0; i < n && ok; i++)
  {
    uint64_t id= 0;
    TFRow tags{0, nullptr};
    if (moosedb_series_list_get(list, i, &id, &tags) != TF_OK)
    {
      take_last_error();
      ok= false;
      break;
    }
    row r{{}, id};
    bool usable= tags.values != nullptr;
    size_t cost= sizeof(row) + sizeof(tf_tuple_entry) + sizeof(uint64_t) + 32;
    for (size_t p= 0; usable && p < k; p++)
    {
      const int pos= def->tag_pos[p];
      if (pos < 0 || (uint) pos >= tags.col_count || tags.values[pos].is_null)
        usable= false;
      else
      {
        const TFStr s= tags.values[pos].data.str_val;
        r.t.emplace_back(s.ptr ? s.ptr : "", s.len);
        cost+= sizeof(std::string) + s.len + 32;
      }
    }
    if (!usable)
      continue;
    est+= cost;
    /* rows + merged entries exist together while building: charge both */
    if (!budget.take(2 * cost))
    {
      ok= false;
      break;
    }
    rows.push_back(std::move(r));
  }
  moosedb_series_list_close(list);
  if (!ok)
    return nullptr;
  std::sort(rows.begin(), rows.end(), [&fld, k](const row &a, const row &b) {
    const int c= cmp_tuple_n(fld, a.t, b.t, k);
    return c ? c < 0 : a.id < b.id;
  });
  auto ix= std::make_shared<tf_tuple_index>();
  ix->version= lv;
  for (row &r : rows)
  {
    if (!ix->entries.empty() &&
        cmp_tuple_n(fld, ix->entries.back().tuple, r.t, k) == 0)
    {
      ix->entries.back().ids.push_back(r.id);
      continue;
    }
    tf_tuple_entry e;
    e.tuple= std::move(r.t);
    e.ids.push_back(r.id);
    ix->entries.push_back(std::move(e));
  }
  rows.clear();
  rows.shrink_to_fit();
  /* what stays: the index; the budget keeps counting it until it is freed */
  if (!budget.take(est))
    return nullptr;
  ix->bytes= est;
  budget.held-= est;                 /* ownership moves to the index object */
  std::lock_guard<std::mutex> guard(share_->tuple_mutex);
  auto &slot= share_->tuples[idx];
  if (!slot || slot->version <= lv)
    slot= ix;
  return ix;
}

/*
  Makes the series of entry `entry` of the tuple index -- or, moving in
  `backward` direction, the first entry from there on that still has series
  satisfying the pushed WHERE (`filter`, sorted ids, or nullptr) -- the
  series of the request. Empty when the end of the index is reached.
*/
bool ha_moosedb::pick_entry(const std::vector<uint64_t> *filter, int64_t entry,
                            bool backward)
{
  idx_ids_.clear();
  idx_req_.have_cur= false;
  const tf_tuple_index *tix= idx_req_.tix.get();
  if (!tix)
    return false;
  const int64_t n= (int64_t) tix->entries.size();
  for (int64_t e= entry; e >= 0 && e < n; e+= backward ? -1 : 1)
  {
    const tf_tuple_entry &en= tix->entries[(size_t) e];
    if (filter && en.ids.size() * 8 < filter->size())
    {
      /* few series, big filter: probe it instead of merging all of it */
      for (const uint64_t id : en.ids)
        if (std::binary_search(filter->begin(), filter->end(), id))
          idx_ids_.push_back(id);
    }
    else if (filter)
      std::set_intersection(en.ids.begin(), en.ids.end(), filter->begin(),
                            filter->end(), std::back_inserter(idx_ids_));
    else
      idx_ids_= en.ids;
    if (!idx_ids_.empty())
    {
      idx_req_.cur= en.tuple;
      idx_req_.have_cur= true;
      return true;
    }
  }
  return true;
}

/* The neighbouring tag tuple of the one being scanned (idx_req_.cur). */
bool ha_moosedb::walk_entry(const std::vector<uint64_t> *filter, bool backward)
{
  if (!idx_req_.have_cur)
  {
    idx_ids_.clear();
    return true;
  }
  if (!idx_req_.tix)
    idx_req_.tix= tuple_index(idx_req_.index);
  const tf_tuple_index *tix= idx_req_.tix.get();
  const tf_index_def *def= index_def(idx_req_.index);
  if (!tix || !def || !table || idx_req_.index >= table->s->keys)
    return false;
  KEY *ki= &table->key_info[idx_req_.index];
  std::vector<const Field *> fld;
  for (size_t p= 0; p < def->tag_pos.size(); p++)
  {
    if (!ki->key_part || p >= ki->user_defined_key_parts ||
        !ki->key_part[p].field)
      return false;
    fld.push_back(ki->key_part[p].field);
  }
  const size_t k= fld.size();
  const std::vector<std::string> cur= idx_req_.cur;
  /* forward: first tuple > cur; backward: last tuple < cur */
  const auto it= std::partition_point(
      tix->entries.begin(), tix->entries.end(),
      [&](const tf_tuple_entry &e) {
        const int c= cmp_tuple_n(fld, e.tuple, cur, k);
        return backward ? c < 0 : c <= 0;
      });
  const int64_t pos= (int64_t) (it - tix->entries.begin());
  return pick_entry(filter, backward ? pos - 1 : pos, backward);
}

/* Series of the current composite-index request (see open_scan()). */
bool ha_moosedb::series_for_index(const std::vector<uint64_t> **ids)
{
  *ids= &idx_ids_;
  const tf_index_def *def= index_def(idx_req_.index);
  if (!def || !table || idx_req_.index >= table->s->keys)
    return false;
  std::vector<const tf_tag_predicate *> preds;
  for (const auto &level : pushed_)
    for (const tf_tag_predicate &p : level)
      preds.push_back(&p);
  const size_t npushed= preds.size();
  for (const tf_tag_predicate &p : idx_req_.preds)
    preds.push_back(&p);
  if (!idx_req_.ordered)
  {
    if (preds.empty())
      return false;
    return resolve_series(preds, &idx_ids_);
  }

  /* The WHERE's own TAG conditions still apply to every series picked. */
  std::vector<uint64_t> filt;
  const bool use_filter= npushed > 0;
  if (use_filter)
  {
    std::vector<const tf_tag_predicate *> w(preds.begin(),
                                            preds.begin() + npushed);
    if (!resolve_series(w, &filt))
      return false;
  }
  const std::vector<uint64_t> *f= use_filter ? &filt : nullptr;
  if (idx_req_.walk)
    return walk_entry(f, idx_req_.backward);

  const size_t k= def->tag_pos.size();
  const size_t nkey= idx_req_.preds.size();
  const idx_op op= idx_req_.op;
  /*
    All tags given and an equal series exists (the common case: MAX(clock) of
    one item, a descending range): that series is the answer, no need to look
    at any other tuple.
  */
  if (nkey == k && nkey > 0 &&
      (op == idx_op::EQ || op == idx_op::GE || op == idx_op::LE))
  {
    if (!resolve_series(preds, &idx_ids_))
      return false;
    if (!idx_ids_.empty())
    {
      idx_req_.cur.clear();
      for (const tf_tag_predicate &p : idx_req_.preds)
        idx_req_.cur.push_back(p.values.empty() ? std::string() : p.values[0]);
      idx_req_.have_cur= true;
      return true;
    }
    if (op == idx_op::EQ)
    {
      idx_req_.have_cur= false;
      return true;
    }
  }

  KEY *ki= &table->key_info[idx_req_.index];
  std::vector<const Field *> fld;
  for (size_t p= 0; p < k; p++)
  {
    if (!ki->key_part || p >= ki->user_defined_key_parts ||
        !ki->key_part[p].field)
      return false;
    fld.push_back(ki->key_part[p].field);
  }
  idx_req_.tix= tuple_index(idx_req_.index);
  if (!idx_req_.tix)
  {
    last_error_= "cannot build the ordered view of the series of this index";
    return false;
  }
  const tf_tuple_index &tix= *idx_req_.tix;
  std::vector<std::string> key;
  for (const tf_tag_predicate &p : idx_req_.preds)
    key.push_back(p.values.empty() ? std::string() : p.values[0]);
  /* entries with prefix < key end at lb; with prefix <= key at ub */
  const auto lb= std::partition_point(
      tix.entries.begin(), tix.entries.end(), [&](const tf_tuple_entry &e) {
        return cmp_tuple_n(fld, e.tuple, key, nkey) < 0;
      }) - tix.entries.begin();
  const auto ub= std::partition_point(
      tix.entries.begin(), tix.entries.end(), [&](const tf_tuple_entry &e) {
        return cmp_tuple_n(fld, e.tuple, key, nkey) <= 0;
      }) - tix.entries.begin();
  const int64_t size= (int64_t) tix.entries.size();
  int64_t entry= -1;
  bool backward= false;
  switch (op) {
  case idx_op::NONE:
    backward= idx_req_.backward;
    entry= backward ? size - 1 : 0;
    break;
  case idx_op::EQ:
    backward= idx_req_.backward;
    entry= lb < ub ? (backward ? ub - 1 : lb) : -1;
    break;
  case idx_op::GE:
    entry= lb;
    break;
  case idx_op::GT:
    entry= ub;
    break;
  case idx_op::LE:
    backward= true;
    entry= ub - 1;
    break;
  case idx_op::LT:
    backward= true;
    entry= lb - 1;
    break;
  }
  return pick_entry(f, entry, backward);
}

/*
  Moves an ordered read to the neighbouring tag tuple. Tuples whose series
  hold no visible row (expired by retention, say) are passed over.
*/
int ha_moosedb::advance_group(uchar *buf, bool backward)
{
  for (;;)
  {
    idx_req_.walk= true;
    idx_req_.backward= backward;
    if (!idx_req_.have_cur)
      return HA_ERR_END_OF_FILE;
    if (int rc= open_scan(idx_req_.lo, idx_req_.hi, true, true))
      return rc;
    if (backward)
      if (int rc= map_status(moosedb_scan_seek_end(scan_.get())))
        return rc;
    if (idx_ids_.empty())
      return HA_ERR_END_OF_FILE;
    const int rc= read_from_scan(buf, backward, HA_ERR_END_OF_FILE);
    if (rc != HA_ERR_END_OF_FILE)
      return rc;
    /* the tuple has no visible rows: series_for_index() moves on from it */
  }
}

/* index_first / index_last of a composite index */
int ha_moosedb::composite_ends(uchar *buf, bool last)
{
  const tf_index_def *def= index_def(active_index);
  if (!def || def->simple_ts || !table || active_index >= table->s->keys)
    return HA_ERR_INTERNAL_ERROR;
  idx_req_= idx_request();
  idx_req_.active= true;
  idx_req_.index= active_index;
  idx_req_.ordered= true;
  idx_req_.op= idx_op::NONE;
  idx_req_.backward= last;
  if (int rc= open_scan(LONGLONG_MIN, LONGLONG_MAX, true, true))
    return rc;
  if (last)
    if (int rc= map_status(moosedb_scan_seek_end(scan_.get())))
      return rc;
  if (idx_ids_.empty())
    return HA_ERR_END_OF_FILE;
  const int rc= read_from_scan(buf, last, HA_ERR_END_OF_FILE);
  return rc == HA_ERR_END_OF_FILE ? advance_group(buf, last) : rc;
}

/*
  Values of key parts [from, to) of `key` (a key image of index `idx`): the
  integer columns that follow the time part of a UNIQUE key. Decoded by the
  fields themselves, in a private record (the caller's buffers stay intact).
*/
int ha_moosedb::key_extra_values(uint idx, const uchar *key, uint from,
                                 uint to, std::vector<longlong> *out)
{
  assert(table && key && out);
  if (!table || !key || !out || !table->key_info || idx >= table->s->keys)
    return HA_ERR_INTERNAL_ERROR;
  KEY *ki= &table->key_info[idx];
  if (!ki->key_part || from > to || to > ki->user_defined_key_parts)
    return HA_ERR_INTERNAL_ERROR;
  uint len= 0;
  for (uint p= 0; p < to; p++)
    len+= ki->key_part[p].store_length;
  std::vector<uchar> scratch(table->s->rec_buff_length, 0);
  key_restore(scratch.data(), key, ki, len);
  const my_ptrdiff_t diff= scratch.data() - table->record[0];
  out->clear();
  for (uint p= from; p < to; p++)
  {
    Field *f= ki->key_part[p].field;
    if (!f)
      return HA_ERR_INTERNAL_ERROR;
    f->move_field_offset(diff);
    out->push_back(f->val_int());
    f->move_field_offset(-diff);
  }
  return 0;
}

/* Whether the row in `rec` has the extra key columns of idx_req_.extra_vals. */
bool ha_moosedb::extras_equal(const uchar *rec) const
{
  const tf_index_def *def= index_def(idx_req_.index);
  if (!def || !table || !rec || idx_req_.index >= table->s->keys ||
      !table->key_info)
    return false;
  const KEY *ki= &table->key_info[idx_req_.index];
  const uint first= (uint) def->tag_pos.size() + 1;
  const my_ptrdiff_t diff= rec - table->record[0];
  bool same= true;
  for (size_t e= 0; e < idx_req_.extra_vals.size() && same; e++)
  {
    if (!ki->key_part || first + e >= ki->user_defined_key_parts ||
        !ki->key_part[first + e].field)
      return false;
    Field *f= ki->key_part[first + e].field;
    f->move_field_offset(diff);
    same= f->val_int() == idx_req_.extra_vals[e];
    f->move_field_offset(-diff);
  }
  return same;
}

int ha_moosedb::composite_read(uchar *buf, const uchar *key,
                               key_part_map map,
                               enum ha_rkey_function find_flag)
{
  const tf_index_def *def= index_def(active_index);
  if (!def || def->simple_ts || !table || !key ||
      active_index >= table->s->keys || !table->key_info)
    return HA_ERR_INTERNAL_ERROR;
  KEY *ki= &table->key_info[active_index];
  const uint k= (uint) def->tag_pos.size();
  uint nparts= 0;
  while (nparts < 32 && ((map >> nparts) & 1))
    nparts++;
  if (nparts == 0 || map != (((key_part_map) 1 << nparts) - 1) ||
      nparts > ki->user_defined_key_parts)
  {
    sql_print_warning("MooseDB: %s: index read with an unsupported key part "
                      "map 0x%llx (flag %d)",
                      share_ ? share_->display.c_str() : "?",
                      (unsigned long long) map, (int) find_flag);
    return HA_ERR_WRONG_COMMAND;
  }
  const uint ntag= std::min(nparts, k);
  const bool time_given= def->has_time && nparts > k;
  /* key parts given after the time part (a UNIQUE key: ns of Zabbix) */
  const uint nextra= time_given ? nparts - k - 1 : 0;
  const bool all_tags= ntag == k;

  idx_req_= idx_request();
  idx_req_.active= true;
  idx_req_.index= active_index;
  if (int rc= key_tag_predicates(active_index, key, ntag, &idx_req_.preds))
  {
    idx_req_= idx_request();
    return rc;
  }
  longlong ts= 0, lo= LONGLONG_MIN, hi= LONGLONG_MAX;
  if (time_given)
    if (int rc= key_to_ts(active_index, key, k, &ts))
    {
      idx_req_= idx_request();
      return rc;
    }
  if (nextra > 0)
    if (int rc= key_extra_values(active_index, key, k + 1, nparts,
                                 &idx_req_.extra_vals))
    {
      idx_req_= idx_request();
      return rc;
    }
  uint tag_len= 0;
  for (uint p= 0; p < ntag; p++)
    tag_len+= ki->key_part[p].store_length;
  /* the range being read ends on the same tags as it starts: it is confined
     to the series of this key */
  const bool confined= end_range && end_range->key &&
                       end_range->length >= tag_len &&
                       memcmp(key, end_range->key, tag_len) == 0;

  bool backward= false, ordered= false, exact= false;
  idx_op op= idx_op::NONE;
  /*
    Only the exact lookup of a UNIQUE key filters on the columns after the time
    part. A range read that names them (clock BETWEEN a AND b AND ns = 1 gives
    the start key (itemid, a, 1)) starts at the time part: rows of that time
    with another ns are a superset the server filters out, and no row that
    belongs is skipped (the server re-evaluates the WHERE after a range read).
  */
  /* (a descending ref read, PREFIX_LAST, is an equality: the server does not re-check it) */
  const bool superset= nextra > 0 && find_flag != HA_READ_KEY_EXACT &&
                       find_flag != HA_READ_PREFIX &&
                       find_flag != HA_READ_PREFIX_LAST;
  if (superset)
    idx_req_.extra_vals.clear();
  /*
    With the time part given, the tags are equalities of a ref / range / MIN
    or MAX(time) read: the rows are those of the key's series. (The server
    only asks for "the next tag tuple" with tags alone.) Tags alone with a
    range operator can be a MIN/MAX of the TAG itself, e.g.
    MAX(itemid) WHERE itemid <= N: then the answer is the neighbouring tuple
    if the key's own series do not exist, an ordered read.
  */
  switch (find_flag) {
  case HA_READ_KEY_EXACT:
  case HA_READ_PREFIX:
    exact= true;
    if (time_given)
      lo= hi= ts;
    break;
  case HA_READ_KEY_OR_NEXT:
    if (time_given)
      lo= ts;
    else if (!(all_tags && confined))
    {
      ordered= true;
      op= idx_op::GE;
    }
    break;
  case HA_READ_AFTER_KEY:
    if (time_given)
    {
      if (ts == LONGLONG_MAX)
        goto not_found;
      lo= superset ? ts : ts + 1;
    }
    else
    {
      ordered= true;
      op= idx_op::GT;
    }
    break;
  case HA_READ_KEY_OR_PREV:
  case HA_READ_PREFIX_LAST_OR_PREV:
    backward= true;
    if (time_given)
      hi= ts;
    else
    {
      ordered= true;
      op= idx_op::LE;
    }
    break;
  case HA_READ_BEFORE_KEY:
    backward= true;
    if (time_given)
    {
      if (ts == LONGLONG_MIN)
        goto not_found;
      hi= superset ? ts : ts - 1;
    }
    else
    {
      ordered= true;
      op= idx_op::LT;
    }
    break;
  case HA_READ_PREFIX_LAST:
    backward= true;
    if (time_given)
      lo= hi= ts;
    else if (!all_tags)
    {
      ordered= true;
      op= idx_op::EQ;
    }
    break;
  default:
    goto unsupported;
  }
  /*
    A forward range read knows where it ends (end_range): when it ends on the
    same tags, its time bound tightens the merge. The server re-checks the
    bound, so inclusive is always a safe superset.
  */
  if (!backward && !ordered && all_tags && def->has_time && confined &&
      end_range->length > tag_len)
  {
    longlong end_ts;
    if (int rc= key_to_ts(active_index, end_range->key, k, &end_ts))
    {
      idx_req_= idx_request();
      return rc;
    }
    hi= std::min(hi, end_ts);
  }
  idx_req_.ordered= ordered;
  idx_req_.exact= exact && nextra == 0;
  idx_req_.op= op;
  idx_req_.backward= backward;
  idx_req_.lo= lo;
  idx_req_.hi= hi;
  {
    if (int rc= open_scan(lo, hi, true, true))
      return rc;
    if (idx_ids_.empty() && ordered)
      return HA_ERR_KEY_NOT_FOUND;
    if (backward)
      if (int rc= map_status(moosedb_scan_seek_end(scan_.get())))
        return rc;
    int rc= read_from_scan(buf, backward, HA_ERR_KEY_NOT_FOUND);
    /* exact lookup of a full UNIQUE key: the rows of that time, filtered by the extra columns */
    while (rc == 0 && !idx_req_.extra_vals.empty() && !extras_equal(buf))
      rc= read_from_scan(buf, backward, HA_ERR_KEY_NOT_FOUND);
    if (ordered && rc == HA_ERR_KEY_NOT_FOUND)
    {
      /* series without visible rows: the next tuple in that direction */
      const int r2= advance_group(buf, backward);
      return r2 == HA_ERR_END_OF_FILE ? HA_ERR_KEY_NOT_FOUND : r2;
    }
    return rc;
  }
unsupported:
  sql_print_warning("MooseDB: %s: unsupported index read (key part map 0x%llx, "
                    "flag %d)",
                    share_ ? share_->display.c_str() : "?",
                    (unsigned long long) map, (int) find_flag);
  idx_req_= idx_request();
  return HA_ERR_WRONG_COMMAND;
not_found:
  idx_req_= idx_request();
  return HA_ERR_KEY_NOT_FOUND;
}

int ha_moosedb::index_read_map(uchar *buf, const uchar *key,
                                key_part_map keypart_map,
                                enum ha_rkey_function find_flag)
{
  DBUG_ENTER("ha_moosedb::index_read_map");
  if (!key || !keypart_map)
    DBUG_RETURN(find_flag == HA_READ_PREFIX_LAST ||
                find_flag == HA_READ_PREFIX_LAST_OR_PREV
                ? index_last(buf) : index_first(buf));

  if (!table || active_index >= table->s->keys)
    DBUG_RETURN(HA_ERR_INTERNAL_ERROR);
  const tf_index_def *def= index_def(active_index);
  if (!def)
    DBUG_RETURN(HA_ERR_INTERNAL_ERROR);
  /* HA_WHOLE_KEY (const / eq_ref lookups, ODKU and REPLACE) = every part */
  if (keypart_map == HA_WHOLE_KEY && table->key_info)
    keypart_map= ((key_part_map) 1 << table->key_info[active_index].user_defined_key_parts) - 1;
  if (!def->simple_ts)
    DBUG_RETURN(composite_read(buf, key, keypart_map, find_flag));
  longlong ts;
  if (int error= key_to_ts(active_index, key, 0, &ts))
    DBUG_RETURN(error);
  longlong lo= LONGLONG_MIN, hi= LONGLONG_MAX;
  bool backward= false;
  switch (find_flag) {
  case HA_READ_KEY_EXACT:
  case HA_READ_PREFIX:
    lo= hi= ts; break;
  case HA_READ_KEY_OR_NEXT:
    lo= ts; break;
  case HA_READ_AFTER_KEY:
    if (ts == LONGLONG_MAX)
      DBUG_RETURN(HA_ERR_KEY_NOT_FOUND);
    lo= ts + 1; break;
  case HA_READ_KEY_OR_PREV:
  case HA_READ_PREFIX_LAST_OR_PREV:
    hi= ts; backward= true; break;
  case HA_READ_BEFORE_KEY:
    if (ts == LONGLONG_MIN)
      DBUG_RETURN(HA_ERR_KEY_NOT_FOUND);
    hi= ts - 1; backward= true; break;
  case HA_READ_PREFIX_LAST:
    lo= hi= ts; backward= true; break;
  default:
    DBUG_RETURN(HA_ERR_WRONG_COMMAND);
  }
  /*
    For forward range reads the server has already set end_range (see
    handler::read_range_first) and stops at it; bounding the scan with it
    lets the merge skip runs past the end. Bounding by the end key
    inclusively is always a superset, the server re-checks the bound.
  */
  if (!backward && end_range && end_range->key)
  {
    longlong end_ts;
    if (int error= key_to_ts(active_index, end_range->key, 0, &end_ts))
      DBUG_RETURN(error);
    hi= std::min(hi, end_ts);
  }
  DBUG_RETURN(open_range_scan(lo, hi, backward, buf, HA_ERR_KEY_NOT_FOUND));
}

int ha_moosedb::index_next(uchar *buf)
{
  DBUG_ENTER("ha_moosedb::index_next");
  const int rc= read_from_scan(buf, false, HA_ERR_END_OF_FILE);
  if (rc == HA_ERR_END_OF_FILE && idx_req_.active && idx_req_.ordered)
    DBUG_RETURN(advance_group(buf, false));
  DBUG_RETURN(rc);
}

/*
  The rows of a KEY_EXACT read on a composite index are the rows of the series
  whose tags equal the key (under the column collation), at the key's time:
  there is nothing left to compare. The generic version would re-compare the
  key parts of every row it returns (correctly, but at a cost comparable to
  decoding the row itself).
*/
int ha_moosedb::index_next_same(uchar *buf, const uchar *key, uint keylen)
{
  DBUG_ENTER("ha_moosedb::index_next_same");
  const tf_index_def *def= index_def(active_index);
  if (def && !def->simple_ts && idx_req_.active && idx_req_.exact &&
      !idx_req_.ordered)
    DBUG_RETURN(index_next(buf));
  DBUG_RETURN(handler::index_next_same(buf, key, keylen));
}

int ha_moosedb::index_prev(uchar *buf)
{
  DBUG_ENTER("ha_moosedb::index_prev");
  const int rc= read_from_scan(buf, true, HA_ERR_END_OF_FILE);
  if (rc == HA_ERR_END_OF_FILE && idx_req_.active && idx_req_.ordered)
    DBUG_RETURN(advance_group(buf, true));
  DBUG_RETURN(rc);
}

int ha_moosedb::index_first(uchar *buf)
{
  DBUG_ENTER("ha_moosedb::index_first");
  const tf_index_def *def= index_def(active_index);
  if (def && !def->simple_ts)
    DBUG_RETURN(composite_ends(buf, false));
  DBUG_RETURN(open_range_scan(LONGLONG_MIN, LONGLONG_MAX, false, buf,
                              HA_ERR_END_OF_FILE));
}

int ha_moosedb::index_last(uchar *buf)
{
  DBUG_ENTER("ha_moosedb::index_last");
  const tf_index_def *def= index_def(active_index);
  if (def && !def->simple_ts)
    DBUG_RETURN(composite_ends(buf, true));
  DBUG_RETURN(open_range_scan(LONGLONG_MIN, LONGLONG_MAX, true, buf,
                              HA_ERR_END_OF_FILE));
}

/* ── Statistics ───────────────────────────────────────────────────────── */

/*
  What the optimizer may assume about index order (see composite_read()).
  Called on an unopened handler too (TABLE_SHARE setup), so it reads only the
  key definition: a TAG part is a tag, the one non-TAG part is the time.

    time part        next/prev, ordered, range    (rows of fixed tags come in
                                                   time order, either way)
    TAG part         next/prev, and ORDER only for single-part queries
                     (all_parts == 0, which is what MIN/MAX(col) asks and is
                     served by the ordered reads); with all_parts == 1 -- the
                     question behind ORDER BY / GROUP BY on the key prefix --
                     no order: rows of different tag values are not returned
                     in tag order unless an ordered read was requested.
                     No HA_READ_RANGE: only equalities (=, IN, <=>) are
                     turned into key ranges on a TAG part.
*/
ulong ha_moosedb::index_flags(uint idx, uint part, bool all_parts) const
{
  constexpr ulong TIME= HA_READ_NEXT | HA_READ_PREV | HA_READ_ORDER |
                        HA_READ_RANGE;
  const TABLE_SHARE *s= table_share;
  if (!s || !s->key_info || idx >= s->keys)
    return TIME;
  const KEY &key= s->key_info[idx];
  if (!key.key_part || part >= key.user_defined_key_parts)
    return 0;
  const Field *f= key.key_part[part].field;
  if (!f)
    return 0;
  /*
    A part after the time column (UNIQUE key, e.g. ns): within one timestamp
    the scans return rows ordered by it (the core sorts timestamp ties by the
    key's extra columns), so ORDER BY clock DESC, ns DESC LIMIT 1 needs no
    sort. No HA_READ_RANGE: only equalities become key ranges on it.
  */
  for (uint q= 0; q < part; q++)
  {
    const Field *g= key.key_part[q].field;
    if (g && !is_tag_comment(g->comment))
      return HA_READ_NEXT | HA_READ_PREV | HA_READ_ORDER;
  }
  if (!is_tag_comment(f->comment))
    return TIME;
  return all_parts ? (HA_READ_NEXT | HA_READ_PREV)
                   : (HA_READ_NEXT | HA_READ_PREV | HA_READ_ORDER);
}

int ha_moosedb::info(uint flag)
{
  DBUG_ENTER("ha_moosedb::info");
  if (flag & HA_STATUS_ERRKEY)
    errkey= dup_key_;   /* the key of the last HA_ERR_FOUND_DUPP_KEY */
  if (!share_ || !share_->table)
    DBUG_RETURN(0);
  if (flag & (HA_STATUS_VARIABLE | HA_STATUS_CONST))
  {
    uint64_t rows= 0, data= 0, compressed= 0;
    uint32_t chunks= 0;
    if (int error= map_status(moosedb_table_stats(share_->table.get(), &rows,
                                                   &data, &compressed,
                                                   &chunks)))
      DBUG_RETURN(error);
    stats.records= (ha_rows) rows;
    stats.deleted= 0;
    stats.data_file_length= compressed;
    stats.index_file_length= 0;
    stats.mean_rec_length= rows ? (ulong) (data / rows) : 0;
    update_rec_per_key(rows);
  }
  DBUG_RETURN(0);
}

/*
  KEY::rec_per_key of the composite indexes: average rows per key prefix.
  A series holds at most one row per timestamp in practice, so:
    prefix of j tags : rows / distinct tuples of those tags, where distinct is
                       min(series, product of the per-tag distinct values)
    the time part    : series per full tag tuple (rows per (tags, time))
  The cardinalities come from the TAG cache (one entry per value, kept in
  step with the series list), recomputed only when the series version moves.
*/
void ha_moosedb::update_rec_per_key(uint64_t rows)
{
  if (!table || !table->s || !table->key_info || !share_ || !share_->table)
    return;
  const tf_layout &l= share_->layout;
  bool any= false;
  for (const tf_index_def &d : l.indexes)
    any|= !d.simple_ts && !d.tag_pos.empty();
  if (!any)
    return;

  std::vector<CHARSET_INFO *> tag_cs;
  for (const tf_column &c : l.columns)
    if (c.conv == tf_conv::TAG)
    {
      if (c.field_index >= table->s->fields || !table->field[c.field_index])
        return;
      tag_cs.push_back(is_int_tag_field(table->field[c.field_index])
                         ? &my_charset_bin
                         : table->field[c.field_index]->charset());
    }

  tf_index_stats &st= share_->index_stats;
  std::lock_guard<std::mutex> guard(st.mutex);
  uint64_t cur= 0;
  if (moosedb_series_version(share_->table.get(), &cur) != TF_OK)
  {
    take_last_error();
    return;
  }
  if (!st.valid || st.version != cur)
  {
    st.valid= false;
    st.distinct.clear();
    std::vector<tf_tag_predicate> wanted;
    for (const tf_index_def &d : l.indexes)
      if (!d.simple_ts)
        for (const int pos : d.tag_pos)
          if (pos >= 0 && (size_t) pos < tag_cs.size())
            wanted.push_back({pos, tag_cs[pos], {}});
    std::vector<const tf_tag_predicate *> preds;
    for (const tf_tag_predicate &p : wanted)
      preds.push_back(&p);
    /* Builds/extends the cache index of every wanted column; no values:
       the (empty) answer is not used. */
    std::vector<uint64_t> unused;
    if (!pushed_series_cached(preds, tag_cs, &unused))
      return;                        /* too big to index: leave the estimates */
    tf_tag_cache &cache= share_->tag_cache;
    std::lock_guard<std::mutex> cg(cache.mutex);
    if (!cache.valid)
      return;
    st.series= cache.len;
    for (const tf_tag_predicate &p : wanted)
    {
      const auto it= cache.indexes.find({p.tag_pos, (const CHARSET_INFO *) p.cs});
      if (it == cache.indexes.end())
        return;
      st.distinct[{p.tag_pos, (const CHARSET_INFO *) p.cs}]=
          it->second.groups.size();
    }
    st.version= cur;
    st.epoch= cache.epoch;
    st.valid= true;
  }
  if (st.series == 0)
    return;
  for (uint idx= 0; idx < l.indexes.size() && idx < table->s->keys; idx++)
  {
    const tf_index_def &d= l.indexes[idx];
    KEY &ki= table->key_info[idx];
    if (d.simple_ts || !ki.rec_per_key)
      continue;
    double distinct= 1;
    for (uint p= 0; p < ki.user_defined_key_parts && p < ki.ext_key_parts; p++)
    {
      double rpk;
      if (p < d.tag_pos.size())
      {
        const int pos= d.tag_pos[p];
        if (pos < 0 || (size_t) pos >= tag_cs.size())
          break;
        const auto it= st.distinct.find({pos, (const CHARSET_INFO *) tag_cs[pos]});
        if (it == st.distinct.end())
          break;
        distinct= std::min<double>((double) st.series,
                                   distinct * (double) std::max<uint64_t>(it->second, 1));
        rpk= (double) rows / distinct;
      }
      else if (p == d.tag_pos.size())
        /* the time part; the last part of a UNIQUE key is one row per value */
        rpk= (ki.flags & HA_NOSAME) && p + 1 == ki.user_defined_key_parts
               ? 1.0 : (double) st.series / distinct;
      else
        rpk= 1.0;                               /* extra UNIQUE columns */
      ki.rec_per_key[p]= (ulong) std::max<double>(1.0, std::min(rpk, 4.0e9));
    }
  }
}

/*
  Series matching TAG equalities, and how many the table has. false when the
  matching could not be resolved.
*/
bool ha_moosedb::estimate_series(const std::vector<tf_tag_predicate> &preds,
                                 uint64_t *matched, uint64_t *total)
{
  std::vector<const tf_tag_predicate *> pv;
  for (const tf_tag_predicate &p : preds)
    pv.push_back(&p);
  std::vector<uint64_t> ids;
  if (!resolve_series(pv, &ids))
    return false;
  /*
    The series count: the TAG cache that just answered knows it (O(1)). Only
    when it is not valid (past its limits) is the list asked, which is an
    Arc clone plus its length -- no walk over the series either way.
  */
  {
    tf_tag_cache &cache= share_->tag_cache;
    std::lock_guard<std::mutex> guard(cache.mutex);
    if (cache.valid)
    {
      *total= cache.len;
      *matched= ids.size();
      return true;
    }
  }
  MooseDBSeriesList *list= nullptr;
  if (moosedb_series_list(share_->table.get(), &list) != TF_OK)
  {
    take_last_error();
    return false;
  }
  *total= moosedb_series_list_len(list);
  moosedb_series_list_close(list);
  *matched= ids.size();
  return true;
}

ha_rows ha_moosedb::records_in_range(uint inx, const key_range *min_key,
                                      const key_range *max_key, page_range *)
{
  DBUG_ENTER("ha_moosedb::records_in_range");
  if (!table || !share_ || !share_->table || inx >= table->s->keys ||
      !table->key_info)
    DBUG_RETURN(HA_POS_ERROR);
  if ((min_key && !min_key->key) || (max_key && !max_key->key))
    DBUG_RETURN(HA_POS_ERROR);
  const tf_index_def *def= index_def(inx);
  if (!def)
    DBUG_RETURN(HA_POS_ERROR);
  /* an equality on every part of the UNIQUE key matches at most one row */
  {
    const KEY &uk= table->key_info[inx];
    if ((uk.flags & HA_NOSAME) && min_key && max_key &&
        min_key->flag == HA_READ_KEY_EXACT &&
        max_key->flag == HA_READ_AFTER_KEY &&
        min_key->length == uk.key_length && max_key->length == uk.key_length)
      DBUG_RETURN(1);
  }
  longlong lo= LONGLONG_MIN, hi= LONGLONG_MAX;

  if (def->simple_ts)
  {
    if (min_key)
    {
      if (key_to_ts(inx, min_key->key, 0, &lo))
        DBUG_RETURN(HA_POS_ERROR);
      if (min_key->flag == HA_READ_AFTER_KEY && lo < LONGLONG_MAX)
        lo++;
    }
    if (max_key)
    {
      if (key_to_ts(inx, max_key->key, 0, &hi))
        DBUG_RETURN(HA_POS_ERROR);
      if (max_key->flag == HA_READ_BEFORE_KEY && hi > LONGLONG_MIN)
        hi--;
    }
    uint64_t rows= 0;
    if (moosedb_estimate_rows(share_->table.get(), lo, hi, &rows) != TF_OK)
    {
      take_last_error();
      DBUG_RETURN(HA_POS_ERROR);
    }
    /* Never 0: the optimizer would treat the range as provably empty. */
    DBUG_RETURN((ha_rows) std::max<uint64_t>(rows, 1));
  }

  /*
    Composite index. The range is "these tags, this time window": the rows
    of the window, scaled by the share of series the tags select. Series are
    assumed to hold similar amounts of data (no per-series row counts exist).
  */
  KEY *ki= &table->key_info[inx];
  if (!ki->key_part)
    DBUG_RETURN(HA_POS_ERROR);
  const uint k= (uint) def->tag_pos.size();
  const auto parts_of= [ki](const key_range *kr) -> uint {
    if (!kr || !kr->key)
      return 0;
    uint acc= 0, n= 0;
    while (n < ki->user_defined_key_parts && acc < kr->length)
      acc+= ki->key_part[n++].store_length;
    return n;
  };
  const uint nmin= parts_of(min_key), nmax= parts_of(max_key);
  const key_range *src= nmin >= nmax ? min_key : max_key;
  const uint nsrc= std::max(nmin, nmax);
  if (nmin > 0 && nmax > 0)
  {
    /* both ends must name the same tags, else this is a range over tags */
    uint len= 0;
    for (uint p= 0; p < std::min(std::min(nmin, nmax), k); p++)
      len+= ki->key_part[p].store_length;
    if (memcmp(min_key->key, max_key->key, len) != 0)
      DBUG_RETURN(HA_POS_ERROR);
  }
  if (def->has_time)
  {
    if (nmin > k)
    {
      if (key_to_ts(inx, min_key->key, k, &lo))
        DBUG_RETURN(HA_POS_ERROR);
      if (min_key->flag == HA_READ_AFTER_KEY && lo < LONGLONG_MAX)
        lo++;
    }
    if (nmax > k)
    {
      if (key_to_ts(inx, max_key->key, k, &hi))
        DBUG_RETURN(HA_POS_ERROR);
      if (max_key->flag == HA_READ_BEFORE_KEY && hi > LONGLONG_MIN)
        hi--;
    }
  }
  uint64_t window= 0;
  if (moosedb_estimate_rows(share_->table.get(), lo, hi, &window) != TF_OK)
  {
    take_last_error();
    DBUG_RETURN(HA_POS_ERROR);
  }
  uint64_t matched= 0, total= 0;
  const uint ntag= std::min(nsrc, k);
  if (ntag > 0)
  {
    std::vector<tf_tag_predicate> preds;
    if (key_tag_predicates(inx, src->key, ntag, &preds) ||
        !estimate_series(preds, &matched, &total))
      DBUG_RETURN(HA_POS_ERROR);
    if (total > 0)
      window= (uint64_t) std::ceil((double) window * (double) matched /
                                   (double) total);
  }
  /* Never 0: the optimizer would treat the range as provably empty. */
  DBUG_RETURN((ha_rows) std::max<uint64_t>(window, 1));
}

/* ── INFORMATION_SCHEMA ───────────────────────────────────────────────── */

namespace {

/*
  Calls f(db, table, path) for every MooseDB table directory in the data
  directory: <datadir>/<db>/<table>/MANIFEST next to <db>/<table>.frm.
  f returns true to stop.
*/
template <typename F> void for_each_moosedb_table(F f)
{
  namespace fs= std::filesystem;
  try
  {
    for (const fs::directory_entry &db : fs::directory_iterator(mysql_real_data_home))
    {
      std::error_code ec;
      if (!db.is_directory(ec))
        continue;
      const std::string db_file= db.path().filename().string();
      if (db_file.empty() || db_file[0] == '#' || db_file[0] == '.')
        continue;
      for (const fs::directory_entry &t : fs::directory_iterator(db.path()))
      {
        if (!t.is_directory(ec))
          continue;
        const std::string t_file= t.path().filename().string();
        if (t_file.rfind("#sql", 0) == 0 ||
            !fs::exists(t.path() / "MANIFEST", ec) ||
            !fs::exists(db.path() / (t_file + ".frm"), ec))
          continue;
        char db_name[NAME_LEN + 1], t_name[NAME_LEN + 1];
        filename_to_tablename(db_file.c_str(), db_name, sizeof(db_name));
        filename_to_tablename(t_file.c_str(), t_name, sizeof(t_name));
        if (f(db_name, t_name, t.path().string()))
          return;
      }
    }
  }
  catch (const std::exception &e)
  {
    sql_print_warning("MooseDB: cannot scan the data directory: %s", e.what());
  }
}

struct tf_info_closer
{
  void operator()(MooseDBInfo *i) const noexcept { moosedb_info_close(i); }
};

void store_str(Field *f, const char *s)
{
  f->store(s ? s : "", s ? strlen(s) : 0, system_charset_info);
}

void store_micros(Field *f, longlong us)
{
  MYSQL_TIME lt;
  micros_to_time(us, &lt);
  f->store_time_dec(&lt, 6);
}

/* Visits every MooseDB table the user may see; f(db, table, info). */
template <typename F> int fill_moosedb_is(THD *thd, F f)
{
  /* Like InnoDB's I_S tables: storage internals need PROCESS. */
  if (check_global_access(thd, PROCESS_ACL, true))
    return 0;
  int result= 0;
  for_each_moosedb_table([&](const char *db, const char *tbl,
                              const std::string &path) {
    MooseDBInfo *raw= nullptr;
    if (moosedb_inspect(path.c_str(), &raw) != TF_OK)
    {
      push_warning_printf(thd, Sql_condition::WARN_LEVEL_WARN,
                          ER_UNKNOWN_ERROR, "MooseDB: %s.%s: %s", db, tbl,
                          client_error(path, std::string(db) + "." + tbl)
                            .c_str());
      return false;
    }
    std::unique_ptr<MooseDBInfo, tf_info_closer> info(raw);
    result= f(db, tbl, info.get());
    return result != 0;
  });
  return result;
}

namespace tf_show {

ST_FIELD_INFO tables_fields[]=
{
  Show::Column("TABLE_SCHEMA",      Show::Varchar(NAME_CHAR_LEN), NOT_NULL),
  Show::Column("TABLE_NAME",        Show::Varchar(NAME_CHAR_LEN), NOT_NULL),
  Show::Column("ROW_COUNT",         Show::ULonglong(),            NOT_NULL),
  Show::Column("PENDING_ROWS",      Show::ULonglong(),            NOT_NULL),
  Show::Column("SERIES",            Show::ULonglong(),            NOT_NULL),
  Show::Column("DATA_BYTES",        Show::ULonglong(),            NOT_NULL),
  Show::Column("COMPRESSED_BYTES",  Show::ULonglong(),            NOT_NULL),
  Show::Column("CHUNKS",            Show::ULong(),                NOT_NULL),
  Show::Column("HOT_CHUNKS",        Show::ULong(),                NOT_NULL),
  Show::Column("WARM_CHUNKS",       Show::ULong(),                NOT_NULL),
  Show::Column("COLD_CHUNKS",       Show::ULong(),                NOT_NULL),
  Show::Column("COMPACTING_CHUNKS", Show::ULong(),                NOT_NULL),
  Show::Column("EXPIRED_CHUNKS",    Show::ULong(),                NOT_NULL),
  Show::Column("RETENTION",         Show::Varchar(32),            NOT_NULL),
  Show::Column("CHUNK_INTERVAL",    Show::Varchar(32),            NOT_NULL),
  Show::Column("COMPRESSION",       Show::Varchar(8),             NOT_NULL),
  Show::Column("ENCRYPTED",         Show::Varchar(3),             NOT_NULL),
  Show::Column("IS_OPEN",           Show::Varchar(3),             NOT_NULL),
  Show::CEnd()
};

ST_FIELD_INFO chunks_fields[]=
{
  Show::Column("TABLE_SCHEMA",      Show::Varchar(NAME_CHAR_LEN), NOT_NULL),
  Show::Column("TABLE_NAME",        Show::Varchar(NAME_CHAR_LEN), NOT_NULL),
  Show::Column("CHUNK_ID",          Show::ULonglong(),            NOT_NULL),
  Show::Column("TS_MIN",            Show::Datetime(6),            NOT_NULL),
  Show::Column("TS_MAX",            Show::Datetime(6),            NOT_NULL),
  Show::Column("ROWS",              Show::ULonglong(),            NOT_NULL),
  Show::Column("SERIES",            Show::ULong(),                NOT_NULL),
  Show::Column("DATA_MB",           Show::Double(20),             NOT_NULL),
  Show::Column("COMPRESSED_MB",     Show::Double(20),             NOT_NULL),
  Show::Column("RATIO",             Show::Double(20),             NOT_NULL),
  Show::Column("STATUS",            Show::Varchar(16),            NOT_NULL),
  Show::Column("COMPRESSION",       Show::Varchar(8),             NOT_NULL),
  Show::Column("ENCRYPTED",         Show::Varchar(3),             NOT_NULL),
  Show::Column("SEALED_AT",         Show::Datetime(6),            NOT_NULL),
  Show::Column("CHUNK_FILE",        Show::Varchar(255),           NOT_NULL),
  Show::CEnd()
};

} // namespace tf_show

int tables_fill(THD *thd, TABLE_LIST *tables, Item *)
{
  TABLE *t= tables->table;
  return fill_moosedb_is(thd, [&](const char *db, const char *tbl,
                                   const MooseDBInfo *info) {
    const TFTableInfo *ti= moosedb_info_table(info);
    if (!ti)
      return 0;
    restore_record(t, s->default_values);
    Field **f= t->field;
    store_str(f[0], db);
    store_str(f[1], tbl);
    f[2]->store((longlong) ti->row_count, true);
    f[3]->store((longlong) ti->pending_rows, true);
    f[4]->store((longlong) ti->series_count, true);
    f[5]->store((longlong) ti->data_bytes, true);
    f[6]->store((longlong) ti->compressed_bytes, true);
    f[7]->store((longlong) ti->chunk_count, true);
    f[8]->store((longlong) ti->hot_chunks, true);
    f[9]->store((longlong) ti->warm_chunks, true);
    f[10]->store((longlong) ti->cold_chunks, true);
    f[11]->store((longlong) ti->compacting_chunks, true);
    f[12]->store((longlong) ti->expired_chunks, true);
    store_str(f[13], ti->retention_period);
    store_str(f[14], ti->chunk_interval);
    store_str(f[15], ti->compression);
    store_str(f[16], ti->encrypted ? "YES" : "NO");
    store_str(f[17], ti->is_open ? "YES" : "NO");
    return schema_table_store_record(thd, t) ? 1 : 0;
  });
}

int chunks_fill(THD *thd, TABLE_LIST *tables, Item *)
{
  TABLE *t= tables->table;
  return fill_moosedb_is(thd, [&](const char *db, const char *tbl,
                                   const MooseDBInfo *info) {
    const uint32_t n= moosedb_info_chunk_count(info);
    for (uint32_t i= 0; i < n; i++)
    {
      const TFChunkInfo *c= moosedb_info_chunk(info, i);
      if (!c)
        continue;
      restore_record(t, s->default_values);
      Field **f= t->field;
      constexpr double MB= 1024.0 * 1024.0;
      store_str(f[0], db);
      store_str(f[1], tbl);
      f[2]->store((longlong) c->chunk_id, true);
      store_micros(f[3], c->ts_min_us);
      store_micros(f[4], c->ts_max_us);
      f[5]->store((longlong) c->rows, true);
      f[6]->store((longlong) c->series, true);
      f[7]->store((double) c->data_bytes / MB);
      f[8]->store((double) c->compressed_bytes / MB);
      f[9]->store(c->data_bytes ? (double) c->compressed_bytes /
                                  (double) c->data_bytes : 0.0);
      store_str(f[10], c->status);
      store_str(f[11], c->compression);
      store_str(f[12], c->encrypted ? "YES" : "NO");
      store_micros(f[13], c->sealed_at_us);
      store_str(f[14], c->file_name);
      if (schema_table_store_record(thd, t))
        return 1;
    }
    return 0;
  });
}

int tables_init(void *p)
{
  auto *schema= static_cast<ST_SCHEMA_TABLE *>(p);
  schema->fields_info= tf_show::tables_fields;
  schema->fill_table= tables_fill;
  return 0;
}

int chunks_init(void *p)
{
  auto *schema= static_cast<ST_SCHEMA_TABLE *>(p);
  schema->fields_info= tf_show::chunks_fields;
  schema->fill_table= chunks_fill;
  return 0;
}

struct st_mysql_information_schema moosedb_is_info=
{ MYSQL_INFORMATION_SCHEMA_INTERFACE_VERSION };

} // namespace

/* ── UDFs behind CALL moosedb_compact / moosedb_apply_retention ─────── */

namespace {

/* 'YYYY-MM-DD[ HH:MM:SS[.ffffff]]' (UTC wall clock) → µs; '' → `dflt`. */
bool parse_bound(const char *s, size_t len, longlong dflt, longlong *out)
{
  std::string str(s ? s : "", s ? len : 0);
  if (str.empty())
  {
    *out= dflt;
    return false;
  }
  MYSQL_TIME lt;
  memset(&lt, 0, sizeof(lt));
  char frac[8]= "";
  int n= sscanf(str.c_str(), "%4u-%2u-%2u %2u:%2u:%2u.%6[0-9]", &lt.year,
                &lt.month, &lt.day, &lt.hour, &lt.minute, &lt.second, frac);
  if (n < 3 || lt.month < 1 || lt.month > 12 || lt.day < 1 || lt.day > 31 ||
      lt.hour > 23 || lt.minute > 59 || lt.second > 59)
    return true;
  if (n == 7)
  {
    std::string digits(frac);
    digits.resize(6, '0');
    lt.second_part= strtoul(digits.c_str(), nullptr, 10);
  }
  *out= time_to_micros(lt);
  return false;
}

/*
  Handle to an open MooseDB table, or NULL with an error raised.

  The caller's privilege `need` on db.table is checked FIRST, before anything
  reveals whether the table exists or is open (no oracle for other users'
  tables). The UDFs are callable directly, so they cannot rely on the SELECT
  check the procedures get from the statement they build.
*/
MooseDBTable *lookup_open_table(UDF_ARGS *args, privilege_t need,
                               std::string *path_out, std::string *label_out)
{
  THD *thd= current_thd;
  assert(thd);
  if (!thd)
  {
    my_printf_error(ER_UNKNOWN_ERROR, "MooseDB: no session", MYF(0));
    return nullptr;
  }
  if (!args->args[0] || !args->args[1])
  {
    my_printf_error(ER_UNKNOWN_ERROR, "MooseDB: database and table are required", MYF(0));
    return nullptr;
  }
  std::string db(args->args[0], args->lengths[0]);
  std::string tbl(args->args[1], args->lengths[1]);
  /* Embedded NULs would make the C-string views used below (privilege check,
     file name) disagree with the length, and trip asserts in debug builds. */
  if (db.empty() || tbl.empty() || db.size() > NAME_LEN ||
      tbl.size() > NAME_LEN || db.find('\0') != std::string::npos ||
      tbl.find('\0') != std::string::npos)
  {
    my_printf_error(ER_UNKNOWN_ERROR, "MooseDB: invalid database or table name",
                    MYF(0));
    return nullptr;
  }
  {
    LEX_CSTRING db_l= {db.c_str(), db.size()};
    LEX_CSTRING tbl_l= {tbl.c_str(), tbl.size()};
    TABLE_LIST tl;
    tl.init_one_table(&db_l, &tbl_l, nullptr, TL_READ);
    if (check_table_access(thd, need, &tl, false, 1, false))
      return nullptr;                    /* ER_TABLEACCESS_DENIED_ERROR */
  }
  char path[FN_REFLEN + 1];
  build_table_filename(path, sizeof(path) - 1, db.c_str(), tbl.c_str(), "", 0);
  *path_out= path;
  *label_out= db + "." + tbl;
  MooseDBTable *t= nullptr;
  if (moosedb_table_lookup(path, &t) != TF_OK)
  {
    take_last_error();
    my_printf_error(ER_UNKNOWN_ERROR,
                    "MooseDB: %s.%s is not an open MooseDB table (use the "
                    "moosedb_compact / moosedb_apply_retention procedures, "
                    "which open it)", MYF(0), db.c_str(), tbl.c_str());
    return nullptr;
  }
  return t;
}

bool init_string_args(UDF_INIT *initid, UDF_ARGS *args, uint n,
                      const char *usage, char *message)
{
  if (args->arg_count != n)
  {
    snprintf(message, MYSQL_ERRMSG_SIZE, "usage: %s", usage);
    return true;
  }
  for (uint i= 0; i < n; i++)
    args->arg_type[i]= STRING_RESULT;
  initid->maybe_null= false;
  return false;
}

} // namespace

extern "C" {

my_bool moosedb_compact_impl_init(UDF_INIT *initid, UDF_ARGS *args,
                                char *message)
{
  return init_string_args(initid, args, 4,
                          "moosedb_compact_impl(db, table, from, to)", message);
}

long long moosedb_compact_impl(UDF_INIT *, UDF_ARGS *args, char *,
                                char *error)
{
  /* Privilege first (see lookup_open_table), then argument validation. */
  std::string path, label;
  tf_table_ptr t(lookup_open_table(args, ALTER_ACL, &path, &label));
  if (!t)
  {
    *error= 1;
    return 0;
  }
  longlong lo, hi;
  if (parse_bound(args->args[2], args->lengths[2], LONGLONG_MIN, &lo) ||
      parse_bound(args->args[3], args->lengths[3], LONGLONG_MAX, &hi))
  {
    my_printf_error(ER_UNKNOWN_ERROR,
                    "MooseDB: bounds must be 'YYYY-MM-DD[ HH:MM:SS[.ffffff]]'",
                    MYF(0));
    *error= 1;
    return 0;
  }
  if (moosedb_compact(t.get(), lo, hi) != TF_OK)
  {
    my_printf_error(ER_UNKNOWN_ERROR, "MooseDB: %s", MYF(0),
                    client_error(path, label).c_str());
    *error= 1;
    return 0;
  }
  return 1;
}

my_bool moosedb_retention_impl_init(UDF_INIT *initid, UDF_ARGS *args,
                                  char *message)
{
  return init_string_args(initid, args, 2,
                          "moosedb_retention_impl(db, table)", message);
}

long long moosedb_retention_impl(UDF_INIT *, UDF_ARGS *args, char *,
                                  char *error)
{
  std::string path, label;
  tf_table_ptr t(lookup_open_table(args, DELETE_ACL, &path, &label));
  if (!t)
  {
    *error= 1;
    return 0;
  }
  if (moosedb_apply_retention(t.get()) != TF_OK)
  {
    my_printf_error(ER_UNKNOWN_ERROR, "MooseDB: %s", MYF(0),
                    client_error(path, label).c_str());
    *error= 1;
    return 0;
  }
  return 1;
}

} // extern "C"

/* ── Plugin registration ──────────────────────────────────────────────── */

static handler *moosedb_create_handler(handlerton *hton, TABLE_SHARE *table,
                                        MEM_ROOT *mem_root)
{
  return new (mem_root) ha_moosedb(hton, table);
}

/* Table data lives in a directory, not in files with extensions. */
static const char *moosedb_exts[]= {NullS};

/*
  mariadb-backup cannot copy MooseDB tables (it skips table subdirectories),
  yet exits 0. prepare_for_backup/end_backup are void, but they tell us a
  BACKUP STAGE is active; mariadb-backup then runs FLUSH ENGINE LOGS
  (die_on_error) in BLOCK_COMMIT, and flush_logs may fail. So fail it only
  while a backup is active and a MooseDB table exists. --no-lock skips that
  statement and stays unprotected (documented).
*/
static std::atomic<bool> backup_active{false};

static void moosedb_prepare_for_backup() { backup_active.store(true); }
static void moosedb_end_backup() { backup_active.store(false); }

static bool moosedb_flush_logs(handlerton *)
{
  if (!backup_active.load())
    return false;
  bool found= false;
  for_each_moosedb_table([&](const char *, const char *, const std::string &) {
    found= true;
    return true;
  });
  if (!found)
    return false;
  my_printf_error(ER_UNKNOWN_ERROR,
                  "MooseDB tables are not copied by mariadb-backup; use "
                  "mariadb-dump --lock-all-tables (see the user guide, Backup)",
                  MYF(0));
  return true;
}

static int moosedb_init_func(void *p)
{
  DBUG_ENTER("moosedb_init_func");
  moosedb_hton= static_cast<handlerton *>(p);
  moosedb_hton->create= moosedb_create_handler;
  /*
    ha_partition moves rows between partitions with copy + delete_row(), and
    MooseDB is append-only: ADD/COALESCE/REORGANIZE PARTITION would
    duplicate or lose rows. Refuse PARTITION BY altogether.
  */
  moosedb_hton->flags= HTON_NO_PARTITION;
  moosedb_hton->table_options= moosedb_table_option_list;
  moosedb_hton->tablefile_extensions= moosedb_exts;
  moosedb_hton->prepare_for_backup= moosedb_prepare_for_backup;
  moosedb_hton->end_backup= moosedb_end_backup;
  moosedb_hton->flush_logs= moosedb_flush_logs;
  moosedb_hton->create_group_by= moosedb_create_group_by;
  push_globals();
  if (moosedb_set_key_callback(moosedb_key_callback) != TF_OK)
  {
    sql_print_error("MooseDB: cannot register the key callback: %s",
                    take_last_error().c_str());
    DBUG_RETURN(1);
  }
  if (moosedb_maintenance_start(srv_compaction_threads) != TF_OK)
  {
    sql_print_error("MooseDB: cannot start the maintenance threads: %s",
                    take_last_error().c_str());
    moosedb_set_key_callback(nullptr);
    take_last_error();
    DBUG_RETURN(1);
  }
  DBUG_RETURN(0);
}

static int moosedb_deinit_func(void *)
{
  moosedb_maintenance_stop();
  moosedb_set_key_callback(nullptr);
  return 0;
}

static struct st_mysql_storage_engine moosedb_storage_engine=
{ MYSQL_HANDLERTON_INTERFACE_VERSION };

maria_declare_plugin(moosedb)
{
  MYSQL_STORAGE_ENGINE_PLUGIN,
  &moosedb_storage_engine,
  "MooseDB",
  "Kevenny",
  "Time-series storage engine: append-only, chunked by time, compressed, WAL-backed",
  PLUGIN_LICENSE_GPL,
  moosedb_init_func,                    /* Plugin Init   */
  moosedb_deinit_func,                  /* Plugin Deinit */
  0x0002,                                /* version 0.2   */
  moosedb_status_variables,             /* status vars   */
  moosedb_system_variables,             /* system vars   */
  "0.2.0",                               /* string version */
  MariaDB_PLUGIN_MATURITY_EXPERIMENTAL   /* maturity      */
},
{
  MYSQL_INFORMATION_SCHEMA_PLUGIN,
  &moosedb_is_info,
  "MOOSEDB_TABLES",
  "Kevenny",
  "MooseDB tables: rows, sizes, chunk states",
  PLUGIN_LICENSE_GPL,
  tables_init,
  NULL,
  0x0002,
  NULL,
  NULL,
  "0.2.0",
  MariaDB_PLUGIN_MATURITY_EXPERIMENTAL
},
{
  MYSQL_INFORMATION_SCHEMA_PLUGIN,
  &moosedb_is_info,
  "MOOSEDB_CHUNKS",
  "Kevenny",
  "MooseDB chunks: time range, size, compression, status",
  PLUGIN_LICENSE_GPL,
  chunks_init,
  NULL,
  0x0002,
  NULL,
  NULL,
  "0.2.0",
  MariaDB_PLUGIN_MATURITY_EXPERIMENTAL
}
maria_declare_plugin_end;
