/*
  MooseDB storage engine — time-series storage for MariaDB.

  Copyright (c) 2026 Kevenny

  This program is free software; you can redistribute it and/or modify
  it under the terms of the GNU General Public License as published by
  the Free Software Foundation; version 2 of the License.
*/

#define MYSQL_SERVER 1

#include "ha_moosedb.h"

#include <algorithm>
#include <cctype>
#include <climits>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <filesystem>
#include <iterator>
#include <system_error>

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

static struct st_mysql_sys_var *moosedb_system_variables[]= {
  MYSQL_SYSVAR(wal_sync_mode),
  MYSQL_SYSVAR(memtable_flush_threshold),
  MYSQL_SYSVAR(compaction_trigger_chunks),
  MYSQL_SYSVAR(compaction_threads),
  MYSQL_SYSVAR(retention_check_interval),
  MYSQL_SYSVAR(bloom_filter_false_positive_rate),
  MYSQL_SYSVAR(chunk_cache_size),
  MYSQL_SYSVAR(max_open_chunks),
  MYSQL_SYSVAR(batch_memory_budget),
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

  /* Indexes: only plain, single-part indexes on the timestamp column. */
  for (uint k= 0; k < s->keys; k++)
  {
    const KEY &key= s->key_info[k];
    if (key.user_defined_key_parts != 1 ||
        key.key_part[0].field != ts_field)
    {
      *err= std::string("index '") + key.name.str +
            "' is not supported: MooseDB only indexes the timestamp column "
            "(TAG columns are indexed implicitly)";
      return true;
    }
    if (key.flags & HA_NOSAME)
    {
      *err= std::string("index '") + key.name.str +
            "' cannot be UNIQUE/PRIMARY: time-series rows are not unique by time";
      return true;
    }
    l.ts_key= k;
  }
  if (l.ts_key == MAX_KEY)
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
  *out= std::move(l);
  return false;
}

/* Owns the strings a TFTableConfig points into. */
struct tf_config_holder
{
  std::vector<const char *> names;
  std::vector<uint8_t> types;
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
  if (batch_)
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

int ha_moosedb::write_row(const uchar *buf)
{
  DBUG_ENTER("ha_moosedb::write_row");
  assert(table);
  if (!table || !buf || !share_ || !share_->table)
    DBUG_RETURN(HA_ERR_INTERNAL_ERROR);

  const tf_layout &l= share_->layout;
  if (values_.size() < l.columns.size() || str_bufs_.size() < l.columns.size())
    DBUG_RETURN(HA_ERR_INTERNAL_ERROR);
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
    DBUG_RETURN(error);

  TFRow row{(uint32_t) values_.size(), values_.data()};
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
  if (!batch_ && !in_bulk_insert_ &&
      thd->locked_tables_mode == LTM_LOCK_TABLES)
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
  }
  error= map_status(moosedb_batch_write(batch_.get(), &row));
  DBUG_RETURN(error);
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
      const bool memo= c.conv == tf_conv::TAG &&
                       (rt == MYSQL_TYPE_VARCHAR || rt == MYSQL_TYPE_STRING) &&
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

int ha_moosedb::commit_batch()
{
  if (!batch_)
    return 0;
  /* commit always releases the handle, also on error */
  MooseDBBatch *b= batch_.release();
  int error= map_status(
    moosedb_batch_commit(b, srv_wal_sync_mode == WAL_SYNC_FSYNC));
  if (error)
  {
    sql_print_error("MooseDB: statement commit failed: %s",
                    last_error_.c_str());
  }
  return error;
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
        cmd != SQLCOM_OPTIMIZE && cmd != SQLCOM_DELETE &&
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

const COND *ha_moosedb::cond_push(const COND *cond)
{
  std::vector<tf_tag_predicate> preds;
  if (share_ && cond)
    collect_tag_predicates(cond, &preds);
  pushed_.push_back(std::move(preds));
  pushed_valid_= false;
  /* The server keeps evaluating the full condition: we only prune series. */
  return cond;
}

void ha_moosedb::cond_pop()
{
  if (!pushed_.empty())
    pushed_.pop_back();
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
  if (!in_bulk_insert_ && batch_)
  {
    error= commit_batch();
    /* ha_reset()'s return value is ignored by the server: report it here */
    if (error)
      print_error(error, MYF(0));
  }
  pushed_.clear();
  pushed_valid_= false;
  scan_.reset();
  reset_batch_state();
  snapshots_.clear();
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

  if (!share_ || !share_->table)
  {
    pushed_filter_= false;
    return false;
  }
  assert(table);
  if (!table)
  {
    pushed_filter_= false;
    return false;
  }
  /* Character sets of the TAG columns, in TAG order. */
  std::vector<CHARSET_INFO *> tag_cs;
  for (const tf_column &c : share_->layout.columns)
    if (c.conv == tf_conv::TAG)
    {
      if (c.field_index >= table->s->fields || !table->field[c.field_index])
      {
        pushed_filter_= false;
        return false;
      }
      /* Integer TAGs are matched on their decimal text, byte for byte. */
      tag_cs.push_back(is_int_tag_field(table->field[c.field_index])
                         ? &my_charset_bin
                         : table->field[c.field_index]->charset());
    }
  for (const tf_tag_predicate *p : preds)
    if (p->tag_pos < 0 || (size_t) p->tag_pos >= tag_cs.size() || !p->cs)
    {
      pushed_filter_= false;
      return false;
    }

  if (pushed_series_cached(preds, tag_cs))
    return true;

  /* Fallback: compare every series (also used past the cache limits). */
  MooseDBSeriesList *list= nullptr;
  if (moosedb_series_list(share_->table.get(), &list) != TF_OK)
  {
    sql_print_warning("MooseDB: TAG pushdown disabled: %s",
                      take_last_error().c_str());
    pushed_filter_= false;
    return false;
  }
  const bool ok= pushed_series_scan(list, preds, tag_cs);
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
    const std::vector<CHARSET_INFO *> &tag_cs)
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
      pushed_ids_.push_back(id);
  }
  std::sort(pushed_ids_.begin(), pushed_ids_.end());
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
    const std::vector<CHARSET_INFO *> &tag_cs)
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
  pushed_ids_= std::move(result);
  return true;
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
  if (!thd)
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
  for (size_t i= 0; i < l.columns.size(); i++)
  {
    const uint fi= l.columns[i].field_index;
    if (fi >= table->s->fields)
      return;
    if (bitmap_is_set(table->read_set, fi) ||
        (index_scan && i == l.ts_column))
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
    error= map_status(moosedb_scan_open_projected(
        share_->table.get(), lo, hi, filtered ? ids->data() : nullptr,
        filtered ? (int64_t) ids->size() : -1, sorted,
        proj_all_ ? nullptr : proj_mask_.data(),
        proj_all_ ? 0 : (uint32_t) proj_mask_.size(), &s));
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
  DBUG_RETURN(scan ? open_scan(LONGLONG_MIN, LONGLONG_MAX, false, false) : 0);
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
  null_template_.assign(nb, 0);
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

int ha_moosedb::index_init(uint idx, bool)
{
  DBUG_ENTER("ha_moosedb::index_init");
  active_index= idx;
  scan_.reset();
  reset_batch_state();
  DBUG_RETURN(0);
}

int ha_moosedb::index_end()
{
  DBUG_ENTER("ha_moosedb::index_end");
  scan_.reset();
  reset_batch_state();
  active_index= MAX_KEY;
  DBUG_RETURN(0);
}

int ha_moosedb::key_to_ts(uint idx, const uchar *key, longlong *out)
{
  assert(table && key);
  if (!table || !key || !out || !table->key_info || idx >= table->s->keys)
    return HA_ERR_INTERNAL_ERROR;
  KEY *ki= &table->key_info[idx];
  Field *f= ki->key_part ? ki->key_part[0].field : nullptr;
  if (!f)
    return HA_ERR_INTERNAL_ERROR;
  /* Decode through the field itself, using record[1] as scratch space. */
  key_restore(table->record[1], key, ki, ki->key_part[0].store_length);
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
  longlong ts;
  if (int error= key_to_ts(active_index, key, &ts))
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
    if (int error= key_to_ts(active_index, end_range->key, &end_ts))
      DBUG_RETURN(error);
    hi= std::min(hi, end_ts);
  }
  DBUG_RETURN(open_range_scan(lo, hi, backward, buf, HA_ERR_KEY_NOT_FOUND));
}

int ha_moosedb::index_next(uchar *buf)
{
  DBUG_ENTER("ha_moosedb::index_next");
  DBUG_RETURN(read_from_scan(buf, false, HA_ERR_END_OF_FILE));
}

int ha_moosedb::index_prev(uchar *buf)
{
  DBUG_ENTER("ha_moosedb::index_prev");
  DBUG_RETURN(read_from_scan(buf, true, HA_ERR_END_OF_FILE));
}

int ha_moosedb::index_first(uchar *buf)
{
  DBUG_ENTER("ha_moosedb::index_first");
  DBUG_RETURN(open_range_scan(LONGLONG_MIN, LONGLONG_MAX, false, buf,
                              HA_ERR_END_OF_FILE));
}

int ha_moosedb::index_last(uchar *buf)
{
  DBUG_ENTER("ha_moosedb::index_last");
  DBUG_RETURN(open_range_scan(LONGLONG_MIN, LONGLONG_MAX, true, buf,
                              HA_ERR_END_OF_FILE));
}

/* ── Statistics ───────────────────────────────────────────────────────── */

int ha_moosedb::info(uint flag)
{
  DBUG_ENTER("ha_moosedb::info");
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
  }
  DBUG_RETURN(0);
}

ha_rows ha_moosedb::records_in_range(uint inx, const key_range *min_key,
                                      const key_range *max_key, page_range *)
{
  DBUG_ENTER("ha_moosedb::records_in_range");
  if (!table || !share_ || !share_->table || inx >= table->s->keys)
    DBUG_RETURN(HA_POS_ERROR);
  if ((min_key && !min_key->key) || (max_key && !max_key->key))
    DBUG_RETURN(HA_POS_ERROR);
  longlong lo= LONGLONG_MIN, hi= LONGLONG_MAX;
  if (min_key)
  {
    if (key_to_ts(inx, min_key->key, &lo))
      DBUG_RETURN(HA_POS_ERROR);
    if (min_key->flag == HA_READ_AFTER_KEY && lo < LONGLONG_MAX)
      lo++;
  }
  if (max_key)
  {
    if (key_to_ts(inx, max_key->key, &hi))
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
  NULL,                                  /* status vars   */
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
