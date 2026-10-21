/*
  MooseDB storage engine -- group_by_handler: GROUP BY done by the engine.

  create_group_by() recognises the queries the engine can aggregate
  (analyze()) and, with moosedb_group_by_pushdown=ON, takes them over;
  everything else returns nullptr and runs the normal way. With
  moosedb_gbh_debug=ON the verdict is reported as a note ("gbh: accepted" /
  "gbh: refused: <reason>") so it can be tested.

  Protocol facts this file relies on (measured on 11.4.13, docs/architecture.md):
    - the server never sorts above the handler: we clear Query::group_by and
      Query::order_by and deliver rows already ordered (ORDER BY, top-N);
      LIMIT/OFFSET and HAVING are applied by the server on what we emit;
    - the tmp table has one field per non-constant item of all_fields, in
      order, the n_aux hidden items first; every field must be filled;
    - create_group_by runs also for EXPLAIN (no init_scan): it opens nothing;
    - the server frees the handler with `delete` (~Pushdown_query);
    - FOUND_ROWS() is left stale by the server: end_scan() sets it.

  Copyright (c) 2026 Kevenny

  This program is free software; you can redistribute it and/or modify
  it under the terms of the GNU General Public License as published by
  the Free Software Foundation; version 2 of the License.
*/

#define MYSQL_SERVER 1

#include <algorithm>
#include <cerrno>
#include <cstring>
#include <memory>
#include <numeric>
#include <string>
#include <vector>

#include "moosedb_gbh.h"

#include "ha_moosedb.h"

#include "my_global.h"
#include "sql_class.h"
#include "sql_select.h"
#include "item_cmpfunc.h"
#include "item_sum.h"
#include "sql_lex.h"
#include "my_decimal.h"

namespace {

constexpr longlong MICROS_PER_SEC_G= 1000000LL;

bool is_int_field(const Field *f)
{
  if (!f)
    return false;
  switch (f->type())
  {
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

/* Unsigned BIGINT does not fit the core's signed 64-bit aggregates. */
bool is_ubigint(const Field *f)
{
  return f && f->type() == MYSQL_TYPE_LONGLONG && f->is_unsigned();
}

longlong floor_div_g(longlong a, longlong b)
{
  return a / b - ((a % b != 0) && ((a < 0) != (b < 0)));
}

/* A collation under which equal strings have equal bytes. */
bool cs_exact(const CHARSET_INFO *cs)
{
  return cs == &my_charset_bin ||
         ((cs->state & MY_CS_BINSORT) && (cs->state & MY_CS_NOPAD));
}

/* The column `item` is, or nullptr (not a column of `table`). */
Field *column_of(Item *item, const TABLE *table)
{
  if (!item)
    return nullptr;
  Item *real= item->real_item();
  if (!real || real->type() != Item::FIELD_ITEM)
    return nullptr;
  Field *f= static_cast<Item_field *>(real)->field;
  return (f && f->table == table) ? f : nullptr;
}

/* A value that does not change during the statement: literal, parameter, cache. */
bool is_constant(const Item *item)
{
  return item && item->const_item() && !item->with_subquery() &&
         !item->with_sum_func();
}

/* ── The plan ─────────────────────────────────────────────────────────── */

struct gkey
{
  Field *field= nullptr;
  bool is_tag= false;
  int tag_pos= -1;
  uint32_t col= 0;                 /* layout column (non-TAG keys)          */
  CHARSET_INFO *cs= nullptr;       /* strings                               */
  bool is_str= false;
  bool is_unsigned= false;
  int res= -1;                     /* key index in the core's result        */
};

enum class aggk : uint8_t { COUNT_STAR, COUNT, SUM, AVG, MIN, MAX };

struct gagg
{
  Item_sum *item= nullptr;
  aggk k= aggk::COUNT_STAR;
  int func= 0;                     /* index into plan.funcs                 */
  tf_conv conv= tf_conv::INT;      /* MIN/MAX: INT, DATETIME or INT_SECS    */
  bool is_unsigned= false;
  uint prec_inc= 0;                /* AVG                                   */
};

enum class ikind : uint8_t { CONST, KEY, AGG };
struct gitem
{
  ikind k= ikind::CONST;
  int idx= -1;
  Item_field *field_item= nullptr;   /* KEY: the item whose result_field is its tmp field */
};

struct gorder
{
  bool is_key= false;
  int idx= 0;
  bool desc= false;
};

struct gplan
{
  ha_moosedb *handler= nullptr;
  TABLE *table= nullptr;
  std::vector<gkey> keys;
  bool has_series= false;
  std::vector<TFAggFunc> funcs;
  std::vector<gagg> aggs;
  std::vector<gitem> items;        /* parallel to all_fields               */
  std::vector<gorder> order;
  std::vector<tf_tag_predicate> preds;
  longlong lo= LONGLONG_MIN, hi= LONGLONG_MAX;
  ulonglong limit= HA_POS_ERROR;
  bool has_having= false;
  bool need_merge= false;

  bool in_group(const Field *f) const
  {
    for (const gkey &k : keys)
      if (k.field == f)
        return true;
    return false;
  }
  int key_of(const Field *f) const
  {
    for (size_t i= 0; i < keys.size(); i++)
      if (keys[i].field == f)
        return (int) i;
    return -1;
  }
  bool stored(const Field *f) const
  {
    tf_conv c;
    return f && handler && handler->moosedb_column_conv(f, &c);
  }
  int layout_col(const Field *f) const
  {
    const tf_layout *l= handler ? handler->moosedb_layout() : nullptr;
    if (!l || !f)
      return -1;
    for (size_t i= 0; i < l->columns.size(); i++)
      if (l->columns[i].field_index == f->field_index)
        return (int) i;
    return -1;
  }
  int add_func(uint8_t op, uint32_t col)
  {
    for (size_t i= 0; i < funcs.size(); i++)
      if (funcs[i].op == op && funcs[i].col == col)
        return (int) i;
    funcs.push_back(TFAggFunc{op, col});
    return (int) funcs.size() - 1;
  }
};

const char *group_by_refusal(gplan *p, ORDER *group_by)
{
  for (ORDER *o= group_by; o; o= o->next)
  {
    if (!o->item)
      return "GROUP BY: not a column";
    Field *f= column_of(*o->item, p->table);
    if (!f)
      return "GROUP BY: not a column";
    tf_conv c;
    if (!p->handler->moosedb_column_conv(f, &c))
      return "GROUP BY: unsupported column type";
    gkey k;
    k.field= f;
    k.is_unsigned= f->is_unsigned();
    if (c == tf_conv::TAG)
    {
      if (!is_int_field(f) && f->cmp_type() != STRING_RESULT)
        return "GROUP BY: unsupported column type";
      const tf_layout *l= p->handler->moosedb_layout();
      k.is_tag= true;
      k.tag_pos= l ? l->tag_position(f->field_index) : -1;
      if (k.tag_pos < 0)
        return "GROUP BY: unsupported column type";
      k.is_str= !is_int_field(f);
      p->has_series= true;
    }
    else if (c == tf_conv::INT && is_int_field(f))
    {
      k.col= (uint32_t) p->layout_col(f);
    }
    else if (c == tf_conv::STRING && f->type() == MYSQL_TYPE_VARCHAR)
    {
      k.col= (uint32_t) p->layout_col(f);
      k.is_str= true;
    }
    else
      return "GROUP BY: unsupported column type";
    if (k.is_str)
      k.cs= f->charset();
    if (p->key_of(f) < 0)
      p->keys.push_back(k);
  }
  /* Result key layout: the series (if any), then the column keys. */
  int next= p->has_series ? 1 : 0;
  for (gkey &k : p->keys)
    k.res= k.is_tag ? 0 : next++;
  return nullptr;
}

/* Registers an aggregate of the plan; returns its index through *out. */
const char *agg_refusal(gplan *p, Item_sum *s, int *out)
{
  if (!s)
    return "aggregate: unsupported function";
  for (size_t i= 0; i < p->aggs.size(); i++)
    if (p->aggs[i].item == s)
    {
      *out= (int) i;
      return nullptr;
    }
  const uint nargs= s->get_arg_count();
  gagg a;
  a.item= s;
  switch (s->sum_func())
  {
  case Item_sum::COUNT_FUNC:
  {
    if (nargs != 1)
      return "aggregate: unsupported argument";
    Item *arg= s->get_arg(0);
    if (!arg)
      return "aggregate: unsupported argument";
    if (arg->const_item())                       /* COUNT(*), COUNT(1) */
    {
      /* a constant that evaluates to NULL counts nothing: leave it to the server */
      if (arg->type() == Item::NULL_ITEM || arg->is_expensive() || arg->is_null())
        return "aggregate: unsupported argument";
      a.k= aggk::COUNT_STAR;
      a.func= p->add_func(TF_AGG_COUNT_STAR, 0);
      break;
    }
    Field *f= column_of(arg, p->table);
    const int col= p->layout_col(f);
    tf_conv cc;
    if (!f || col < 0 || !p->handler->moosedb_column_conv(f, &cc))
      return "aggregate: unsupported argument";
    /* the core refuses COUNT of a TAG column */
    if (cc == tf_conv::TAG)
      return "aggregate: COUNT of a TAG column";
    a.k= aggk::COUNT;
    a.func= p->add_func(TF_AGG_COUNT, (uint32_t) col);
    break;
  }
  case Item_sum::SUM_FUNC:
  case Item_sum::AVG_FUNC:
  {
    if (nargs != 1)
      return "aggregate: unsupported argument";
    Field *f= column_of(s->get_arg(0), p->table);
    tf_conv c;
    if (!f || !p->handler->moosedb_column_conv(f, &c) || c != tf_conv::INT ||
        !is_int_field(f) || is_ubigint(f))
      return "aggregate: unsupported argument type";
    a.k= s->sum_func() == Item_sum::SUM_FUNC ? aggk::SUM : aggk::AVG;
    a.func= p->add_func(TF_AGG_SUM, (uint32_t) p->layout_col(f));
    if (a.k == aggk::AVG)
      a.prec_inc= static_cast<Item_sum_avg *>(s)->prec_increment;
    break;
  }
  case Item_sum::MIN_FUNC:
  case Item_sum::MAX_FUNC:
  {
    if (nargs != 1)
      return "aggregate: unsupported argument";
    Field *f= column_of(s->get_arg(0), p->table);
    tf_conv c;
    if (!f || !p->handler->moosedb_column_conv(f, &c))
      return "aggregate: unsupported argument type";
    if (p->handler->moosedb_is_timestamp_column(f))
    {
      if (c != tf_conv::DATETIME && c != tf_conv::INT_SECS)
        return "aggregate: unsupported argument type";
    }
    else if (!(c == tf_conv::INT && is_int_field(f) && !is_ubigint(f)))
      return "aggregate: unsupported argument type";
    a.conv= c;
    a.is_unsigned= f->is_unsigned();
    a.k= s->sum_func() == Item_sum::MIN_FUNC ? aggk::MIN : aggk::MAX;
    a.func= p->add_func(a.k == aggk::MIN ? TF_AGG_MIN : TF_AGG_MAX,
                        (uint32_t) p->layout_col(f));
    break;
  }
  default:
    return "aggregate: unsupported function";   /* DISTINCT, GROUP_CONCAT, STD... */
  }
  p->aggs.push_back(a);
  *out= (int) p->aggs.size() - 1;
  return nullptr;
}

/* A select-list or ORDER BY item: records what it is in `*out`. */
const char *item_refusal(gplan *p, Item *item, const char *expr_reason,
                         gitem *out)
{
  if (!item)
    return expr_reason;
  Item *real= item->real_item();
  if (!real)
    return expr_reason;
  switch (real->type())
  {
  case Item::SUM_FUNC_ITEM:
  {
    int idx= -1;
    const char *r= agg_refusal(p, static_cast<Item_sum *>(real), &idx);
    if (r)
      return r;
    out->k= ikind::AGG;
    out->idx= idx;
    return nullptr;
  }
  case Item::WINDOW_FUNC_ITEM:
    return "window function";
  case Item::FIELD_ITEM:
  {
    Field *f= column_of(real, p->table);
    const int k= f ? p->key_of(f) : -1;
    if (k < 0)
      return "column not in GROUP BY";
    out->k= ikind::KEY;
    out->idx= k;
    out->field_item= static_cast<Item_field *>(real);
    return nullptr;
  }
  default:
    if (!is_constant(real))
      return expr_reason;
    out->k= ikind::CONST;
    return nullptr;
  }
}

/* column OP constant, with the column a stored column of the table. */
const char *field_vs_const(const gplan *p, Item *field, Item *c)
{
  if (!p->stored(column_of(field, p->table)))
    return "WHERE: unsupported predicate";
  return is_constant(c) ? nullptr : "WHERE: unsupported predicate";
}

/* Shape check of the WHERE: a conjunction of column-vs-constant predicates. */
const char *where_refusal(const gplan *p, Item *cond)
{
  if (!cond)
    return nullptr;
  if (cond->type() == Item::COND_ITEM)
  {
    Item_cond *c= static_cast<Item_cond *>(cond);
    if (c->functype() != Item_func::COND_AND_FUNC)
      return "WHERE: OR/XOR";
    List_iterator_fast<Item> li(*c->argument_list());
    Item *a;
    while ((a= li++))
    {
      const char *r= where_refusal(p, a);
      if (r)
        return r;
    }
    return nullptr;
  }
  if (cond->type() != Item::FUNC_ITEM)
    return "WHERE: unsupported expression";
  Item_func *f= static_cast<Item_func *>(cond);
  Item **a= f->arguments();
  const uint n= f->argument_count();
  if (!a)
    return "WHERE: unsupported expression";
  switch (f->functype())
  {
  case Item_func::EQ_FUNC:
  case Item_func::EQUAL_FUNC:
  case Item_func::NE_FUNC:
  case Item_func::LT_FUNC:
  case Item_func::LE_FUNC:
  case Item_func::GE_FUNC:
  case Item_func::GT_FUNC:
  {
    if (n != 2)
      return "WHERE: unsupported predicate";
    if (column_of(a[0], p->table))
      return field_vs_const(p, a[0], a[1]);
    return field_vs_const(p, a[1], a[0]);        /* constant OP column */
  }
  case Item_func::BETWEEN:
  {
    if (n != 3)
      return "WHERE: unsupported predicate";
    const char *r= field_vs_const(p, a[0], a[1]);
    return r ? r : field_vs_const(p, a[0], a[2]);
  }
  case Item_func::IN_FUNC:
  case Item_func::LIKE_FUNC:
  {
    if (n < 2)
      return "WHERE: unsupported predicate";
    for (uint i= 1; i < n; i++)
    {
      const char *r= field_vs_const(p, a[0], a[i]);
      if (r)
        return r;
    }
    return nullptr;
  }
  case Item_func::ISNULL_FUNC:
  case Item_func::ISNOTNULL_FUNC:
    return (n == 1 && p->stored(column_of(a[0], p->table)))
           ? nullptr : "WHERE: unsupported predicate";
  case Item_func::MULT_EQUAL_FUNC:
    return nullptr;                      /* judged by the exact capture below */
  default:
    return "WHERE: unsupported predicate";
  }
}

/*
  Scope analysis and plan. nullptr = the engine can aggregate the query
  (`p` is the plan); otherwise a static string with the reason it cannot.
*/
const char *analyze(THD *thd, Query *q, gplan *p)
{
  if (!thd || !q || !q->select)
    return "internal: no query";
  LEX *lex= thd->lex;
  st_select_lex *sl= lex ? lex->current_select : nullptr;
  if (!sl || !sl->master_unit())
    return "internal: no select";

  /* Where the SELECT sits. */
  if (lex->sql_command != SQLCOM_SELECT)
    return "not a top-level SELECT";
  if (thd->spcont || thd->in_sub_stmt)
    return "inside a stored routine or trigger";
  if (lex->unit.is_unit_op())
    return "UNION/INTERSECT/EXCEPT";
  if (sl->master_unit()->item)
    return "subquery";
  if (sl->master_unit()->derived)
    return "derived table, view or CTE";
  if (sl->master_unit() != &lex->unit)
    return "not a top-level SELECT";

  /* The table. */
  TABLE_LIST *tl= q->from;
  if (!tl)
    return "internal: no table";
  if (tl->next_local)
    return "more than one table";
  if (tl->view || tl->derived || !tl->table)
    return "derived table, view or CTE";
  p->handler= moosedb_handler_of(tl->table->file);
  p->table= tl->table;
  if (!p->handler)
    return "not a MooseDB table";
  if (!p->handler->moosedb_layout() || !p->handler->moosedb_table_handle())
    return "internal: table not open";

  /* Clauses the engine does not do. */
  if (!q->group_by)
    return "no GROUP BY";
  if (q->distinct)
    return "DISTINCT";
  if (sl->olap != UNSPECIFIED_OLAP_TYPE)
    return "ROLLUP";
  if (sl->window_funcs.elements)
    return "window function";
  if (sl->limit_params.with_ties)
    return "WITH TIES";
  /* The server leaves FOUND_ROWS() stale when a group_by_handler runs. */
  if (sl->options & OPTION_FOUND_ROWS)
    return "SQL_CALC_FOUND_ROWS";

  const char *r= group_by_refusal(p, q->group_by);
  if (r)
    return r;

  /* SELECT list, with the hidden items (GROUP BY columns, HAVING and ORDER BY
     aggregates, a NULL for ORDER BY NULL) the server adds in front. */
  List_iterator_fast<Item> li(*q->select);
  Item *it;
  while ((it= li++))
  {
    gitem gi;
    r= item_refusal(p, it, "select list: expression", &gi);
    if (r)
      return r;
    p->items.push_back(gi);
  }

  r= where_refusal(p, q->where);
  if (r)
    return r;
  if (q->where &&
      !p->handler->moosedb_gbh_where(q->where, &p->preds, &p->lo, &p->hi))
    return "WHERE: only TAG equality/IN and time bounds";

  for (ORDER *o= q->order_by; o; o= o->next)
  {
    gitem gi;
    r= item_refusal(p, o->item ? *o->item : nullptr, "ORDER BY: expression",
                    &gi);
    if (r)
      return r;
    if (gi.k == ikind::CONST)
      continue;
    gorder t;
    t.is_key= gi.k == ikind::KEY;
    t.idx= gi.idx;
    t.desc= o->direction == ORDER::ORDER_DESC;
    p->order.push_back(t);
  }

  p->limit= q->limit ? q->limit->get_select_limit() : HA_POS_ERROR;
  p->has_having= q->having != nullptr;
  p->need_merge= p->has_series;
  for (const gkey &k : p->keys)
    if (!k.is_tag && k.is_str && !cs_exact(k.cs))
      p->need_merge= true;
  return nullptr;
}

/* ── The executor ─────────────────────────────────────────────────────── */

struct agg_closer
{
  void operator()(MooseDBAgg *a) const noexcept { moosedb_agg_close(a); }
};

struct kcell                /* one key value of one group */
{
  int64_t v= 0;             /* integer value, or arena offset of a string */
  uint32_t len= 0;
  uint8_t null= 0;
};

struct acell                /* one engine function of one group */
{
  uint64_t count= 0;
  __int128 sum= 0;
  int64_t val= 0;
};

std::string i128_to_string(__int128 v)
{
  if (v == 0)
    return "0";
  const bool neg= v < 0;
  unsigned __int128 u= neg ? (unsigned __int128) 0 - (unsigned __int128) v
                           : (unsigned __int128) v;
  char buf[48];
  size_t i= sizeof(buf);
  while (u && i > 0)
  {
    buf[--i]= (char) ('0' + (int) (u % 10));
    u/= 10;
  }
  if (neg && i > 0)
    buf[--i]= '-';
  return std::string(buf + i, sizeof(buf) - i);
}

int32_t cancel_cb(void *ctx)
{
  return ctx ? (int32_t) thd_killed(static_cast<THD *>(ctx)) : 0;
}

class moosedb_gbh final : public group_by_handler
{
public:
  moosedb_gbh(THD *thd_arg, handlerton *ht_arg, gplan &&plan)
    : group_by_handler(thd_arg, ht_arg), p_(std::move(plan)) {}

  int init_scan() override { return guarded([&] { return do_init_scan(); }); }
  int next_row() override { return guarded([&] { return do_next_row(); }); }
  int end_scan() override { return guarded([&] { return do_end_scan(); }); }
  void print_error(int error, myf errflag) override;

private:
  /* No C++ exception may reach the server: out of memory becomes an error. */
  template <class F> int guarded(F fn)
  {
    try
    {
      return fn();
    }
    catch (const std::bad_alloc &)
    {
      return oom();
    }
    catch (...)
    {
      return oom(HA_ERR_INTERNAL_ERROR, "unexpected exception");
    }
  }
  int oom(int code= HA_ERR_OUT_OF_MEM, const char *m= "out of memory while aggregating")
  {
    try { msg_.assign(m); } catch (...) { msg_.clear(); }
    release_noexcept();
    return code;
  }
  void release_noexcept() noexcept { try { release(); } catch (...) {} }
  int do_init_scan();
  int do_next_row();
  int do_end_scan();
  bool arena_add(const char *p, size_t n, int64_t *off);
  bool check_killed()
  {
    if (thd && thd_killed(thd)) { killed_= true; return true; }
    return false;
  }
  int fail(const std::string &msg, int code= HA_ERR_INTERNAL_ERROR)
  {
    msg_= msg;
    return code;
  }
  int fail_status(TFStatus st);
  bool charge(uint64_t bytes);
  int map_slots();
  int compute();
  int build_groups(const TFAggBatch &b);
  int fetch_tags(const uint64_t *ids, uint64_t n);
  int merge_groups();
  int order_groups();
  int cmp_key(uint32_t a, uint32_t b, size_t j) const;
  int cmp_raw(uint32_t a, uint32_t b) const;
  int cmp_term(uint32_t a, uint32_t b, const gorder &t) const;
  bool keys_equal(uint32_t a, uint32_t b) const;
  uint64_t key_hash(uint32_t g) const;
  int store_row(uint32_t g);
  int store_agg(Field *f, const gagg &a, uint32_t g);
  void release();

  const kcell &kc(uint32_t g, size_t j) const { return kcells_[(size_t) g * nk_ + j]; }
  kcell &kc(uint32_t g, size_t j) { return kcells_[(size_t) g * nk_ + j]; }
  const acell &ac(uint32_t g, int f) const { return acells_[(size_t) g * nf_ + f]; }
  acell &ac(uint32_t g, int f) { return acells_[(size_t) g * nf_ + f]; }
  const char *kptr(const kcell &c) const { return arena_.data() + c.v; }

  gplan p_;
  std::string msg_;
  bool killed_= false;
  std::vector<Field *> slots_;
  size_t nk_= 0, nf_= 0;
  uint64_t max_memory_= 0, used_= 0;
  uint64_t ngroups_= 0;
  std::vector<kcell> kcells_;
  std::vector<acell> acells_;
  std::string arena_;
  std::vector<uint32_t> live_;
  std::vector<std::vector<my_decimal>> avg_;   /* per aggregate, ORDER BY AVG only */
  size_t pos_= 0;
  std::unique_ptr<MooseDBAgg, agg_closer> agg_;
};

void moosedb_gbh::release()
{
  agg_.reset();
  kcells_= {};
  acells_= {};
  arena_= {};
  live_= {};
  avg_= {};
  slots_.clear();
  ngroups_= 0;
  used_= 0;
  pos_= 0;
}

/* Appends to the key-string arena, charging its real capacity (it grows by doubling). */
bool moosedb_gbh::arena_add(const char *p, size_t n, int64_t *off)
{
  if (arena_.size() + n > arena_.capacity())
  {
    const size_t cap= std::max(arena_.capacity() * 2, arena_.size() + n);
    if (!charge(cap - arena_.capacity()))
      return false;
    arena_.reserve(cap);
  }
  *off= (int64_t) arena_.size();
  arena_.append(p, n);
  return true;
}

bool moosedb_gbh::charge(uint64_t bytes)
{
  if (bytes > max_memory_ || used_ > max_memory_ - bytes)
  {
    msg_= "aggregation exceeds moosedb_agg_max_memory (" +
          std::to_string(max_memory_) + " bytes)";
    return false;
  }
  used_+= bytes;
  return true;
}

int moosedb_gbh::fail_status(TFStatus st)
{
  if (st == TF_ERR_CANCELLED)
  {
    killed_= true;
    if (p_.handler)
      p_.handler->moosedb_gbh_error();     /* drain the pending Rust error */
    return HA_ERR_QUERY_INTERRUPTED;
  }
  std::string m= p_.handler ? p_.handler->moosedb_gbh_error() : std::string();
  if (m.empty())
    m= "aggregation failed (status " + std::to_string((int) st) + ")";
  msg_= m;
  return st == TF_ERR_FULL ? HA_ERR_RECORD_FILE_FULL : HA_ERR_INTERNAL_ERROR;
}

void moosedb_gbh::print_error(int error, myf errflag)
{
  if (killed_ || (thd && thd->killed))
  {
    if (thd)
      thd->send_kill_message();
    return;
  }
  if (!msg_.empty())
  {
    my_printf_error(ER_UNKNOWN_ERROR, "MooseDB: %s", errflag, msg_.c_str());
    return;
  }
  group_by_handler::print_error(error, errflag);
}

/* all_fields -> tmp table fields, see the protocol notes at the top. */
int moosedb_gbh::map_slots()
{
  if (!table || !table->field)
    return fail("internal: no temporary table");
  slots_.assign(p_.items.size(), nullptr);
  uint cursor= 0;
  for (size_t i= 0; i < p_.items.size(); i++)
  {
    const gitem &it= p_.items[i];
    if (it.k == ikind::CONST)
      continue;
    if (cursor >= table->s->fields || !table->field[cursor])
      return fail("internal: temporary table has too few fields");
    Field *f= table->field[cursor++];
    if (it.k == ikind::AGG)
    {
      if (it.idx < 0 || (size_t) it.idx >= p_.aggs.size() ||
          p_.aggs[it.idx].item->result_field != f)
        return fail("internal: aggregate does not match its field");
    }
    else if (!it.field_item || it.field_item->result_field != f)
      return fail("internal: group column does not match its field");
    slots_[i]= f;
  }
  if (cursor != table->s->fields)
    return fail("internal: temporary table has unmapped fields");
  return 0;
}

int moosedb_gbh::do_init_scan()
{
  release();
  msg_.clear();
  killed_= false;
  if (!thd || !p_.handler)
    return fail("internal: no session");
  moosedb_gbh_count();
  nk_= p_.keys.size();
  nf_= p_.funcs.size();
  max_memory_= moosedb_agg_max_memory_of(thd);
  int r= map_slots();
  if (r)
    return r;
  if (p_.limit == 0)
    return 0;                               /* LIMIT 0: nothing to deliver */
  return compute();
}

int moosedb_gbh::compute()
{
  MooseDBTable *t= p_.handler->moosedb_table_handle();
  if (!t)
    return fail("internal: table not open");

  std::vector<TFAggKey> keys;
  if (p_.has_series)
    keys.push_back(TFAggKey{TF_AGG_KEY_SERIES, 0});
  for (const gkey &k : p_.keys)
    if (!k.is_tag)
      keys.push_back(TFAggKey{TF_AGG_KEY_COLUMN, k.col});

  /*
    The series ids of a TAG filter and the aggregation snapshot are two calls:
    the series version read before resolving must still be current once the
    snapshot is open, else resolve again (a new series would be seen
    partially).
  */
  const bool restrict_series= !p_.preds.empty();
  std::vector<uint64_t> ids;
  MooseDBAgg *opened= nullptr;
  bool settled= false;
  for (int attempt= 0; attempt < 64 && !settled; attempt++)
  {
    uint64_t v0= 0, v1= 0;
    if (restrict_series && moosedb_series_version(t, &v0) != TF_OK)
      return fail_status(TF_ERR_INTERNAL);
    if (restrict_series)
    {
      if (!p_.handler->moosedb_gbh_series(p_.preds, &ids))
        return fail("cannot resolve the TAG values of the WHERE");
      if (ids.empty())
        return 0;                           /* no such series: no groups */
    }
    opened= nullptr;
    TFStatus st= moosedb_agg_open(t, p_.lo, p_.hi,
                                  restrict_series ? ids.data() : nullptr,
                                  restrict_series ? (int64_t) ids.size() : -1,
                                  &opened);
    if (st != TF_OK)
      return fail_status(st);
    agg_.reset(opened);
    /* Only a series filter needs the version check: without one every series is in. */
    if (!restrict_series || (moosedb_series_version(t, &v1) == TF_OK && v1 == v0))
      settled= true;
    else
      agg_.reset();
  }
  if (!settled)
    return fail("the set of series kept changing while the aggregation "
                "started", HA_ERR_LOCK_WAIT_TIMEOUT);

  TFAggSpec spec{};
  spec.keys= keys.empty() ? nullptr : keys.data();
  spec.key_count= (uint32_t) keys.size();
  spec.funcs= p_.funcs.empty() ? nullptr : p_.funcs.data();
  spec.func_count= (uint32_t) p_.funcs.size();
  spec.filters= nullptr;
  spec.filter_count= 0;
  spec.max_memory= max_memory_;
  spec.threads= std::max<uint>(1, moosedb_agg_threads_value());
  TFStatus st= moosedb_agg_run(agg_.get(), &spec, cancel_cb, thd);
  if (st != TF_OK)
    return fail_status(st);

  TFAggBatch b{};
  st= moosedb_agg_result(agg_.get(), 0, (uint64_t) 1 << 40, &b);
  if (st != TF_OK)
    return fail_status(st);
  int r= build_groups(b);
  if (r)
    return r;
  agg_.reset();                             /* the snapshot is not needed any more */
  if (p_.need_merge && (r= merge_groups()))
    return r;
  return order_groups();
}

/* Series ids -> the TAG values of the key columns. */
int moosedb_gbh::fetch_tags(const uint64_t *ids, uint64_t n)
{
  MooseDBTable *t= p_.handler->moosedb_table_handle();
  MooseDBSeriesList *list= nullptr;
  if (moosedb_series_list(t, &list) != TF_OK)
    return fail_status(TF_ERR_INTERNAL);
  struct list_closer
  {
    MooseDBSeriesList *l;
    ~list_closer() { moosedb_series_list_close(l); }
  } closer{list};

  /* The series list itself (every series of the table, shared with the server) is outside the budget: it cannot be restricted to the groups. */
  if (!charge(n * sizeof(std::pair<uint64_t, uint32_t>)))
    return HA_ERR_RECORD_FILE_FULL;
  std::vector<std::pair<uint64_t, uint32_t>> by_id(n);
  for (uint64_t g= 0; g < n; g++)
    by_id[g]= {ids[g], (uint32_t) g};
  std::sort(by_id.begin(), by_id.end());

  uint64_t found= 0;
  const uint64_t len= moosedb_series_list_len(list);
  for (uint64_t i= 0; i < len; i++)
  {
    if ((i & 0xFFFF) == 0 && check_killed())
      return HA_ERR_QUERY_INTERRUPTED;
    uint64_t id= 0;
    TFRow tags{0, nullptr};
    if (moosedb_series_list_get(list, i, &id, &tags) != TF_OK)
      continue;
    auto lo= std::lower_bound(by_id.begin(), by_id.end(),
                              std::make_pair(id, (uint32_t) 0));
    if (lo == by_id.end() || lo->first != id)
      continue;
    for (; lo != by_id.end() && lo->first == id; ++lo)
    {
      found++;
      const uint32_t g= lo->second;
      for (size_t j= 0; j < nk_; j++)
      {
        const gkey &k= p_.keys[j];
        if (!k.is_tag)
          continue;
        kcell &c= kc(g, j);
        if ((uint) k.tag_pos >= tags.col_count || !tags.values ||
            tags.values[k.tag_pos].is_null)
        {
          c.null= 1;
          continue;
        }
        const TFStr s= tags.values[k.tag_pos].data.str_val;
        const char *sp= s.ptr ? s.ptr : "";
        if (k.is_str)
        {
          if (!arena_add(sp, s.len, &c.v))
            return HA_ERR_RECORD_FILE_FULL;
          c.len= (uint32_t) s.len;
        }
        else
        {
          /* integer TAG: canonical decimal text */
          const std::string txt(sp, s.len);
          char *end= nullptr;
          errno= 0;
          if (k.is_unsigned)
            c.v= (int64_t) strtoull(txt.c_str(), &end, 10);
          else
            c.v= (int64_t) strtoll(txt.c_str(), &end, 10);
          if (errno || !end || end == txt.c_str() || *end)
            return fail("unreadable integer TAG value");
        }
      }
    }
  }
  if (found < n)
    return fail("a series of the result is missing from the series list");
  return 0;
}

int moosedb_gbh::build_groups(const TFAggBatch &b)
{
  ngroups_= b.row_count;
  if (ngroups_ == 0)
    return 0;
  if (ngroups_ > UINT32_MAX / 2)
    return fail("too many groups", HA_ERR_RECORD_FILE_FULL);
  const size_t expect_keys= (p_.has_series ? 1 : 0) +
      (size_t) std::count_if(p_.keys.begin(), p_.keys.end(),
                             [](const gkey &k) { return !k.is_tag; });
  if (b.key_count != expect_keys || b.func_count != nf_ ||
      (expect_keys && !b.keys) || (nf_ && !b.funcs))
    return fail("internal: aggregation result has an unexpected shape");
  if (!charge(ngroups_ * (nk_ * sizeof(kcell) + nf_ * sizeof(acell))))
    return HA_ERR_RECORD_FILE_FULL;
  kcells_.assign((size_t) ngroups_ * nk_, kcell());
  acells_.assign((size_t) ngroups_ * nf_, acell());

  for (size_t f= 0; f < nf_; f++)
  {
    const TFAggFuncBatch &fb= b.funcs[f];
    if (!fb.count)
      return fail("internal: aggregation result misses a count column");
    for (uint64_t g= 0; g < ngroups_; g++)
    {
      acell &c= ac((uint32_t) g, (int) f);
      c.count= fb.count[g];
      switch (p_.funcs[f].op)
      {
      case TF_AGG_SUM:
        if (!fb.sum_lo || !fb.sum_hi)
          return fail("internal: aggregation result misses a sum column");
        c.sum= (__int128) (((unsigned __int128) (uint64_t) fb.sum_hi[g] << 64) |
                           (unsigned __int128) fb.sum_lo[g]);
        break;
      case TF_AGG_MIN:
      case TF_AGG_MAX:
        if (!fb.val)
          return fail("internal: aggregation result misses a value column");
        c.val= fb.val[g];
        break;
      default:
        break;
      }
    }
  }

  for (size_t j= 0; j < nk_; j++)
  {
    const gkey &k= p_.keys[j];
    if (k.is_tag)
      continue;
    if ((size_t) k.res >= b.key_count)
      return fail("internal: aggregation result misses a key column");
    const TFAggKeyBatch &kb= b.keys[k.res];
    for (uint64_t g= 0; g < ngroups_; g++)
    {
      kcell &c= kc((uint32_t) g, j);
      if (kb.nulls && kb.nulls[g])
      {
        c.null= 1;
        continue;
      }
      if (k.is_str)
      {
        if (kb.kind != TF_AGG_KEYCOL_BYTES || !kb.starts || !kb.lens ||
            (!kb.data && kb.lens[g]) || kb.starts[g] + kb.lens[g] > kb.data_len)
          return fail("internal: bad string key column");
        if (!arena_add(kb.lens[g] ? (const char *) kb.data + kb.starts[g] : "",
                       kb.lens[g], &c.v))
          return HA_ERR_RECORD_FILE_FULL;
        c.len= kb.lens[g];
      }
      else
      {
        if (kb.kind != TF_AGG_KEYCOL_INT || !kb.values)
          return fail("internal: bad integer key column");
        c.v= static_cast<const int64_t *>(kb.values)[g];
      }
      if ((g & 0xFFFF) == 0 && thd_killed(thd))
      {
        killed_= true;
        return HA_ERR_QUERY_INTERRUPTED;
      }
    }
  }
  if (p_.has_series)
  {
    if (b.keys[0].kind != TF_AGG_KEYCOL_SERIES || !b.keys[0].values)
      return fail("internal: bad series key column");
    int r= fetch_tags(static_cast<const uint64_t *>(b.keys[0].values), ngroups_);
    if (r)
      return r;
  }
  if (!charge(ngroups_ * sizeof(uint32_t)))
    return HA_ERR_RECORD_FILE_FULL;
  live_.resize(ngroups_);
  std::iota(live_.begin(), live_.end(), 0u);
  return 0;
}

/* NULL < any value; strings by the column's collation; integers by sign. */
int moosedb_gbh::cmp_key(uint32_t a, uint32_t b, size_t j) const
{
  const kcell &x= kc(a, j), &y= kc(b, j);
  if (x.null || y.null)
    return x.null == y.null ? 0 : (x.null ? -1 : 1);
  const gkey &k= p_.keys[j];
  if (k.is_str)
    return k.cs->strnncollsp((const uchar *) kptr(x), x.len,
                             (const uchar *) kptr(y), y.len);
  if (k.is_unsigned)
    return (uint64_t) x.v < (uint64_t) y.v ? -1 : (uint64_t) x.v > (uint64_t) y.v;
  return x.v < y.v ? -1 : x.v > y.v;
}

/* Total order on the raw key bytes: the deterministic tiebreak. */
int moosedb_gbh::cmp_raw(uint32_t a, uint32_t b) const
{
  for (size_t j= 0; j < nk_; j++)
  {
    const kcell &x= kc(a, j), &y= kc(b, j);
    if (x.null != y.null)
      return x.null ? -1 : 1;
    if (x.null)
      continue;
    if (p_.keys[j].is_str)
    {
      const size_t n= std::min(x.len, y.len);
      int c= n ? memcmp(kptr(x), kptr(y), n) : 0;
      if (!c)
        c= x.len < y.len ? -1 : x.len > y.len;
      if (c)
        return c;
    }
    else if (x.v != y.v)
    {
      if (p_.keys[j].is_unsigned)
        return (uint64_t) x.v < (uint64_t) y.v ? -1 : 1;
      return x.v < y.v ? -1 : 1;
    }
  }
  return 0;
}

bool moosedb_gbh::keys_equal(uint32_t a, uint32_t b) const
{
  for (size_t j= 0; j < nk_; j++)
    if (cmp_key(a, b, j) != 0)
      return false;
  return true;
}

uint64_t moosedb_gbh::key_hash(uint32_t g) const
{
  uint64_t h= 0x9E3779B97F4A7C15ULL;
  for (size_t j= 0; j < nk_; j++)
  {
    const kcell &c= kc(g, j);
    uint64_t x;
    if (c.null)
      x= 0x1234567ULL;
    else if (p_.keys[j].is_str)
    {
      ulong nr1= 1, nr2= 4;
      p_.keys[j].cs->hash_sort((const uchar *) kptr(c), c.len, &nr1, &nr2);
      x= ((uint64_t) nr1 * 0x9E3779B97F4A7C15ULL) ^ (uint64_t) nr2;
    }
    else
      x= (uint64_t) c.v * 0xBF58476D1CE4E5B9ULL;
    h= (h ^ x) * 0x100000001B3ULL;
    h^= h >> 29;
  }
  return h;
}

/*
  Groups whose keys compare equal under the collation (different series with
  the same TAG value; 'a' and 'A' under a _ci collation) become one: states
  are combined, the key shown is the smallest by raw bytes (deterministic).
*/
int moosedb_gbh::merge_groups()
{
  if (!charge(ngroups_ * (sizeof(uint64_t) + 4)))
    return HA_ERR_RECORD_FILE_FULL;
  std::vector<uint64_t> h(ngroups_);
  for (uint64_t g= 0; g < ngroups_; g++)
  {
    h[g]= key_hash((uint32_t) g);
    if ((g & 0xFFFF) == 0 && thd_killed(thd))
    {
      killed_= true;
      return HA_ERR_QUERY_INTERRUPTED;
    }
  }
  std::sort(live_.begin(), live_.end(), [&](uint32_t a, uint32_t b) {
    return h[a] != h[b] ? h[a] < h[b] : a < b;
  });

  auto absorb = [&](uint32_t into, uint32_t from) -> bool {
    for (size_t f= 0; f < nf_; f++)
    {
      acell &d= ac(into, (int) f);
      const acell &s= ac(from, (int) f);
      switch (p_.funcs[f].op)
      {
      case TF_AGG_SUM:
        if (__builtin_add_overflow(d.sum, s.sum, &d.sum))
          return false;
        break;
      case TF_AGG_MIN:
        if (s.count && (!d.count || s.val < d.val))
          d.val= s.val;
        break;
      case TF_AGG_MAX:
        if (s.count && (!d.count || s.val > d.val))
          d.val= s.val;
        break;
      default:
        break;
      }
      d.count+= s.count;
    }
    return true;
  };

  std::vector<uint32_t> out;
  out.reserve(live_.size());
  size_t i= 0;
  while (i < live_.size())
  {
    size_t j= i;
    while (j < live_.size() && h[live_[j]] == h[live_[i]])
      j++;
    const size_t first= out.size();       /* representatives of this hash */
    for (size_t m= i; m < j; m++)
    {
      if ((m & 0xFFFF) == 0 && check_killed())
        return HA_ERR_QUERY_INTERRUPTED;
      const uint32_t g= live_[m];
      bool merged= false;
      for (size_t r= first; r < out.size() && !merged; r++)
      {
        if (!keys_equal(out[r], g))
          continue;
        if (!absorb(out[r], g))
          return fail("SUM overflows the 128-bit accumulator",
                      HA_ERR_RECORD_FILE_FULL);
        if (cmp_raw(g, out[r]) < 0)
          for (size_t jj= 0; jj < nk_; jj++)
            kc(out[r], jj)= kc(g, jj);
        merged= true;
      }
      if (!merged)
        out.push_back(g);
    }
    i= j;
  }
  live_.swap(out);
  return 0;
}

int moosedb_gbh::cmp_term(uint32_t a, uint32_t b, const gorder &t) const
{
  if (t.is_key)
    return cmp_key(a, b, (size_t) t.idx);
  const gagg &g= p_.aggs[t.idx];
  const acell &x= ac(a, g.func), &y= ac(b, g.func);
  switch (g.k)
  {
  case aggk::COUNT_STAR:
  case aggk::COUNT:
    return x.count < y.count ? -1 : x.count > y.count;
  default:
    break;
  }
  if (!x.count || !y.count)
    return (x.count == 0) == (y.count == 0)
           ? 0 : (x.count == 0 ? -1 : 1);
  switch (g.k)
  {
  case aggk::SUM:
    return x.sum < y.sum ? -1 : x.sum > y.sum;
  case aggk::AVG:
  {
    const auto &d= avg_[t.idx];
    return my_decimal_cmp(&d[a], &d[b]);
  }
  default:
    return x.val < y.val ? -1 : x.val > y.val;
  }
}

int moosedb_gbh::order_groups()
{
  /* AVG compares after the division, as the server would. */
  avg_.assign(p_.aggs.size(), {});
  for (const gorder &t : p_.order)
  {
    if (t.is_key || p_.aggs[t.idx].k != aggk::AVG || !avg_[t.idx].empty())
      continue;
    const gagg &g= p_.aggs[t.idx];
    if (!charge(ngroups_ * sizeof(my_decimal)))
      return HA_ERR_RECORD_FILE_FULL;
    avg_[t.idx].resize(ngroups_);
    for (uint32_t i : live_)
    {
      const acell &c= ac(i, g.func);
      if (!c.count)
        continue;
      my_decimal sum_d, cnt_d;
      const std::string s= i128_to_string(c.sum);
      if (str2my_decimal(E_DEC_FATAL_ERROR, s.data(), s.size(), &my_charset_latin1, &sum_d) ||
          int2my_decimal(E_DEC_FATAL_ERROR, (longlong) c.count, TRUE, &cnt_d) ||
          my_decimal_div(E_DEC_FATAL_ERROR, &avg_[t.idx][i], &sum_d, &cnt_d,
                         g.prec_inc))
        return fail("AVG overflows DECIMAL", HA_ERR_RECORD_FILE_FULL);
    }
  }
  if (check_killed())
    return HA_ERR_QUERY_INTERRUPTED;
  auto less= [&](uint32_t a, uint32_t b) {
    for (const gorder &t : p_.order)
    {
      const int c= cmp_term(a, b, t);
      if (c)
        return t.desc ? c > 0 : c < 0;
    }
    return cmp_raw(a, b) < 0;
  };
  if (!p_.has_having && p_.limit < live_.size())
  {
    std::partial_sort(live_.begin(), live_.begin() + (size_t) p_.limit,
                      live_.end(), less);
    live_.resize((size_t) p_.limit);
  }
  else
    std::sort(live_.begin(), live_.end(), less);
  return 0;
}

int moosedb_gbh::store_agg(Field *f, const gagg &a, uint32_t g)
{
  const acell &c= ac(g, a.func);
  switch (a.k)
  {
  case aggk::COUNT_STAR:
  case aggk::COUNT:
    f->set_notnull();
    f->store((longlong) c.count, true);
    return 0;
  case aggk::SUM:
  case aggk::AVG:
  {
    if (!c.count)
    {
      f->set_null();
      return 0;
    }
    f->set_notnull();
    const std::string s= i128_to_string(c.sum);
    if (a.k == aggk::SUM)
    {
      if (f->store(s.data(), (uint) s.size(), &my_charset_latin1) > 0)
        return fail("SUM does not fit its DECIMAL result", HA_ERR_RECORD_FILE_FULL);
      return 0;
    }
    my_decimal sum_d, cnt_d, res;
    if (str2my_decimal(E_DEC_FATAL_ERROR, s.data(), s.size(), &my_charset_latin1, &sum_d) ||
        int2my_decimal(E_DEC_FATAL_ERROR, (longlong) c.count, TRUE, &cnt_d) ||
        my_decimal_div(E_DEC_FATAL_ERROR, &res, &sum_d, &cnt_d, a.prec_inc) ||
        f->store_decimal(&res) > 0)
      return fail("AVG does not fit its DECIMAL result", HA_ERR_RECORD_FILE_FULL);
    return 0;
  }
  case aggk::MIN:
  case aggk::MAX:
  {
    if (!c.count)
    {
      f->set_null();
      return 0;
    }
    f->set_notnull();
    if (a.conv == tf_conv::DATETIME)
    {
      MYSQL_TIME lt;
      moosedb_micros_to_time(c.val, &lt);
      f->store_time_dec(&lt, f->decimals());
    }
    else if (a.conv == tf_conv::INT_SECS)
      f->store(floor_div_g(c.val, MICROS_PER_SEC_G), a.is_unsigned);
    else
      f->store(c.val, a.is_unsigned);
    return 0;
  }
  }
  return 0;
}

int moosedb_gbh::store_row(uint32_t g)
{
  if (table->s->null_bytes && table->s->default_values)
    memcpy(table->record[0], table->s->default_values, table->s->null_bytes);
  MY_BITMAP *old_map= dbug_tmp_use_all_columns(table, &table->write_set);
  int r= 0;
  for (size_t i= 0; i < p_.items.size() && !r; i++)
  {
    Field *f= slots_[i];
    if (!f)
      continue;
    const gitem &it= p_.items[i];
    if (it.k == ikind::KEY)
    {
      const kcell &c= kc(g, (size_t) it.idx);
      const gkey &k= p_.keys[it.idx];
      if (c.null)
        f->set_null();
      else
      {
        f->set_notnull();
        if (k.is_str)
          f->store(kptr(c), c.len, k.cs);
        else
          f->store((longlong) c.v, k.is_unsigned);
      }
    }
    else
      r= store_agg(f, p_.aggs[it.idx], g);
  }
  dbug_tmp_restore_column_map(&table->write_set, old_map);
  return r;
}

int moosedb_gbh::do_next_row()
{
  if (pos_ >= live_.size())
    return HA_ERR_END_OF_FILE;
  return store_row(live_[pos_++]);
}

int moosedb_gbh::do_end_scan()
{
  /*
    The server leaves FOUND_ROWS() stale for a pushed-down GROUP BY (it
    returns before storing it); set what the normal path would: the rows
    sent so far. SQL_CALC_FOUND_ROWS queries are refused by analyze().
  */
  if (thd && thd->lex && thd->lex->current_select &&
      thd->lex->current_select->join)
  {
    JOIN *j= thd->lex->current_select->join;
    thd->limit_found_rows= j->send_records - j->duplicate_rows;
  }
  release();
  return 0;
}

}  // namespace

const char *moosedb_gbh_scope_refusal(THD *thd, Query *q)
{
  try
  {
    gplan plan;
    return analyze(thd, q, &plan);
  }
  catch (...)
  {
    return "internal: out of memory";
  }
}

static group_by_handler *create_group_by_impl(THD *thd, Query *query);

group_by_handler *moosedb_create_group_by(THD *thd, Query *query)
{
  try
  {
    return create_group_by_impl(thd, query);
  }
  catch (...)       // out of memory while planning: the normal path runs the query
  {
    return nullptr;
  }
}

static group_by_handler *create_group_by_impl(THD *thd, Query *query)
{
  assert(thd);
  if (!thd || !query)
    return nullptr;
  const bool debug= moosedb_gbh_debug_enabled(thd);
  if (!debug && !moosedb_group_by_pushdown_enabled(thd))
    return nullptr;

  gplan plan;
  const char *why= analyze(thd, query, &plan);
  if (debug)
  {
    if (why)
      push_warning_printf(thd, Sql_condition::WARN_LEVEL_NOTE, ER_UNKNOWN_ERROR,
                          "gbh: refused: %s", why);
    else
      push_warning(thd, Sql_condition::WARN_LEVEL_NOTE, ER_UNKNOWN_ERROR,
                   "gbh: accepted");
  }
  if (why || !moosedb_group_by_pushdown_enabled(thd))
    return nullptr;

  handlerton *ht= query->from->table->file->ht;
  /*
    The server frees the handler with a plain `delete` (~Pushdown_query), so
    ownership has to leave the smart pointer here.
  */
  group_by_handler *h=
    std::make_unique<moosedb_gbh>(thd, ht, std::move(plan)).release();
  /* The engine delivers rows ordered and grouped: nothing is left to the server. */
  query->group_by= nullptr;
  query->order_by= nullptr;
  return h;
}
