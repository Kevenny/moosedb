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
#include <chrono>
#include <climits>
#include <cstring>
#include <memory>
#include <numeric>
#include <atomic>
#include <functional>
#include <string>
#include <thread>
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

/*
  Binary PAD SPACE collation (utf8mb4_bin, latin1_bin...) of a charset whose
  space is the single byte 0x20: equal after trimming trailing 0x20, and the
  order is that of the trimmed bytes with the shorter operand padded by spaces.
  The core groups such a key by the trimmed value (TF_AGG_KEY_COLUMN_PAD).
*/
bool cs_pad_bin(const CHARSET_INFO *cs)
{
  if (!cs || cs_exact(cs))
    return false;
  const char *n= cs->cs_name.str;
  return (cs->state & MY_CS_BINSORT) && !(cs->state & MY_CS_NOPAD) &&
         cs->mbminlen == 1 && n &&
         (strcmp(n, "utf8mb4") == 0 || strcmp(n, "utf8mb3") == 0 ||
          strcmp(n, "latin1") == 0 || strcmp(n, "ascii") == 0);
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
  bool pad= false;                 /* non-TAG string, binary PAD SPACE: the core trims and groups it */
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

/*
  A filter column: the WHERE conjuncts that reference only this one stored,
  non-TAG column. They are evaluated by the server's own Items on every
  distinct value of the column (see build_mask), so the engine and the
  server agree on collation, PAD and every conversion.
*/
struct gfilter
{
  Field *field= nullptr;
  uint32_t col= 0;                 /* layout column                          */
  bool is_str= false;
  std::vector<Item *> conds;       /* ANDed                                  */
  /* `col = const` / `col IN (consts)` compared byte for byte: the core can filter by the values themselves */
  bool exact= false;
  bool pad= false;                 /* exact: PAD SPACE binary collation (trailing 0x20 ignored) */
  std::vector<Item *> consts;
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
  std::vector<gfilter> filters;
  Item *where= nullptr;            /* the WHERE, for the zone-map hints */
  longlong lo= LONGLONG_MIN, hi= LONGLONG_MAX;
  ulonglong limit= HA_POS_ERROR;
  bool has_having= false;
  bool need_merge= false;
  bool scalar= false;              /* aggregates without GROUP BY: one group, one row, even over no rows */
  int top_func= -1;                /* core top-N: index into funcs, -1 = off */
  bool top_desc= false;

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
    {
      k.cs= f->charset();
      k.pad= !k.is_tag && cs_pad_bin(k.cs);
    }
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
    /*
      The server already folded this one (opt_sum_query: COUNT(*) with no WHERE
      from the table's row count, MIN/MAX from an index): it is a constant with
      no field in the temporary table. Item_sum::const_item() is only true then:
      Item_func_or_sum::const_item() returns const_item_cache (item.h:5904),
      Item_sum::mark_as_sum_func() clears it (item_sum.cc:525) and only
      Item_sum::make_const() sets it (item_sum.h:503), so SUM(1), COUNT(5)... are not.
    */
    if (real->const_item())
    {
      out->k= ikind::CONST;
      return nullptr;
    }
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

/*
  A constant the predicate evaluation may repeat for every dictionary value:
  fixed for the statement, cheap, deterministic, no stored routine or UDF, and
  a bound parameter (an unbound one has type PARAM_ITEM).
*/
bool const_safe(Item *it, int depth= 0)
{
  if (!it || depth > 16 || !is_constant(it) || it->type() == Item::PARAM_ITEM ||
      it->is_expensive())
    return false;
  if (it->type() == Item::FUNC_ITEM)
  {
    Item_func *f= static_cast<Item_func *>(it);
    if (f->functype() == Item_func::FUNC_SP || f->functype() == Item_func::UDF_FUNC)
      return false;
    Item **a= f->arguments();
    for (uint i= 0; a && i < f->argument_count(); i++)
      if (!const_safe(a[i], depth + 1))
        return false;
  }
  return true;
}

/*
  Shape check of one WHERE predicate that is not a TAG/time condition: AND/OR/NOT
  of comparisons, BETWEEN, IN, LIKE, IS [NOT] NULL whose operands are a column
  of the table or a constant. *col collects the one column the whole predicate
  may reference; a second one is a refusal. A comparison between a string column
  and a non-string constant (or the reverse) is refused: the server converts
  per row and warns per row, which a once-per-distinct-value evaluation cannot
  reproduce.
*/
const char *pred_refusal(const gplan *p, Item *it, Field **col, int depth= 0)
{
  if (!it || depth > 16)
    return "WHERE: unsupported expression";
  if (it->type() == Item::COND_ITEM)
  {
    Item_cond *c= static_cast<Item_cond *>(it);
    if (c->functype() != Item_func::COND_AND_FUNC &&
        c->functype() != Item_func::COND_OR_FUNC)
      return "WHERE: XOR";
    List_iterator_fast<Item> li(*c->argument_list());
    Item *a;
    while ((a= li++))
    {
      const char *r= pred_refusal(p, a, col, depth + 1);
      if (r)
        return r;
    }
    return nullptr;
  }
  if (it->type() != Item::FUNC_ITEM)
    return "WHERE: unsupported expression";
  Item_func *f= static_cast<Item_func *>(it);
  Item **a= f->arguments();
  const uint n= f->argument_count();
  if (!a || n == 0)
    return "WHERE: unsupported expression";
  uint min_args= 2, max_args= 2;
  switch (f->functype())
  {
  case Item_func::NOT_FUNC:
    return n == 1 ? pred_refusal(p, a[0], col, depth + 1)
                  : "WHERE: unsupported predicate";
  case Item_func::EQ_FUNC:
  case Item_func::EQUAL_FUNC:
  case Item_func::NE_FUNC:
  case Item_func::LT_FUNC:
  case Item_func::LE_FUNC:
  case Item_func::GE_FUNC:
  case Item_func::GT_FUNC:
    break;
  case Item_func::BETWEEN:
    min_args= max_args= 3;
    break;
  case Item_func::IN_FUNC:
    max_args= UINT_MAX;
    break;
  case Item_func::LIKE_FUNC:
    max_args= 3;                              /* [ESCAPE c] */
    break;
  case Item_func::ISNULL_FUNC:
  case Item_func::ISNOTNULL_FUNC:
    min_args= max_args= 1;
    break;
  default:
    return "WHERE: unsupported predicate";
  }
  if (n < min_args || n > max_args)
    return "WHERE: unsupported predicate";
  Field *mine= nullptr;
  bool string_const= false, other_const= false, dec_or_time= false;
  for (uint i= 0; i < n; i++)
  {
    Field *fld= column_of(a[i], p->table);
    if (fld)
    {
      if ((mine && mine != fld) || (*col && *col != fld))
        return "WHERE: predicate over two columns";
      mine= *col= fld;
    }
    else if (const_safe(a[i]))
    {
      if (a[i]->type() == Item::NULL_ITEM || a[i]->is_null())
        continue;                             /* NULL compares as NULL either way */
      (a[i]->cmp_type() == STRING_RESULT ? string_const : other_const)= true;
      if (a[i]->cmp_type() == DECIMAL_RESULT || a[i]->cmp_type() == TIME_RESULT)
        dec_or_time= true;
    }
    else
      return "WHERE: unsupported predicate";
  }
  if (mine)
  {
    const Item_result ct= mine->cmp_type();
    if (ct != STRING_RESULT && ct != INT_RESULT)
      return "WHERE: predicate on an unsupported column type";
    if (ct == STRING_RESULT ? other_const : string_const)
      return "WHERE: comparison across string and number";
    /* a DECIMAL/TIME constant against an integer column: the server converts per row (and warns per row) */
    if (ct == INT_RESULT && dec_or_time)
      return "WHERE: comparison across string and number";
  }
  return nullptr;
}

/* The column of a filter must be one the dictionary phase can enumerate. */
const char *filter_column_refusal(const gplan *p, Field *f, gfilter *out)
{
  tf_conv c;
  if (!p->handler->moosedb_column_conv(f, &c))
    return "WHERE: predicate on a column that is not stored";
  if (p->handler->moosedb_is_timestamp_column(f))
    return "WHERE: predicate on the time column other than bounds";
  if (c == tf_conv::TAG)
    return "WHERE: predicate on a TAG column other than =/IN";
  const int col= p->layout_col(f);
  if (col < 0)
    return "WHERE: predicate on a column that is not stored";
  if (c == tf_conv::INT && is_int_field(f))
    out->is_str= false;
  else if (c == tf_conv::STRING &&
           (f->real_type() == MYSQL_TYPE_VARCHAR || f->real_type() == MYSQL_TYPE_STRING))
    out->is_str= true;
  else
    return "WHERE: predicate on an unsupported column type";
  out->field= f;
  out->col= (uint32_t) col;
  return nullptr;
}

void flatten_and(Item *it, std::vector<Item *> *out, int depth= 0)
{
  if (it && depth < 64 && it->type() == Item::COND_ITEM &&
      static_cast<Item_cond *>(it)->functype() == Item_func::COND_AND_FUNC)
  {
    List_iterator_fast<Item> li(*static_cast<Item_cond *>(it)->argument_list());
    Item *a;
    while ((a= li++))
      flatten_and(a, out, depth + 1);
    return;
  }
  out->push_back(it);
}

/*
  Marks the filter `exact` when its whole predicate is `col = const` or
  `col IN (consts)` (not negated, no OR/NOT) and the SQL comparison is a byte
  comparison, so the core can filter by the values with no dictionary phase:
    - integers: the comparison is on integers (the constants are INT);
    - strings: a VARCHAR whose collation is binary NO PAD (PAD SPACE ones, even
      *_bin, ignore trailing spaces: not byte-exact), the comparison collation
      is that very collation, and the constants are in the column's charset.
  A NULL constant never matches; it is left to the dictionary path (simpler).
*/
void exact_shape(const gplan *p, gfilter *g)
{
  if (g->conds.size() != 1 || !g->conds[0] || g->conds[0]->type() != Item::FUNC_ITEM)
    return;
  Item_func *f= static_cast<Item_func *>(g->conds[0]);
  Item **a= f->arguments();
  const uint n= f->argument_count();
  if (!a || !g->field)
    return;
  std::vector<Item *> cs;
  if (f->functype() == Item_func::EQ_FUNC && n == 2)
  {
    if (column_of(a[0], p->table) == g->field)
      cs.push_back(a[1]);
    else if (column_of(a[1], p->table) == g->field)
      cs.push_back(a[0]);
    else
      return;
  }
  else if (f->functype() == Item_func::IN_FUNC && n >= 2 &&
           !static_cast<Item_func_opt_neg *>(f)->negated &&
           column_of(a[0], p->table) == g->field)
    cs.assign(a + 1, a + n);
  else
    return;
  const bool is_in= f->functype() == Item_func::IN_FUNC;
  /* IN: the comparison type is that of its arguments (all of one kind, checked below) */
  const Type_handler *th= is_in ? nullptr : static_cast<Item_bool_func2 *>(f)->compare_type_handler();
  CHARSET_INFO *ccs= is_in ? static_cast<Item_func_opt_neg *>(f)->compare_collation()
                           : static_cast<Item_bool_func2 *>(f)->compare_collation();
  for (Item *c : cs)
  {
    if (!c || column_of(c, p->table) || c->type() == Item::NULL_ITEM || c->is_null())
      return;
    if (g->is_str)
    {
      if (c->cmp_type() != STRING_RESULT ||
          !my_charset_same(c->collation.collation, g->field->charset()))
        return;
    }
    else if (c->cmp_type() != INT_RESULT)
      return;
  }
  if (g->is_str)
  {
    CHARSET_INFO *fcs= g->field->charset();
    if (g->field->real_type() != MYSQL_TYPE_VARCHAR || (th && th->cmp_type() != STRING_RESULT) ||
        ccs != fcs)
      return;
    if (!cs_exact(fcs))
    {
      if (!cs_pad_bin(fcs))
        return;
      g->pad= true;
    }
  }
  else if (th && th->cmp_type() != INT_RESULT)
    return;
  g->exact= true;
  g->consts= std::move(cs);
}

/*
  The WHERE is a conjunction. TAG =/IN and bounds on the time axis go to the
  engine's own series/time selection (exact capture, as for DELETE); every other
  conjunct must touch exactly one stored non-TAG column and is grouped with the
  others of that column into one filter, evaluated at init_scan.
*/
const char *where_refusal(gplan *p, Item *cond)
{
  if (!cond)
    return nullptr;
  std::vector<Item *> conj;
  flatten_and(cond, &conj);
  p->where= cond;
  for (Item *c : conj)
  {
    if (!c)
      return "WHERE: unsupported expression";
    if (p->handler->moosedb_gbh_where(c, &p->preds, &p->lo, &p->hi))
      continue;
    Field *col= nullptr;
    const char *r= pred_refusal(p, c, &col);
    if (r)
      return r;
    if (!col)
      return "WHERE: condition without a column";
    gfilter *g= nullptr;
    for (gfilter &x : p->filters)
      if (x.field == col)
        g= &x;
    if (!g)
    {
      gfilter fresh;
      r= filter_column_refusal(p, col, &fresh);
      if (r)
        return r;
      p->filters.push_back(std::move(fresh));
      g= &p->filters.back();
    }
    g->conds.push_back(c);
  }
  for (gfilter &g : p->filters)
    exact_shape(p, &g);
  return nullptr;
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

  /*
    Without GROUP BY the server still calls us for every query; only the ones
    with an aggregate (implicit grouping: one group, one row even over no rows)
    are ours.
  */
  p->scalar= !q->group_by;
  if (p->scalar)
  {
    bool has_sum= false;
    List_iterator_fast<Item> si(*q->select);
    for (Item *x; !has_sum && (x= si++);)
      has_sum= x->with_sum_func();
    if (!has_sum)
      return "no GROUP BY";
  }

  /* Clauses the engine does not do. */
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

  /* Everything folded by the server: nothing left for the engine. */
  if (p->scalar && p->aggs.empty())
    return "no aggregate left for the engine";

  r= where_refusal(p, q->where);
  if (r)
    return r;

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
    if (!k.is_tag && k.is_str && !cs_exact(k.cs) && !k.pad)
      p->need_merge= true;
  /*
    Core top-N (ORDER BY one aggregate LIMIT K, one key). Ties: the core breaks
    them by ascending key (signed integer, unsigned bytes, NULL first) which is
    the handler's own tiebreak (cmp_raw) for these keys, so the K groups kept are
    the same. A binary PAD SPACE key (gkey::pad) is grouped by the core on the
    trimmed value, so need_merge stays false, and its tiebreak is the collation
    order (pad_cmp in agg.rs = strnncollsp). Not for a TAG key (tiebreak by
    series id, not by TAG value, and series may need merging), a _ci/_ai or
    other collation-merged string (the core would need the collation weights),
    BIGINT UNSIGNED (core compares signed), AVG, HAVING, a second ORDER BY term
    other than the key ASC, or no GROUP BY.
  */
  if (!p->has_having && p->limit != HA_POS_ERROR && p->limit > 0 && !p->has_series &&
      !p->need_merge && p->keys.size() == 1 &&
      (p->order.size() == 1 ||
       /* ORDER BY aggregate, the key ASC: the SQL order is total and the second term is exactly the core's
          tiebreak (signed integer, unsigned bytes with the shorter prefix first, NULL first). For a binary
          NO PAD collation of a one-byte-unit charset (utf8mb4/utf8mb3/latin1/ascii: code point order ==
          byte order) strnncollsp == memcmp-then-length. */
       (p->order.size() == 2 && p->order[1].is_key && !p->order[1].desc &&
        p->order[1].idx == 0 &&
        (!p->keys[0].is_str || (p->keys[0].cs && p->keys[0].cs->mbminlen == 1)))) &&
      !p->order[0].is_key && p->order[0].idx >= 0 &&
      (size_t) p->order[0].idx < p->aggs.size() &&
      p->aggs[p->order[0].idx].k != aggk::AVG &&
      !(!p->keys[0].is_str && is_ubigint(p->keys[0].field)))
  {
    p->top_func= p->aggs[p->order[0].idx].func;
    p->top_desc= p->order[0].desc;
  }
  /* The core's own limits (agg.rs MAX_KEYS, MAX_FUNCS, MAX_FILTERS): beyond them it fails, the normal path must run instead. */
  if (p->keys.size() > 16 || p->funcs.size() > 64 || p->filters.size() > 16)
    return "too many keys, aggregates or filtered columns for the engine";
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

/* The values of an exact filter, in the core's layout. */
struct exact_set
{
  std::vector<int64_t> ints;
  std::vector<uint64_t> starts;
  std::vector<uint32_t> lens;
  std::string data;
};

/*
  Sorts `v` (group indices) with `less` on up to `threads` threads: the chunks
  are sorted apart, then merged pairwise. `less` only reads shared data. A
  thread that cannot be started is replaced by running its job inline, and
  nothing a job throws leaves it (it would end the process): on a failure the
  whole vector, still a permutation, is sorted again on this thread.
  The sort does not poll KILL / max_statement_time: it is not interruptible
  (about 1 s for a million string groups); the checks resume right after it.
*/
template <class Less>
void parallel_sort(std::vector<uint32_t> &v, const Less &less, unsigned threads)
{
  const size_t n= v.size();
  if (threads < 2 || n < 65536)
  {
    std::sort(v.begin(), v.end(), less);
    return;
  }
  threads= std::min(threads, 16u);
  std::vector<size_t> cut(threads + 1);
  for (unsigned i= 0; i <= threads; i++)
    cut[i]= n * i / threads;
  std::atomic<bool> failed(false);
  auto run_all= [&](std::vector<std::function<void()>> &jobs) {
    std::vector<std::thread> ts;
    ts.reserve(jobs.size());
    for (size_t i= 1; i < jobs.size(); i++)
    {
      try { ts.emplace_back(jobs[i]); }
      catch (...) { jobs[i](); }
    }
    jobs[0]();
    for (std::thread &t : ts)
      t.join();
  };
  std::vector<std::function<void()>> jobs;
  for (unsigned i= 0; i < threads; i++)
    jobs.push_back([&, i] {
      try { std::sort(v.begin() + cut[i], v.begin() + cut[i + 1], less); }
      catch (...) { failed= true; }
    });
  run_all(jobs);
  for (unsigned step= 1; step < threads; step*= 2)
  {
    jobs.clear();
    for (unsigned i= 0; i + step < threads; i+= 2 * step)
      jobs.push_back([&, i, step] {
        try
        {
          std::inplace_merge(v.begin() + cut[i], v.begin() + cut[i + step],
                             v.begin() + cut[std::min(i + 2 * step, threads)], less);
        }
        catch (...) { failed= true; }
      });
    run_all(jobs);
  }
  if (failed)
    std::sort(v.begin(), v.end(), less);
}

uint64_t us_since(std::chrono::steady_clock::time_point t0)
{
  return (uint64_t) std::chrono::duration_cast<std::chrono::microseconds>(
             std::chrono::steady_clock::now() - t0).count();
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
  int budget_exceeded()
  {
    msg_= "aggregation exceeds moosedb_agg_max_memory (" + std::to_string(max_memory_) + " bytes)";
    return HA_ERR_RECORD_FILE_FULL;
  }
  bool charge(uint64_t bytes);
  int map_slots();
  int compute();
  int build_mask(const gfilter &f, std::vector<uint8_t> *mask, uint64_t *values);
  int build_exact(const gfilter &f, exact_set *out);
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
  std::vector<TFColPred> hints_;               /* zone-map hints of the WHERE (INT/FLOAT ranges), set in compute() */
  std::string msg_;
  bool killed_= false;
  std::vector<Field *> slots_;
  size_t nk_= 0, nf_= 0;
  uint64_t max_memory_= 0, used_= 0;
  uint64_t ngroups_= 0;
  uint64_t eval_us_= 0;                        /* Item evaluation inside the dictionary phase */
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
  eval_us_= 0;
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
  r= compute();
  if (r || !p_.scalar || !live_.empty())
    return r;
  /* Implicit grouping over no row (an early "nothing can match" exit): the one row COUNT = 0, the rest NULL. */
  if (!charge(nf_ * sizeof(acell)))
    return HA_ERR_RECORD_FILE_FULL;
  ngroups_= 1;
  acells_.assign(nf_, acell());
  live_.assign(1, 0u);
  return 0;
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
      keys.push_back(TFAggKey{(uint8_t) (k.pad ? TF_AGG_KEY_COLUMN_PAD : TF_AGG_KEY_COLUMN), k.col});

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

  /*
    Filters. `col = const` / `col IN (consts)` on a byte-exact comparison goes
    to the core as a value set (no dictionary). Every other predicate: one
    dictionary phase per column on this very snapshot (so the dictionary holds
    only the values of the WHERE's time range and series), the server's Items
    decide each value, the engine filters by the mask.
  */
  using clk= std::chrono::steady_clock;
  const auto t_dict= clk::now();
  uint64_t dict_values= 0;
  /*
    Range predicates of the WHERE (`col > K`, BETWEEN, ...) as zone-map hints:
    the core skips entries that cannot hold a matching row, in the dictionary
    phase and in the final run alike. Extraction is the one of the normal scan
    (collect_value_predicates); the exact test stays with the mask / value set.
  */
  hints_.clear();
  {
    std::vector<tf_val_pred> vp;
    if (p_.handler)
      p_.handler->moosedb_gbh_value_preds(p_.where, &vp);
    for (const tf_val_pred &v : vp)
    {
      TFColPred c{};
      c.col= v.col;
      c.kind= v.kind;
      c.int_lo= v.ilo;
      c.int_hi= v.ihi;
      c.float_lo= v.flo;
      c.float_hi= v.fhi;
      hints_.push_back(c);
    }
  }
  std::vector<std::vector<uint8_t>> masks(p_.filters.size());
  std::vector<exact_set> sets(p_.filters.size());     /* never resized: the filters point into them */
  std::vector<TFAggFilter> tfs;
  for (size_t i= 0; i < p_.filters.size(); i++)
  {
    const gfilter &gf= p_.filters[i];
    TFAggFilter t{};
    t.col= gf.col;
    if (gf.exact)
    {
      int r= build_exact(gf, &sets[i]);
      if (r)
        return r;
      exact_set &s= sets[i];
      if (s.ints.empty() && s.lens.empty())
      {
        moosedb_gbh_phase(us_since(t_dict), 0, 0, 0, 0);
        return 0;                           /* nothing can match: no groups */
      }
      t.mode= TF_AGG_FILTER_VALUES;
      if (!gf.is_str)
      {
        t.int_values= s.ints.data();
        t.int_count= s.ints.size();
      }
      else
      {
        t.value_starts= s.starts.data();
        t.value_lens= s.lens.data();
        t.value_count= s.lens.size();
        t.value_data= (const uint8_t *) s.data.data();
        t.value_data_len= s.data.size();
        t.pad_space= gf.pad ? 1 : 0;
      }
    }
    else
    {
      int r= build_mask(gf, &masks[i], &dict_values);
      if (r)
        return r;
      if (std::find(masks[i].begin(), masks[i].end(), (uint8_t) 1) == masks[i].end())
      {
        moosedb_gbh_phase(us_since(t_dict), 0, dict_values, eval_us_, 0);
        return 0;                           /* no value passes: no groups */
      }
      t.mask= masks[i].data();
      t.mask_len= masks[i].size();
    }
    tfs.push_back(t);
  }
  const auto t_agg= clk::now();
  const uint64_t dict_us= (uint64_t) std::chrono::duration_cast<std::chrono::microseconds>(t_agg - t_dict).count();

  TFAggSpec spec{};
  spec.keys= keys.empty() ? nullptr : keys.data();
  spec.key_count= (uint32_t) keys.size();
  spec.funcs= p_.funcs.empty() ? nullptr : p_.funcs.data();
  spec.func_count= (uint32_t) p_.funcs.size();
  spec.filters= tfs.empty() ? nullptr : tfs.data();
  spec.filter_count= (uint32_t) tfs.size();
  spec.preds= hints_.empty() ? nullptr : hints_.data();
  spec.pred_count= (uint32_t) hints_.size();
  if (used_ >= max_memory_)
    return budget_exceeded();
  spec.max_memory= max_memory_ - used_;
  spec.threads= std::max<uint>(1, moosedb_agg_threads_value());
  if (p_.top_func >= 0)
  {
    spec.top_mode= p_.top_desc ? TF_AGG_TOP_DESC : TF_AGG_TOP_ASC;
    spec.top_func= (uint32_t) p_.top_func;
    spec.top_limit= p_.limit;
  }
  {
    uint64_t nex= 0;
    for (const gfilter &gf : p_.filters)
      nex+= gf.exact;
    moosedb_gbh_shortcuts(nex, p_.top_func >= 0 ? 1 : 0);
  }
  TFStatus st= moosedb_agg_run(agg_.get(), &spec, cancel_cb, thd);
  if (st != TF_OK)
    return fail_status(st);

  TFAggBatch b{};
  st= moosedb_agg_result(agg_.get(), 0, (uint64_t) 1 << 40, &b);
  if (st != TF_OK)
    return fail_status(st);
  const auto t_post= clk::now();
  const uint64_t agg_us= (uint64_t) std::chrono::duration_cast<std::chrono::microseconds>(t_post - t_agg).count();
  int r= build_groups(b);
  if (r)
    return r;
  agg_.reset();                             /* the snapshot is not needed any more */
  if (p_.need_merge && (r= merge_groups()))
    return r;
  r= order_groups();
  moosedb_gbh_phase(dict_us, agg_us, dict_values, eval_us_, us_since(t_post));
  return r;
}

/*
  Value set of an exact filter (see exact_shape): the constants are evaluated
  now (parameters are bound at EXECUTE). A NULL never matches. Integers keep
  the mathematical value: out of the column's range = cannot match = dropped;
  the core stores BIGINT UNSIGNED as the int64 bit pattern.
*/
int moosedb_gbh::build_exact(const gfilter &f, exact_set *out)
{
  Field *fld= f.field;
  if (!fld || !out || f.consts.empty())
    return fail("internal: exact filter without values");
  const uint bits= fld->pack_length() * 8;
  const bool fu= fld->is_unsigned();
  if (!f.is_str && (bits < 8 || bits > 64))
    return fail("internal: unexpected integer width");
  for (Item *c : f.consts)
  {
    if (!c || (thd && thd->is_error()))
      return fail("internal: bad constant");
    if (f.is_str)
    {
      String tmp;
      String *s= c->val_str(&tmp);
      if (c->null_value || !s)
        continue;
      if (s->length() > UINT32_MAX || !charge(s->length() + 16))
        return HA_ERR_RECORD_FILE_FULL;
      out->starts.push_back(out->data.size());
      out->lens.push_back((uint32_t) s->length());
      out->data.append(s->ptr(), s->length());
    }
    else
    {
      const longlong v= c->val_int();
      if (c->null_value)
        continue;
      const __int128 m= (c->unsigned_flag && v < 0) ? (__int128) (ulonglong) v : (__int128) v;
      const __int128 lo= fu ? 0 : -((__int128) 1 << (bits - 1));
      const __int128 hi= fu ? (((__int128) 1 << bits) - 1) : (((__int128) 1 << (bits - 1)) - 1);
      if (m < lo || m > hi)
        continue;
      if (!charge(sizeof(int64_t)))
        return HA_ERR_RECORD_FILE_FULL;
      out->ints.push_back((int64_t) (uint64_t) m);
    }
  }
  return 0;
}

/* Puts the table's first record back as it was (the dictionary phase overwrites one column of it). */
struct record_keeper
{
  TABLE *t;
  std::string saved;
  explicit record_keeper(TABLE *t_arg) : t(t_arg)
  {
    if (t && t->record[0] && t->s)
      saved.assign((const char *) t->record[0], t->s->reclength);
  }
  ~record_keeper()
  {
    if (t && t->record[0] && !saved.empty())
      memcpy(t->record[0], saved.data(), saved.size());
  }
};

/*
  Dictionary phase of one filter column. The engine lists the distinct values of
  the column over the WHERE's time range and series (same snapshot as the final
  run); each one is stored in the column's own Field of the table's record, the
  server's Items of the predicate (val_int, NULL = not true, as in a WHERE) decide,
  and the verdicts become the mask the engine filters by.

  The server does not re-check the WHERE on the engine's groups, so the Items
  themselves -- not a reimplementation -- must decide: collation, PAD SPACE,
  LIKE, NULL logic and every conversion are exactly the server's.

  Cost: one Item evaluation per DISTINCT value (a column with millions of
  distinct values pays that many evaluations, ~1 us each).
  (`col = const` / `col IN (consts)` on a byte-exact comparison never gets here:
  it is sent to the core as a value set, see exact_shape / build_exact.)
*/
int moosedb_gbh::build_mask(const gfilter &f, std::vector<uint8_t> *mask,
                            uint64_t *values)
{
  TABLE *src= p_.table;
  Field *fld= f.field;
  if (!src || !src->record[0] || !src->s || !fld || !agg_ || !mask || !values ||
      f.conds.empty())
    return fail("internal: filter without a column");
  TFAggKey key{TF_AGG_KEY_COLUMN, f.col};
  TFAggSpec spec{};
  spec.keys= &key;
  spec.key_count= 1;
  spec.preds= hints_.empty() ? nullptr : hints_.data();
  spec.pred_count= (uint32_t) hints_.size();
  if (used_ >= max_memory_)
    return budget_exceeded();
  spec.max_memory= max_memory_ - used_;
  spec.threads= std::max<uint>(1, moosedb_agg_threads_value());
  TFStatus st= moosedb_agg_run(agg_.get(), &spec, cancel_cb, thd);
  if (st != TF_OK)
    return fail_status(st);
  TFAggBatch b{};
  st= moosedb_agg_result(agg_.get(), 0, (uint64_t) 1 << 40, &b);
  if (st != TF_OK)
    return fail_status(st);
  const uint64_t n= b.row_count;
  if (b.key_count != 1 || !b.keys || b.total_groups != n)
    return fail("internal: dictionary has an unexpected shape");
  mask->clear();
  if (n == 0)
    return 0;
  const TFAggKeyBatch &kb= b.keys[0];
  if (f.is_str ? (kb.kind != TF_AGG_KEYCOL_BYTES || !kb.starts || !kb.lens)
               : (kb.kind != TF_AGG_KEYCOL_INT || !kb.values))
    return fail("internal: bad dictionary column");
  if (!charge(n))
    return HA_ERR_RECORD_FILE_FULL;
  mask->assign((size_t) n, 0);
  *values+= n;

  record_keeper keep(src);
  MY_BITMAP *old_r= dbug_tmp_use_all_columns(src, &src->read_set);
  MY_BITMAP *old_w= dbug_tmp_use_all_columns(src, &src->write_set);
  int rc= 0;
  const auto t_eval= std::chrono::steady_clock::now();
  for (uint64_t g= 0; g < n && !rc; g++)
  {
    if ((g & 0x3FF) == 0 && check_killed())
    {
      rc= HA_ERR_QUERY_INTERRUPTED;
      break;
    }
    if (kb.nulls && kb.nulls[g])
    {
      if (!fld->real_maybe_null())
      {
        rc= fail("internal: NULL in the dictionary of a NOT NULL column");
        break;
      }
      fld->set_null();
    }
    else
    {
      if (fld->real_maybe_null())
        fld->set_notnull();
      int sr;
      if (f.is_str)
      {
        if ((!kb.data && kb.lens[g]) || kb.starts[g] + kb.lens[g] > kb.data_len)
        {
          rc= fail("internal: bad dictionary string");
          break;
        }
        sr= fld->store(kb.lens[g] ? (const char *) kb.data + kb.starts[g] : "",
                       kb.lens[g], fld->charset());
      }
      else
        sr= fld->store((longlong) static_cast<const int64_t *>(kb.values)[g],
                       fld->is_unsigned());
      if (sr)
      {
        rc= fail("a stored value of a filtered column does not convert back for the WHERE evaluation (ill-formed data?); run the query with moosedb_group_by_pushdown=OFF");
        break;
      }
    }
    bool pass= true;
    for (Item *c : f.conds)
      if (!c->val_int() || c->null_value)
      {
        pass= false;
        break;
      }
    if (thd->is_error())
    {
      rc= HA_ERR_INTERNAL_ERROR;      /* the server already set the error */
      break;
    }
    (*mask)[(size_t) g]= pass ? 1 : 0;
  }
  eval_us_+= us_since(t_eval);
  dbug_tmp_restore_column_map(&src->write_set, old_w);
  dbug_tmp_restore_column_map(&src->read_set, old_r);
  return rc;
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
  /* Every group the core returns has at least one row (but the one group of a scalar aggregate); a COUNT(*) column lets us check it cheaply. */
  for (size_t fi= 0; fi < nf_ && !p_.scalar; fi++)
    if (p_.funcs[fi].op == TF_AGG_COUNT_STAR)
      for (uint64_t g= 0; g < ngroups_; g++)
        if (!ac((uint32_t) g, (int) fi).count)
          return fail("internal: the aggregation returned an empty group");
  live_.resize((size_t) ngroups_);
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
  /* (hash, group) pairs sorted contiguously: no indirection into a hash array during the sort */
  if (!charge(live_.size() * sizeof(std::pair<uint64_t, uint32_t>)))
    return HA_ERR_RECORD_FILE_FULL;
  std::vector<std::pair<uint64_t, uint32_t>> hv;
  hv.reserve(live_.size());
  for (size_t m= 0; m < live_.size(); m++)
  {
    hv.emplace_back(key_hash(live_[m]), live_[m]);
    if ((m & 0xFFFF) == 0 && check_killed())
      return HA_ERR_QUERY_INTERRUPTED;
  }
  std::sort(hv.begin(), hv.end());
  for (size_t m= 0; m < hv.size(); m++)
    live_[m]= hv[m].second;

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
    while (j < live_.size() && hv[j].first == hv[i].first)
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
    parallel_sort(live_, less, std::max<uint>(1, moosedb_agg_threads_value()));
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
