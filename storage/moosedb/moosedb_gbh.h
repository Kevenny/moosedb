/*
  MooseDB storage engine -- group_by_handler hook (aggregation done by the
  engine). See moosedb_gbh.cc.

  Copyright (c) 2026 Kevenny

  This program is free software; you can redistribute it and/or modify
  it under the terms of the GNU General Public License as published by
  the Free Software Foundation; version 2 of the License.
*/

#pragma once

class THD;
class handler;
class ha_moosedb;
class group_by_handler;
struct Query;

/* handlerton::create_group_by of MooseDB. */
group_by_handler *moosedb_create_group_by(THD *thd, Query *query);

/*
  Scope analysis: nullptr when the query is one the engine could aggregate,
  otherwise a static string with the reason it cannot. Pure: no side effects.
*/
const char *moosedb_gbh_scope_refusal(THD *thd, Query *query);

/* Implemented in ha_moosedb.cc (session variables and the handlerton). */
bool moosedb_group_by_pushdown_enabled(THD *thd);
bool moosedb_gbh_debug_enabled(THD *thd);
ha_moosedb *moosedb_handler_of(handler *file);
