/* Copyright (C) 2008-2009 S. V. H. Haugan

  This program is free software; you can redistribute it and/or modify
  it under the terms of the GNU General Public License as published by
  the Free Software Foundation; version 2 of the License.

  This program is distributed in the hope that it will be useful,
  but WITHOUT ANY WARRANTY; without even the implied warranty of
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
  GNU General Public License for more details.

  You should have received a copy of the GNU General Public License
  along with this program; if not, write to the Free Software
  Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA */

#ifdef USE_PRAGMA_IMPLEMENTATION
#pragma implementation // gcc: Class implementation
#endif

#define MYSQL_SERVER 1

// Changed for MySql 5.5:
#include "all-mysql-includes.h"

#define FULL_LOGGING
#include "bucket.h"
#include "logging.h"
#include "ha_handler.h"
#include "bitbucket.h"
#include "scalarrbucket.h"
#include "ha_scalarray.h"
#include "ha_scalarray_toolbox.h"

/* Static declarations for handlerton */

static handler *create_scalarray_handler(handlerton *hton, TABLE_SHARE *table,
                                         MEM_ROOT *mem_root)
{
  return new (mem_root) ha_scalarray(hton, table);
}

/* Static declarations for shared structures */

static MEI scalarray_engine_data;
static MEI *mei = &scalarray_engine_data;

// To control thread logging, we need a thread variable that's accessible to
// the logging system. And the thread variable must be MEI-specific! What we
// do is to declare one in each storage engine, and set
// mei->thread_logging_sysvar_ptr to point to it. This is picked up by the
// thread_logging() func in the logging system.

static MYSQL_THDVAR_ULONGLONG(thread_logging, PLUGIN_VAR_RQCMDARG, "???", NULL,
                              NULL, 0, 0, 128 * 1024 * 1024ULL * 1024, 0);

static void mei_init() // Getting errors w/static initialization
{
  scalarray_engine_data.engine_name = "scalarray" /*engine_name*/;
  scalarray_engine_data.eng_name = "A";
  // scalarray_engine_data.create_handler = scalarray_create_handler;
  // scalarray_engine_data.max_mem = 0;
  // scalarray_engine_data.logging = 0; DOH!!! These are set as sysvars!!!
  scalarray_engine_data.thread_logging_sysvar_ptr = (void *)&mysql_sysvar_thread_logging;
  scalarray_engine_data.mem_used = 0;
  scalarray_engine_data.hibernations = 0;
  scalarray_engine_data.wakeups = 0;
  scalarray_engine_data.locks = 0;
  pthread_mutex_init(&scalarray_engine_data.engine_mutex, MY_MUTEX_INIT_FAST);
  bzero((void *)&scalarray_engine_data.open_tables, sizeof(scalarray_engine_data.open_tables));
}

static const char *ha_scalarray_exts[] = {
    BMI, // Bit Map Index
    SAR, // Scalar ARray
    SAI, // Scalar Array Index
    NullS};

static int scalarray_init(void *hton_in)
{
  handlerton *hton = (handlerton *)hton_in;

  // Logfile init before any logging! Logfile init should only be done once,
  // so it must have an engine-independent pthread_once system *inside*
  // it. I.e. we can't do *that* part here (nor in e.g.  scalarray_init)!

  logfile_init();

  scalarray_engine_data.eng_name = "A"; // Used as log line prefix

  DBUG_ENTER("scalarray_init"); // Now we can tell the world!

  //! hton->state = SHOW_OPTION_YES; // Pardon me??
  hton->db_type = DB_TYPE_UNKNOWN;
  hton->create = create_scalarray_handler; //
  hton->flags = HTON_CAN_RECREATE;

  hton->tablefile_extensions = ha_scalarray_exts;

  scalarray_engine_data.engine_name = "scalarray";
  scalarray_engine_data.thread_logging_sysvar_ptr = (void *)&mysql_sysvar_thread_logging;
  scalarray_engine_data.mem_used = 0;
  scalarray_engine_data.hibernations = 0;
  scalarray_engine_data.wakeups = 0;
  scalarray_engine_data.locks = 0; // Is this one used?

  my_hash_init(PSI_INSTRUMENT_ME,
               &scalarray_engine_data.open_tables, // Hash of open tables
               &my_charset_bin, 32, 0, 0,          // Standard stuff
               (my_hash_get_key)bucket_get_key,    // get/free_key used by
               (my_hash_free_key)bucket_free_key,  // all engines: in bucket.cc
               0);                                 //

  pthread_mutex_init(&scalarray_engine_data.engine_mutex, MY_MUTEX_INIT_FAST);

  bucket_init(mei);
  scalarray_mei = mei;
  DBUG_RETURN(0);
}

static int scalarray_fini(void *hton)
{
  bucket_fini(mei); // **GLOBAL** mei!
  return 0;
}

static MYSQL_SYSVAR_ULONGLONG(max_mem, scalarray_engine_data.max_mem,
                              PLUGIN_VAR_RQCMDARG, "???", NULL, NULL,
                              1024 * 1024, 0, 4 * 1024 * 1024ULL * 1024, 0);

static MYSQL_SYSVAR_ULONGLONG(logging, scalarray_engine_data.logging,
                              PLUGIN_VAR_RQCMDARG, "???", NULL, NULL, 1, 0,
                              128 * 1024 * 1024ULL * 1024, 0);

static struct st_mysql_sys_var *scalarray_system_variables[] = {
    MYSQL_SYSVAR(max_mem),
    MYSQL_SYSVAR(logging),
    MYSQL_SYSVAR(thread_logging),
    NULL};

static SHOW_VAR scalarray_status[] = {
    {"Scalarray_mem_used", (char *)&scalarray_engine_data.mem_used, SHOW_LONG},
    {"Scalarray_hibernations", (char *)&scalarray_engine_data.hibernations, SHOW_LONG},
    {"Scalarray_wakeups", (char *)&scalarray_engine_data.wakeups, SHOW_LONG},
    {"Scalarray_locks", (char *)&scalarray_engine_data.locks, SHOW_LONG},
    {NullS, NullS, SHOW_LONG}};

struct st_mysql_storage_engine scalarray_storage_engine =
    {MYSQL_HANDLERTON_INTERFACE_VERSION};

maria_declare_plugin(example){
    MYSQL_STORAGE_ENGINE_PLUGIN,
    &scalarray_storage_engine,
    "SCALARRAY",
    "Brian Aker, MySQL AB",
    "Scalarray storage engine",
    PLUGIN_LICENSE_GPL,
    scalarray_init,               /* Plugin Init */
    scalarray_fini,               /* Plugin Deinit */
    0x0001,                       /* version number (0.1) */
    scalarray_status,             /* status variables */
    scalarray_system_variables,   /* system variables */
    "0.1",                        /* string version */
    MariaDB_PLUGIN_MATURITY_GAMMA /* maturity */
} maria_declare_plugin_end;

// mysql_declare_plugin(scalarray){
//     MYSQL_STORAGE_ENGINE_PLUGIN,
//     &bucket_storage_engine,
//     "SCALARRAY",
//     "S.V.H. Haugan, ITA, University of Oslo, Norway",
//     "Scalarray storage engine",
//     PLUGIN_LICENSE_GPL,
//     scalarray_init, /* Plugin Init */
//     scalarray_fini, /* Plugin Deinit */
//     0x0100 /* 1.0 */,
//     scalarray_status,           /* status variables                */
//     scalarray_system_variables, /* system variables                */
//     NULL,                       /* config options                  */
//     0                           /* flags */
// } mysql_declare_plugin_end;

/*****************************************************************************
 ** HANDLER
 ******************************************************************************/

#define TSTATUS \
  if (table)    \
  table->status = 0

ha_scalarray::ha_scalarray(handlerton *h, TABLE_SHARE *t)
    : ha_handler(h, t, &scalarray_engine_data), val_is_d(0), val_is_f(0), val_is_dec(0)
{
  ref_length = sizeof(ulonglong);
  DBUG_ENTER("ha_scalarray::ha_scalarray");
  TSTATUS;
  std_errkey = 2;
  DBUG_VOID_RETURN;
}

ha_scalarray::~ha_scalarray()
{
  DBUG_ENTER("ha_scalarray::~ha_scalarray");
  TSTATUS;
  DBUG_VOID_RETURN;
}

const char **ha_scalarray::bas_ext() const
{
  DBUG_ENTER("ha_scalarray::bas_ext");
  TSTATUS;
  DBUG_RETURN(ha_scalarray_exts);
}

ulonglong ha_scalarray::table_flags(void) const
{
  return (HA_CAN_SQL_HANDLER |
          HA_CAN_REPAIR |
          // HA_REQUIRE_PRIMARY_KEY |
          HA_PRIMARY_KEY_IN_READ_INDEX |
          HA_TABLE_SCAN_ON_INDEX |
          HA_HAS_NEW_CHECKSUM |
          HA_CAN_INSERT_DELAYED |
          HA_FILE_BASED |
          HA_NO_TRANSACTIONS |
          HA_HAS_RECORDS |
          HA_DUPLICATE_POS |
          HA_STATS_RECORDS_IS_EXACT);
}

ulong ha_scalarray::index_flags(uint inx, uint part, bool all_parts) const
{
  return ((table_share->key_info[inx].algorithm == HA_KEY_ALG_FULLTEXT)
              ? 0
              : (HA_READ_NEXT | HA_READ_PREV | HA_READ_RANGE | HA_READ_ORDER | HA_KEYREAD_ONLY));
}

// UNSUPP(test,feature) : If test, report feature as unsupported
#define UNSUPP(c, a)                               \
  if (c)                                           \
  {                                                \
    my_error(ER_CHECK_NOT_IMPLEMENTED, MYF(0), a); \
    LOG1("UNSUPP: %s\n", a);                       \
    DBUG_RETURN(HA_ERR_UNSUPPORTED);               \
  }

// Unsupported for singlecolumn tables
#define UNSUP1(c, a) \
  if (singlecol)     \
  UNSUPP(c, a)

// Unsupported for dual-column tables
#define UNSUP2(c, a) \
  if (!singlecol)    \
  UNSUPP(c, a)

int ha_scalarray::create(const char *name, TABLE *table_arg,
                         HA_CREATE_INFO *create_info)
{
  IFACE_LOG_LOCAL;
  DBUG_ENTER("ha_scalarray::create");
  TSTATUS;
  int error = 0;
  LOG2("::create(%s) [comment:%s]\n", name, table_share->comment.str);
  char name_buff[FN_REFLEN];
  File create_file;
  enum enum_field_types t; // Temporary short-hand
  KEY *key;
  KEY_PART_INFO *key_part;
  Field *keyfield;
  Field **field = table_arg->s->field;
  int must_have_unique_key = 0;

  LOG2("table=%llx, table, table_arg=%llx\n", (ulonglong)table, (ulonglong)table_arg);

  // Check column count: only one or two allowed
  UNSUPP(table_arg->s->fields < 1, "less than one column!");
  UNSUPP(table_arg->s->fields > 2, "more than two columns");

  // Record whether we're single-column or dual-column
  singlecol = table_arg->s->fields == 1;

  // Which column [coming from "above" is the "value" column?
  vc = singlecol ? 0 : 1;

  // Check columns for various things
  UNSUPP(field[0]->real_maybe_null(), "nullable columns");
  UNSUP2(field[1]->real_maybe_null(), "nullable columns");

  if (!singlecol)
  {
    log_field_flags(field[0]);
    t = field[0]->type();
    UNSUP2(t != MYSQL_TYPE_LONGLONG && t != MYSQL_TYPE_LONG &&
               t != MYSQL_TYPE_ENUM && t != MYSQL_TYPE_INT24 &&
               t != MYSQL_TYPE_SHORT && t != MYSQL_TYPE_TINY,
           "non-integer/enum first columns");
    UNSUP2(!(field[0]->flags & UNSIGNED_FLAG), "signed first columns");
  }

  // Check value column (two or one)
  log_field_flags(field[vc]);
  t = field[vc]->real_type();
  UNSUPP(t != MYSQL_TYPE_LONGLONG && t != MYSQL_TYPE_LONG &&
             t != MYSQL_TYPE_ENUM && t != MYSQL_TYPE_INT24 &&
             t != MYSQL_TYPE_SHORT && t != MYSQL_TYPE_TINY &&
             t != MYSQL_TYPE_DOUBLE && t != MYSQL_TYPE_FLOAT &&
             t != MYSQL_TYPE_NEWDECIMAL,
         "non-int/float/double/decimal/enum value columns");

  // Check key count
  UNSUP1(table_arg->s->keys > 0, "value-only tables with keys");
  UNSUP2(table_arg->s->keys < 1, "[id,val] tables with < 1 key (PRIMARY)");
  UNSUP2(table_arg->s->keys > 3, "[id,val] tables with > 3 keys");

  UNSUPP(t == MYSQL_TYPE_NEWDECIMAL && field[vc]->pack_length() > 8,
         "newdecimal fields with pack_length longer than 8 bytes");

  if (singlecol)
    goto SKIP_KEYCHECK; // GOTO!

  // First key

  key = table_arg->key_info;
  log_key_flags(key);
  UNSUPP(!(key->flags & HA_NOSAME), "non-unique first/primary keys");

  UNSUPP(key->user_defined_key_parts > 2, "more than two parts in one key");

  // First key, first part
  key_part = key->key_part;

  UNSUPP(key_part->fieldnr != 1, "column 2 as first part of index 1 ");

  keyfield = key_part->field;
  log_field_flags(keyfield);
  UNSUPP(!(keyfield->flags & PRI_KEY_FLAG), "non-primary-key first-columns");

  // Check second part of first key (if it exists)
  if (key->user_defined_key_parts == 2)
  {
    // If there's a second part to the first key,
    // we need to specify that the first column is unique by itself:
    must_have_unique_key = 1;
    key_part++;
    log_field_flags(key_part->field);
    UNSUPP(key_part->fieldnr != 2, "2nd part of first key on column 1");
  }

  // SECOND key (if it exists)
  if (table_arg->s->keys == 1)
    goto SKIP_KEYCHECK;

  key++;
  log_key_flags(key);
  UNSUPP(key->user_defined_key_parts > 2, "keys with > 2 parts");

  key_part = key->key_part;
  UNSUPP(key_part->fieldnr != 2, "column 1 as part 1 of index 2");

  keyfield = key_part->field;
  log_field_flags(keyfield);
  UNSUPP(keyfield->flags & AUTO_INCREMENT_FLAG, "auto-increment on 2nd column");

  if (key->user_defined_key_parts == 1)
    UNSUPP(key->flags & HA_NOSAME, "a unique index on 2nd column alone");

  if (key->user_defined_key_parts == 2)
  {
    // Second key, second part:
    key_part++;
    log_field_flags(key_part->field);
    UNSUPP(key_part->fieldnr != 1, "2nd part of second key on column 2");
  }

  // Third key (if it exists)
  UNSUPP(must_have_unique_key && table_arg->s->keys < 3,
         "a table without a unique index on column 1 [alone]");

  if (table_arg->s->keys == 2)
    goto SKIP_KEYCHECK;

  key++;
  log_key_flags(key);
  UNSUPP(key->user_defined_key_parts > 1, "third key with multiple parts");

  key_part = key->key_part;
  UNSUPP(key_part->fieldnr != 1, "third key not on first column");

  keyfield = key_part->field;
  log_field_flags(keyfield);

SKIP_KEYCHECK:

  // Checks ok, now for the files.

  for (const char **ext = bas_ext(); *ext && !error; ext++)
  {
    create_file = my_create(fn_format(name_buff, name, "", *ext, MY_REPLACE_EXT | MY_UNPACK_FILENAME),
                            0, O_RDWR | O_TRUNC, MYF(MY_WME));
    error = create_file < 0;
    if (!error)
      my_close(create_file, MYF(0));
  }

  if (error)
  {
    for (const char **ext = bas_ext(); *ext; ext++)
    {
      unlink(fn_format(name_buff, name, "", *ext,
                       MY_REPLACE_EXT | MY_UNPACK_FILENAME));
    }
    DBUG_RETURN(-1);
  }

  DBUG_RETURN(0);
}

int ha_scalarray::open(const char *name, int mode, uint test_if_locked)
{
  IFACE_LOG_LOCAL;
  DBUG_ENTER3("ha_scalarray::open(name=%s mode=%d test_if_locked=%u)", name, mode, test_if_locked);
  TSTATUS;

  if (table->s->keys == 3)
    std_errkey = 2;
  else
    std_errkey = 0;

  singlecol = table->s->fields == 1; // We're only a list of numbers?
  vc = singlecol ? 0 : 1;            // MySQL will think value is col=0

  int type = table->field[vc]->type();
  this->val_is_f = type == MYSQL_TYPE_FLOAT;        // VALUE=double
  this->val_is_dec = type == MYSQL_TYPE_NEWDECIMAL; // VALUE=longlong
  this->val_is_d = type == MYSQL_TYPE_DOUBLE;       // VALUE=double

  // CALC TYPECODE !

  uint ixlen;
  if (singlecol)
    ixlen = 8;
  else
  {
    ixlen = table->field[0]->pack_length();
    if (ixlen == 3)
      ixlen = 4; // Upgrade!
    if (ixlen >= 5)
      ixlen = 8; // Upgrade!
  }

  // For integral types, it's simple: length & sign matters
  //
  uint valen = table->field[vc]->pack_length();
  int signd = table->field[vc]->flags & UNSIGNED_FLAG ? 0 : 1;

  // Newdecimal => INT-like!
  if (type == MYSQL_TYPE_NEWDECIMAL)
  {
    signd = 0; // Value must be treated as unsignd, no matter what!
    if (valen > 8)
    {
      my_error(ER_CHECK_NOT_IMPLEMENTED, MYF(0), "large decimal fields");
      DBUG_RETURN(HA_ERR_CRASHED_ON_USAGE);
    }
  }

  // Adjust value length to match a native data type:
  if (valen == 3)
    valen = 4; // 1,2 ok, 3 is not!
  if (valen >= 5)
    valen = 8; // 5,6,7 upgrades to 8

  // Floats => typecode 15, doubles => typecode 19

  if (type == MYSQL_TYPE_FLOAT)
  {
    valen = 5;
    signd = 1; // Unsignd floats allowed, but invisible at this level
  }

  // Doubles => typecode 19
  if (type == MYSQL_TYPE_DOUBLE)
  {
    valen = 9;
    signd = 1; // Unsignd doubles allowed, but invisible at this level
  }

  typecode = 100 * ixlen + 10 * signd + valen;

  LOG4("ixlen=%d, signd=%d, valen=%d, typecode=%d\n", ixlen, signd, valen, typecode);

  // CREATE & open correct TOOLBOX W/CASE STMT

  int res = 0;
  switch (typecode)
  {

#define TYPECODE_EXPANSION(itype, vtype, typecode, extra)                    \
  case typecode:                                                             \
  {                                                                          \
    toolbox.b##typecode = new ha_scalarray_toolbox<itype, vtype>(this, mei); \
    res = toolbox.b##typecode->open(name, mode, test_if_locked);             \
    break;                                                                   \
  }

#include "typecode_expansion.h"

    ALL_TYPECODE_EXPANSIONS()

#undef TYPECODE_EXPANSION // Tidy up after last define

  default:
    LOCALLOG(LOG1("Ouch!! Can't create toolbox w/ typecode %d\n", typecode));
    CRASH("Can't create toolbox with the given typecode");
  }

  DBUG_RETURN(res);
}

#define TYPECODE_EXPANSION(IT, VT, tcode, method) \
  case tcode:                                     \
    res = toolbox.b##tcode->method;               \
    break;
#include "typecode_expansion.h"
#define TB_CALL_RES(method) ALL_TYPECODE_EXPANSIONS(method)

// TAKEN OUT!!:
//    DBUG_ENTER1(ha_scalarray::method [%s],toolbox->name);
//    DBUG_RETURN( res );
#define PASS_THROUGH_RES(ret_type, decl, method) \
  ret_type ha_scalarray::decl                    \
  {                                              \
    TSTATUS;                                     \
    ret_type res = (ret_type)0;                  \
    switch (typecode)                            \
    {                                            \
      TB_CALL_RES(method);                       \
    }                                            \
    return res;                                  \
  }

PASS_THROUGH_RES(int, close(void), close())

PASS_THROUGH_RES(int, repair(THD *thd, HA_CHECK_OPT *opt), repair(thd, opt))

PASS_THROUGH_RES(int, check(THD *thd, HA_CHECK_OPT *opt), check(thd, opt))

PASS_THROUGH_RES(int, end_bulk_insert(void), end_bulk_insert())

PASS_THROUGH_RES(int, write_row(const uchar *row), write_row(row))

PASS_THROUGH_RES(bool, start_bulk_update(void), start_bulk_update());

PASS_THROUGH_RES(int,
                 bulk_update_row(const uchar *old_row, const uchar *new_row,
                                 ha_rows *dup_key_found),
                 bulk_update_row(old_row, new_row, dup_key_found))

PASS_THROUGH_RES(int, exec_bulk_update(ha_rows *dup_key_found),
                 exec_bulk_update(dup_key_found));

PASS_THROUGH_RES(bool, start_bulk_delete(void), start_bulk_delete())

PASS_THROUGH_RES(int, end_bulk_delete(), end_bulk_delete())

PASS_THROUGH_RES(int, rnd_init(bool scan), rnd_init(scan))

PASS_THROUGH_RES(int, rnd_next(uchar *buf), rnd_next(buf))

PASS_THROUGH_RES(int, rnd_prev(uchar *buf), rnd_prev(buf))

PASS_THROUGH_RES(int, rnd_pos(uchar *buf, uchar *pos), rnd_pos(buf, pos))

PASS_THROUGH_RES(int, index_init(uint idx, bool sorted), index_init(idx, sorted))

PASS_THROUGH_RES(ha_rows,
                 records_in_range(uint inx, const key_range *min_k, const key_range *max_k, page_range *pages),
                 records_in_range(inx, min_k, max_k, pages))

PASS_THROUGH_RES(int,
                 index_read(uchar *buf, const uchar *key, uint key_len, enum ha_rkey_function find_flag),
                 index_read(buf, key, key_len, find_flag))

PASS_THROUGH_RES(int,
                 index_read_last(uchar *buf, const uchar *key, uint key_len),
                 index_read_last(buf, key, key_len))

PASS_THROUGH_RES(int, index_next(uchar *buf), index_next(buf))

PASS_THROUGH_RES(int, index_prev(uchar *buf), index_prev(buf))

PASS_THROUGH_RES(int, index_first(uchar *buf), index_first(buf))

PASS_THROUGH_RES(int, index_last(uchar *buf), index_last(buf))

PASS_THROUGH_RES(int, update_row(const uchar *old_data, const uchar *new_data),
                 update_row(old_data, new_data))

PASS_THROUGH_RES(int, delete_row(const uchar *buf), delete_row(buf))

#undef TYPECODE_EXPANSION

// Define macros for invoking void toolbox methods via the right
// union members, based on typecode

#define TYPECODE_EXPANSION(IT, VT, tcode, method) \
  case tcode:                                     \
    toolbox.b##tcode->method;                     \
    break;
#include "typecode_expansion.h"
#define TB_CALL_VOID(method) ALL_TYPECODE_EXPANSIONS(method)

#define PASS_THROUGH_VOID(decl, method)   \
  void ha_scalarray::decl                 \
  {                                       \
    DBUG_ENTER("ha_scalarray::" #method); \
    TSTATUS;                              \
    switch (typecode)                     \
    {                                     \
      TB_CALL_VOID(method);               \
    }                                     \
    DBUG_VOID_RETURN;                     \
  }

PASS_THROUGH_VOID(start_bulk_insert(ha_rows rows, uint i_dont_know_what_this_is), start_bulk_insert(rows))

PASS_THROUGH_RES(int, end_bulk_update(), end_bulk_update())

PASS_THROUGH_VOID(position(const uchar *record), position(record))
