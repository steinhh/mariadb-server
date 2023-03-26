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
#include "all-mysql-includes.h"

#ifdef bitmap_init
#undef bitmap_init
#endif

#include "bucket.h"
#define FULL_LOGGING
#include "logging.h"
#include "ha_handler.h"
#include "bitbucket.h"
#include "ha_bitmap.h"

#define BITB_SHARE_BUCKET ((bitbucket *)share->bucket)

/* Static declarations for handlerton */

static handler *create_bitmap_handler(handlerton *hton, TABLE_SHARE *table,
                                      MEM_ROOT *mem_root)
{
  return new (mem_root) ha_bitmap(hton, table);
}

/* Static declarations for shared structures */

static MEI bitmap_engine_data;
static MEI *mei = &bitmap_engine_data;

static MYSQL_THDVAR_ULONGLONG(
    thread_logging,
    PLUGIN_VAR_RQCMDARG,
    "???",
    NULL,
    NULL,
    0,
    0,
    128 * 1024 * 1024ULL * 1024,
    0);

// List of extensions used - to ensure auto-deletion (used both in
// ::bas_ext() and - more importantly, perhaps(?), in bitmap_init
//
static const char *ha_bitmap_exts[] = {BMP, NullS};

// bitmap_init/_fini are invoked once each: startup and shutdown, thanks to
// their mention in mysql_declare_plugin(bitmap), below.

static int bitmap_init(void *hton_in)
{
  handlerton *hton = (handlerton *)hton_in;

  // Logfile init before any logging! Logfile init should only be done once,
  // so it must have an engine-independent pthread_once system *inside*
  // it. I.e. we can't do *that* part here (nor in e.g.  scalarray_init)!

  logfile_init();

  bitmap_engine_data.eng_name = "B"; // Used as log line prefix

  DBUG_ENTER("bitmap_init"); // Now we can tell the world!

  //! hton->state= SHOW_OPTION_YES; // Pardon me??
  hton->db_type = DB_TYPE_UNKNOWN;
  hton->create = create_bitmap_handler; //
  hton->flags = HTON_CAN_RECREATE;

  hton->tablefile_extensions = ha_bitmap_exts;

  bitmap_engine_data.engine_name = "bitmap";
  bitmap_engine_data.thread_logging_sysvar_ptr = (void *)&mysql_sysvar_thread_logging;
  bitmap_engine_data.mem_used = 0;
  bitmap_engine_data.hibernations = 0;
  bitmap_engine_data.wakeups = 0;
  bitmap_engine_data.locks = 0; // Is this one used?

  my_hash_init(PSI_INSTRUMENT_ME,
               &bitmap_engine_data.open_tables,   // Hash of open tables
               &my_charset_bin, 32, 0, 0,         // Standard stuff
               (my_hash_get_key)bucket_get_key,   // get/free_key used by
               (my_hash_free_key)bucket_free_key, // all engines: in bucket.cc
               0);                                //

  pthread_mutex_init(&bitmap_engine_data.engine_mutex, MY_MUTEX_INIT_FAST);

  bucket_init(mei);
  bitmap_mei = mei;
  DBUG_RETURN(0);
}

static int bitmap_fini(void *hton)
{
  DBUG_ENTER("bitmap_fini");

  my_hash_free(&bitmap_engine_data.open_tables);

  pthread_mutex_destroy(&bitmap_engine_data.engine_mutex);

  bucket_fini(mei); // Deletes the free_ids bitbucket

  LOG2("%s memory leak: %ld\n", mei->engine_name, mei->mem_used);

  DBUG_RETURN(0);
}

static MYSQL_SYSVAR_ULONGLONG(max_mem, bitmap_engine_data.max_mem,
                              PLUGIN_VAR_RQCMDARG, "???", NULL, NULL, 1024 * 1024, 0,
                              4 * 1024 * 1024ULL * 1024, 0);

static MYSQL_SYSVAR_ULONGLONG(logging, bitmap_engine_data.logging,
                              PLUGIN_VAR_RQCMDARG, "???", NULL, NULL, 1, 0,
                              128 * 1024 * 1024ULL * 1024, 0);

static struct st_mysql_sys_var *bitmap_system_variables[] = {
    MYSQL_SYSVAR(max_mem),
    MYSQL_SYSVAR(logging),
    MYSQL_SYSVAR(thread_logging),
    NULL};

static SHOW_VAR bitmap_status[] = {
    {"Bitmap_mem_used", (char *)&bitmap_engine_data.mem_used, SHOW_LONG},
    {"Bitmap_hibernations", (char *)&bitmap_engine_data.hibernations, SHOW_LONG},
    {"Bitmap_wakeups", (char *)&bitmap_engine_data.wakeups, SHOW_LONG},
    {NullS, NullS, SHOW_LONG}};

struct st_mysql_storage_engine bitmap_storage_engine = {
    MYSQL_HANDLERTON_INTERFACE_VERSION};

maria_declare_plugin(example){
    MYSQL_STORAGE_ENGINE_PLUGIN,
    &bitmap_storage_engine,
    "BITMAP",
    "S.V.H. Haugan, ITA, University of Oslo, Norway",
    "Bitmap storage engine",
    PLUGIN_LICENSE_GPL,
    bitmap_init,                  /* Plugin Init */
    bitmap_fini,                  /* Plugin Deinit */
    0x0001,                       /* version number (0.1) */
    bitmap_status,                /* status variables */
    bitmap_system_variables,      /* system variables */
    "0.1",                        /* string version */
    MariaDB_PLUGIN_MATURITY_GAMMA /* maturity */
} maria_declare_plugin_end;

// struct st_mysql_storage_engine scalarray_storage_engine =
//     {MYSQL_HANDLERTON_INTERFACE_VERSION};

// mysql_declare_plugin(bitmap) // MACRO!!!! Fucking lowercase MACRO!!
//     {
//         MYSQL_STORAGE_ENGINE_PLUGIN,
//         &bucket_storage_engine,
//         "BITMAP",
//         "S.V.H. Haugan, ITA, University of Oslo, Norway",
//         "Bitmap storage engine",
//         PLUGIN_LICENSE_GPL,
//         bitmap_init, /* Plugin Init */
//         bitmap_fini, /* Plugin Deinit */
//         0x0100 /* 1.0 */,
//         bitmap_status,           /* status variables                */
//         bitmap_system_variables, /* system variables                */
//         NULL,                    /* config options                  */
//         0                        /* flags */
//     } mysql_declare_plugin_end;

/*****************************************************************************
 ** HANDLER
 ******************************************************************************/

#define TSTATUS \
  if (table)    \
  table->status = 0

ha_bitmap::ha_bitmap(handlerton *h, TABLE_SHARE *t)
    : ha_handler(h, t, &bitmap_engine_data)
{
  ref_length = sizeof(ulonglong);
  DBUG_ENTER("ha_bitmap::ha_bitmap");
  TSTATUS;
  DBUG_VOID_RETURN;
}

ha_bitmap::~ha_bitmap()
{
  DBUG_ENTER("ha_bitmap::~ha_bitmap");
  TSTATUS;
  DBUG_VOID_RETURN;
}

const char **ha_bitmap::bas_ext() const
{
  DBUG_ENTER("ha_bitmap::bas_ext");
  TSTATUS;
  DBUG_RETURN(ha_bitmap_exts);
}

// This table does not have NULLs, so it has new checksums
//
ulonglong ha_bitmap::table_flags(void) const
{
  return (HA_CAN_SQL_HANDLER | HA_HAS_NEW_CHECKSUM | HA_FILE_BASED |
          HA_NO_TRANSACTIONS | HA_HAS_RECORDS | HA_STATS_RECORDS_IS_EXACT);
}

int ha_bitmap::open(const char *name, int mode, uint test_if_locked)
{
  IFACE_LOG_LOCAL;
  DBUG_ENTER("ha_bitmap::open");
  TSTATUS;
  LOG3("::open(%s,%d,%d)\n", name, mode, test_if_locked);

  // See note in bucket_get_share() about engine locking.

  ha_engine_lock();

  if (!(share = bucket_get_share(name, mei)))
  {
    LOCALLOG(LOGG("Ooops, bucket_get_share returned NULL!"));
    ha_engine_lock_yield();
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);
  }

  // The thr_lock is a MySQL thing - relates the handler (MySQL "parent"
  // class) lock_data to share->thr_lock. This thingy apparently does not need
  // to be "destroyed".

  this->thr_lock_data_init();

  // If the share already has a bucket there already, we're not creating a new
  // share - so we just lock_yield & return OK. No checking of the table.

  if (share->bucket)
  {
    ha_engine_lock_yield();
    DBUG_RETURN(0);
  }

  // New share - new bucket!

  share->bucket = new bitbucket(share->table_name, LOCK_LOCKED, bas_ext(), mei);

  //

  // The bucket is ready - register, but keep engine locked
  bucket_register_share(share, mei);

  // We want to check it before anyone else gets their hands on it, then
  // lock_yield & return if everything's ok

  // But remember to tell bucket about the opened engine!  This must
  // occur *before* engine is unlocked: the bitbucket might get
  // used by other threads for ops that require locking, resulting in
  // disaster (i.e.: it doesn't even attempt to lock, b/c it's been
  // told that it's already locked; we unlock it here, and it knows
  // nothing about it...

  if (!BITB_SHARE_BUCKET->is_crashed())
  {
    share->bucket->bucket_engine_lock_set(LOCK_OPEN);
    ha_engine_lock_yield();
    DBUG_RETURN(0);
  }

  // Oops. No, something went wrong.
  LOCALLOG(LOG1("Table crash on open: %s\n", name));

  // There won't be a "close" coming, so delete the bucket & free the share

  delete (bitbucket *)share->bucket;

  if (bucket_free_share(share, mei) != 0)
  {
    CRASHLOG("HEY - Other users of just opened %s? Doh!\n", name);
    CRASH("Won't continue\n");
  }

  ha_engine_lock_yield();
  DBUG_RETURN(HA_ERR_CRASHED_ON_USAGE);
}

int ha_bitmap::close(void)
{
  IFACE_LOG_LOCAL;
  DBUG_ENTER("ha_bitmap::close");
  TSTATUS;

  // If bucket_free_share() decides we're the last user, it'll delete the
  // share, so we can't reach the bucket. Need to keep pointer safe:

  bitbucket *bucket = BITB_SHARE_BUCKET;

  ha_engine_lock();
  int users = bucket_free_share(share, mei);
  ha_engine_lock_yield();

  if (users == 0)
    delete bucket;
  DBUG_RETURN(0);
}

int ha_bitmap::create(const char *name, TABLE *table_arg,
                      HA_CREATE_INFO *create_info)
{
  IFACE_LOG_LOCAL;
  DBUG_ENTER("ha_bitmap::create");
  TSTATUS;
  LOG2("::create(%s) [%s]\n", name, table_share->comment.str);
  char name_buff[FN_REFLEN];
  File create_file;
  enum enum_field_types t;
  KEY *key;
  KEY_PART_INFO *key_part;
  Field *ifield;
  Field **field = table_arg->s->field;

#define UNSUPP(c, a)                               \
  if (c)                                           \
  {                                                \
    my_error(ER_CHECK_NOT_IMPLEMENTED, MYF(0), a); \
    DBUG_RETURN(HA_ERR_UNSUPPORTED);               \
  }

  // Check column count: exactly two
  UNSUPP(table_arg->s->fields < 1, "less than one column");
  UNSUPP(table_arg->s->fields > 1, "more than one column");

  // Check column
  UNSUPP(field[0]->real_maybe_null(), "nullable columns");

  t = field[0]->type();
  UNSUPP(t != MYSQL_TYPE_LONGLONG && t != MYSQL_TYPE_LONG &&
             t != MYSQL_TYPE_ENUM && t != MYSQL_TYPE_INT24 &&
             t != MYSQL_TYPE_SHORT && t != MYSQL_TYPE_TINY,
         "non-integer/enum columns");

  UNSUPP(!(field[0]->flags & UNSIGNED_FLAG), "signed columns");

  UNSUPP(table_arg->s->keys != 1, "columns without keys");

  key = table_arg->key_info + 0;
  UNSUPP(key->user_defined_key_parts != 1, "tables with compound keys");

  key_part = key->key_part;
  ifield = key_part->field;
  UNSUPP(!(ifield->flags & PRI_KEY_FLAG), "non-primary or unique keys");
  LOG1("flags: %u\n", ifield->flags);

  if ((create_file = my_create(fn_format(name_buff, name, "", BMP,
                                         MY_REPLACE_EXT | MY_UNPACK_FILENAME),
                               0, O_RDWR | O_TRUNC, MYF(MY_WME))) < 0)
    DBUG_RETURN(-1);

  my_close(create_file, MYF(0));
  DBUG_RETURN(0);
}

int ha_bitmap::write_row(const uchar *buf)
{
  IFACE_NLL_ON_SPEEDTEST;
  DBUG_ENTER("ha_bitmap::write_row");
  TSTATUS;
  if (BITB_SHARE_BUCKET->is_crashed())
  {
    LOCALLOG(LOG1("Crashed-before-write: %s", share->table_name));
    DBUG_RETURN(HA_ERR_CRASHED_ON_USAGE);
  }
  longlong val;

  // Seems to be all that's required to make auto-increment work -
  // sweet!  We don't even need to set next_insert_id upon opening a
  // new file, since MySQL seems to do that for us by calling
  // index_last!

  if (table->next_number_field && buf == table->record[0])
  {
    if (int res = update_auto_increment())
      return res;
  }

  val = lget(0, buf + table->s->null_bytes);
  LOG3("val=%lld, null_bytes: %d, *buf: %d\n", val, table->s->null_bytes, *buf);
  //! ha_statistic_increment(&SSV::ha_write_count); //! No longer implemented in 10.5??
  LOGG("Hello\n");
  if (BITB_SHARE_BUCKET->isset(val))
    DBUG_RETURN(HA_ERR_FOUND_DUPP_KEY);
  LOGG("Hello2\n");
  BITB_SHARE_BUCKET->set(val);
  LOGG("Hello3\n");
  stats.records = BITB_SHARE_BUCKET->records(); // Not needed??
  LOGG("Hello4\n");
  DBUG_RETURN(0);
}

int ha_bitmap::update_row(const uchar *old_data, const uchar *new_data)
{
  DBUG_ENTER("::update_row");
  TSTATUS;
  DBUG_RETURN(HA_ERR_WRONG_COMMAND);
}

int ha_bitmap::delete_row(const uchar *buf)
{
  DBUG_ENTER("ha_bitmap::delete_row");
  TSTATUS;
  longlong val;
  val = lget(0, buf + table->s->null_bytes);

  //! ha_statistic_increment(&SSV::ha_delete_count); // No longer supported in 10.5?
  LOG1("::delete_row([%lld])\n", val);
  BITB_SHARE_BUCKET->unset(val);
  DBUG_RETURN(0);
}

int ha_bitmap::rnd_init(bool scan)
{
  DBUG_ENTER("ha_bitmap::rnd_init");
  TSTATUS;
  LOG1("::rnd_init(%d)\n", scan);
  table->status = 0;
  current_position = -1;
  DBUG_RETURN(0);
}

int ha_bitmap::rnd_next(uchar *buf)
{
  DBUG_ENTER("ha_bitmap::rnd_next");
  TSTATUS;
  memset(buf, 0, table->s->null_bytes);

  if (BITB_SHARE_BUCKET == NULL)
  {
    DEBUGLOG("Ooops! BITB_SHARE_BUCKET is NULL in ha_bitmap::rnd_next!\n");
    CRASH("Hate to crash, but what's the alternative!\n");
  }
  current_position = BITB_SHARE_BUCKET->next(current_position);

  LOGG("Got next current_position\n");

  if (current_position == HA_HANDLER_NOTFOUND)
    DBUG_RETURN(HA_ERR_END_OF_FILE);

  put(0, buf + table->s->null_bytes, current_position);
  DBUG_RETURN(0);
}

int ha_bitmap::rnd_prev(uchar *buf)
{
  DBUG_ENTER("ha_bitmap::rnd_prev");
  TSTATUS;
  memset(buf, 0, table->s->null_bytes);

  bitbucket *b = BITB_SHARE_BUCKET;

  current_position = b->prev(current_position);

  if (current_position < HA_HANDLER_NOTFOUND)
    DBUG_RETURN(HA_ERR_END_OF_FILE);

  put(0, buf + table->s->null_bytes, current_position);
  DBUG_RETURN(0);
}

int ha_bitmap::rnd_pos(uchar *buf, uchar *pos)
{
  DBUG_ENTER("ha_bitmap::rnd_pos");
  TSTATUS;
  current_position = (my_off_t)my_get_ptr(pos, ref_length);
  put(0, buf + table->s->null_bytes, current_position);
  DBUG_RETURN(0);
}

void ha_bitmap::position(const uchar *record)
{
  DBUG_ENTER("ha_bitmap::position");
  TSTATUS;
  ulonglong val;
  if (0)
  {
    val = lget(0, record + table->s->null_bytes);
    if (val != current_position)
      LOG2("%lld != %lld\n", val, current_position);
  }
  my_store_ptr(ref, ref_length, current_position);
  DBUG_VOID_RETURN;
}

ha_rows ha_bitmap::records_in_range(uint inx,
                                    const key_range *min_key,
                                    const key_range *max_key,
                                    page_range *pages)
{
  DBUG_ENTER("ha_bitmap::records_in_range");
  TSTATUS;
  LOG2("%s records_in_range(inx=%d,*min_key,*max_key)\n", share->table_name, inx);

  if (BITB_SHARE_BUCKET->records() == 0)
    DBUG_RETURN(0);

  ulonglong lower = HA_HANDLER_NOTFOUND;
  ulonglong upper = BITB_SHARE_BUCKET->max_value; // HA_HANDLER_NOTFOUND if empty

  if (min_key && min_key->key)
  {
    lower = lget(0, min_key->key);
    LOG1("lower=%lld\n", lower);
    if (min_key->flag == HA_READ_AFTER_KEY)
    {
      lower++;
      LOGG("lower++ b/c HA_READ_AFTER_KEY\n");
    }
    if (lower > BITB_SHARE_BUCKET->max_value)
      DBUG_RETURN(0);
  }

  if (max_key && max_key->key)
  {
    upper = lget(0, max_key->key);
    LOG1("upper=%lld\n", upper);
    if (max_key->flag == HA_READ_BEFORE_KEY)
    {
      if (upper == 0)
        DBUG_RETURN(0);
      upper--;
      LOGG("upper-- b/c HA_READ_BEFORE_KEY\n");
    }
    if (upper > BITB_SHARE_BUCKET->max_value)
      upper = BITB_SHARE_BUCKET->max_value;
  }

  double average_density = BITB_SHARE_BUCKET->records() / (double)BITB_SHARE_BUCKET->max_value;

  longlong nvals = (ulonglong)((upper - lower) * average_density);

  if (nvals <= 1)
    nvals = 2; // nvals==1 or 0 may be taken *literally* by MySQL

  LOG4("%s records_in_range(%lld,%lld) = %lld\n", share->table_name, lower, upper, nvals);

  DBUG_RETURN(nvals);
}

int ha_bitmap::index_read(uchar *buf, const uchar *key, uint key_len,
                          enum ha_rkey_function find_flag)
{
  DBUG_ENTER("ha_bitmap::index_read");
  table->status = STATUS_NOT_FOUND;
  current_position = lget(0, key);

  LOG3("%s::index_read(%s:%lld)\n", share->table_name, fft(find_flag), current_position);
  act_fflag = find_flag;
  memset(buf, 0, table->s->null_bytes);
  bitbucket *b = BITB_SHARE_BUCKET; // Shorthand

  switch (find_flag)
  {

  case HA_READ_KEY_EXACT:
    if (b->isset(current_position))
      break; // Found
    DBUG_RETURN(HA_ERR_KEY_NOT_FOUND);
    //====================

  case HA_READ_KEY_OR_NEXT:
    if (b->isset(current_position))
      break; // Finish if found, go on if not
             // fall through
  case HA_READ_AFTER_KEY:
    current_position = b->next(current_position);
    if (current_position != HA_HANDLER_NOTFOUND)
      break; // Ok, found
    DBUG_RETURN(HA_ERR_END_OF_FILE);
    //=====================

  case HA_READ_KEY_OR_PREV:
    if (b->isset(current_position))
      break;
    // fall through
  case HA_READ_BEFORE_KEY:
    current_position = b->prev(current_position);
    if (current_position != HA_HANDLER_NOTFOUND)
      break; // Ok, found
    DBUG_RETURN(HA_ERR_END_OF_FILE);
    //=====================

  default:
    LOGG("Ouch!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n");
    DBUG_RETURN(HA_ERR_UNSUPPORTED);
  }

  table->status = 0;
  put(0, buf + table->s->null_bytes, current_position);
  DBUG_RETURN(0);
}

int ha_bitmap::index_read_last(uchar *buf, const uchar *key, uint key_len)
{
  DBUG_ENTER("ha_bitmap::index_read_last");
  TSTATUS;
  LOGG("::index_read_last(...)\n");
  // With no duplicates, this is the same as a regular read...
  DBUG_RETURN(index_read(buf, key, key_len, HA_READ_KEY_EXACT));
}

int ha_bitmap::index_next(uchar *buf)
{
  DBUG_ENTER("ha_bitmap::index_next");
  TSTATUS;
  // LOG2("%s::index_next[%s]\n",share->table_name,fft((enum ha_rkey_function)act_fflag));
  if (act_fflag == HA_READ_KEY_EXACT)
  {
    LOG2("%s::index_next[%s]\n", share->table_name, fft((enum ha_rkey_function)act_fflag));
    DBUG_RETURN(HA_ERR_END_OF_FILE);
  }
  DBUG_RETURN(rnd_next(buf));
}

int ha_bitmap::index_prev(uchar *buf)
{
  DBUG_ENTER("ha_bitmap::index_prev");
  TSTATUS;
  DBUG_RETURN(rnd_prev(buf));
}

int ha_bitmap::index_first(uchar *buf)
{
  DBUG_ENTER("ha_bitmap::index_first");
  TSTATUS;
  current_position = -1;
  DBUG_RETURN(rnd_next(buf));
}

int ha_bitmap::index_last(uchar *buf)
{
  DBUG_ENTER("::index_last");
  TSTATUS;
  if (!BITB_SHARE_BUCKET->records())
    DBUG_RETURN(HA_ERR_END_OF_FILE);
  current_position = BITB_SHARE_BUCKET->max_value;
  put(0, buf + table->s->null_bytes, current_position);
  DBUG_RETURN(0);
}
