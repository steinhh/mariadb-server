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
#include <mysql_com.h>

#define FULL_LOGGING
#include "bucket.h"
#include "logging.h"
#define HA_HANDLER_CC
#include "ha_handler.h"
#define BUCKET ((Bucket *)share->bucket)

/*****************************************************************************
 ** HANDLER
 ******************************************************************************/

ha_handler::ha_handler(handlerton *h, TABLE_SHARE *t, MEI *m)
    : handler(h, t), std_errkey(0), write_can_replace(0), ignore_dup_key(0),
      keyread(0) // EXTRA's
{
  mei = m;
  DBUG_ENTER("ha_handler::ha_handler");
  DBUG_VOID_RETURN;
}

ha_handler::~ha_handler()
{
  DBUG_ENTER("ha_handler::~ha_handler");
  DBUG_VOID_RETURN;
}

const char *ha_handler::index_type(uint key_number) //////////////////////////
{
  DBUG_ENTER("::index_type");
  DBUG_RETURN(
      (table_share->key_info[key_number].flags & HA_FULLTEXT)  ? "FULLTEXT"
      : (table_share->key_info[key_number].flags & HA_SPATIAL) ? "SPATIAL"
      : (table_share->key_info[key_number].algorithm == HA_KEY_ALG_RTREE)
          ? "RTREE"
          : "BTREE");
}

// #include <my_stacktrace.h>

int ha_handler::info(uint flag) //////////??
{
  DBUG_ENTER1("ha_handler::info [%s]", share->table_name);
  table->status = 0;
  if (flag & HA_STATUS_ERRKEY)
  {
    errkey = std_errkey;
    LOG1("errkey: %d\n", errkey);
    if (!(flag & ~(HA_STATUS_ERRKEY | HA_STATUS_NO_LOCK)))
      DBUG_RETURN(0);
  }
  if (flag & HA_STATUS_NO_LOCK)
    LOGG("ha_handler::info(HA_STATUS_NO_LOCK)\n");
  if (flag & HA_STATUS_TIME)
    LOGG("ha_handler::info(HA_STATUS_TIME)\n");
  if (flag & HA_STATUS_CONST)
    LOGG("ha_handler::info(HA_STATUS_CONST)\n");
  if (flag & HA_STATUS_VARIABLE)
    LOGG("ha_handler::info(HA_STATUS_VARIABLE)\n");
  if (flag & HA_STATUS_ERRKEY)
    LOGG("ha_handler::info(HA_STATUS_ERRKEY)\n");
  if (flag & HA_STATUS_AUTO)
    LOGG("ha_handler::info(HA_STATUS_AUTO)\n");
  uint known_flags = HA_STATUS_AUTO | HA_STATUS_ERRKEY | HA_STATUS_VARIABLE |
                     HA_STATUS_CONST | HA_STATUS_TIME | HA_STATUS_NO_LOCK;
  if (flag & ~known_flags)
    LOG1("ha_handler::info(?????%d)\n", flag & ~known_flags);

    // Set & print stats field in one simple line
#define PPSET(name, val) \
  stats.name = val;      \
  LOG1(#name "=%llu\n", (ulonglong)stats.name)

  PPSET(data_file_length, BUCKET->data_file_length());
  PPSET(max_data_file_length, 1024 * 1024ULL * 1024);
  PPSET(index_file_length, BUCKET->data_file_length());
  PPSET(max_index_file_length, 1024 * 1024ULL * 1024);
  PPSET(delete_length, 0); // Free bytes
  PPSET(auto_increment_value, BUCKET->autoincrement_value());
  PPSET(records, BUCKET->records());
  PPSET(deleted, BUCKET->deleted());
  PPSET(mean_rec_length, BUCKET->mean_rec_length());
  PPSET(create_time, BUCKET->ctime);
  PPSET(update_time, BUCKET->mtime);
  PPSET(check_time, BUCKET->atime);
  if (BUCKET->sleeping())
    PPSET(check_time, 0);
  PPSET(block_size, 1024);
  DBUG_RETURN(0);
}

void ha_handler::log_lock(enum thr_lock_type lock_type)
{
#define PLO(l)             \
  else if (lock_type == l) \
  {                        \
    LOGG(#l "\n");         \
  }
  if (0)
  {
  }
  PLO(TL_IGNORE)
  PLO(TL_UNLOCK)
  PLO(TL_READ_DEFAULT)
  PLO(TL_READ)
  PLO(TL_READ_WITH_SHARED_LOCKS)
  PLO(TL_READ_HIGH_PRIORITY)
  PLO(TL_READ_NO_INSERT)
  PLO(TL_WRITE_ALLOW_WRITE)
  //  PLO(TL_WRITE_ALLOW_READ)
  PLO(TL_WRITE_CONCURRENT_INSERT)
  PLO(TL_WRITE_DELAYED)
  PLO(TL_WRITE_LOW_PRIORITY)
  PLO(TL_WRITE)
  PLO(TL_WRITE_ONLY)
  else LOG2("TL_LOCK???? = %d, %d\n", lock_type, lock_type == TL_WRITE_DELAYED);
}

/* The following is parameter to ha_extra() */
void ha_handler::log_extra_operation(enum ha_extra_function op)
{
  // DBUG_ENTER("ha_handler::log_extra_operation");
#define LEO(v) \
  if (op == v) \
    LOG1(#v ": %llu\n", (ulonglong)op);
  LEO(HA_EXTRA_NORMAL);
  LEO(HA_EXTRA_QUICK);
  LEO(HA_EXTRA_NOT_USED);
  LEO(HA_EXTRA_CACHE);
  LEO(HA_EXTRA_NO_CACHE);
  LEO(HA_EXTRA_NO_READCHECK);
  LEO(HA_EXTRA_READCHECK);
  LEO(HA_EXTRA_KEYREAD);
  LEO(HA_EXTRA_NO_KEYREAD);
  LEO(HA_EXTRA_NO_USER_CHANGE);
  LEO(HA_EXTRA_KEY_CACHE);
  LEO(HA_EXTRA_NO_KEY_CACHE);
  LEO(HA_EXTRA_WAIT_LOCK);
  LEO(HA_EXTRA_NO_WAIT_LOCK);
  LEO(HA_EXTRA_WRITE_CACHE);
  LEO(HA_EXTRA_FLUSH_CACHE);
  LEO(HA_EXTRA_NO_KEYS);
  LEO(HA_EXTRA_KEYREAD_CHANGE_POS);
  LEO(HA_EXTRA_REMEMBER_POS);
  LEO(HA_EXTRA_RESTORE_POS);
  LEO(HA_EXTRA_REINIT_CACHE);
  LEO(HA_EXTRA_FORCE_REOPEN);
  LEO(HA_EXTRA_FLUSH);
  LEO(HA_EXTRA_NO_ROWS);
  LEO(HA_EXTRA_RESET_STATE);
  LEO(HA_EXTRA_IGNORE_DUP_KEY);
  LEO(HA_EXTRA_NO_IGNORE_DUP_KEY);
  LEO(HA_EXTRA_PREPARE_FOR_DROP);
  LEO(HA_EXTRA_PREPARE_FOR_UPDATE);
  LEO(HA_EXTRA_PRELOAD_BUFFER_SIZE);
  LEO(HA_EXTRA_CHANGE_KEY_TO_UNIQUE);
  LEO(HA_EXTRA_CHANGE_KEY_TO_DUP);
  LEO(HA_EXTRA_KEYREAD_PRESERVE_FIELDS);
  LEO(HA_EXTRA_MMAP);
  LEO(HA_EXTRA_IGNORE_NO_KEY);
  LEO(HA_EXTRA_NO_IGNORE_NO_KEY);
  LEO(HA_EXTRA_MARK_AS_LOG_TABLE);
  LEO(HA_EXTRA_WRITE_CAN_REPLACE);
  LEO(HA_EXTRA_WRITE_CANNOT_REPLACE);
  LEO(HA_EXTRA_DELETE_CANNOT_BATCH);
  LEO(HA_EXTRA_UPDATE_CANNOT_BATCH);
  LEO(HA_EXTRA_INSERT_WITH_UPDATE);
  LEO(HA_EXTRA_PREPARE_FOR_RENAME);
  LEO(HA_EXTRA_ADD_CHILDREN_LIST);
  LEO(HA_EXTRA_ATTACH_CHILDREN);
  LEO(HA_EXTRA_IS_ATTACHED_CHILDREN);
  LEO(HA_EXTRA_DETACH_CHILDREN);
  // DBUG_VOID_RETURN;
}

int ha_handler::extra(enum ha_extra_function operation)
{
  DBUG_ENTER1("ha_handler::extra [%s]", share->table_name);
  if (table)
    table->status = 0;
  log_extra_operation(operation);
  if (operation == HA_EXTRA_IGNORE_DUP_KEY)
    ignore_dup_key = 1;
  else if (operation == HA_EXTRA_NO_IGNORE_DUP_KEY)
    ignore_dup_key = 0;
  else if (operation == HA_EXTRA_WRITE_CAN_REPLACE)
    write_can_replace = 1;
  else if (operation == HA_EXTRA_WRITE_CANNOT_REPLACE)
    write_can_replace = 0;
  else if (operation == HA_EXTRA_KEYREAD)
  {
    if (table)
      table->status = STATUS_GARBAGE; // Should this be set by default?
    keyread = 1;
  }
  else if (operation == HA_EXTRA_NO_KEYREAD)
  {
    keyread = 0;
    LOGG("CONSIDER table->status = STATUS_GARBAGE\n");
  }
  else
  {
    LOGG("WARNING: UNHANDLED extra() OPERATION!\n");
  }
  LOG2("igndupkey=%d, writcanrep=%d\n", ignore_dup_key, write_can_replace);
  DBUG_RETURN(0);
}

int ha_handler::extra_opt(enum ha_extra_function operation, ulong cache_size)
{
  DBUG_ENTER("ha_handler::extra_opt");
  DBUG_RETURN(extra(operation));
}

// Log-Key-Flag macro:
#define LKF(flag)   \
  if (flags & flag) \
  LOG1("%s: " #flag "\n", key->name.str)
void ha_handler::log_key_flags(KEY *key)
{
  IFACE_LOG_LOCAL;
  int flags = key->flags;
  DBUG_ENTER("ha_handler::log_key_flags");
  LKF(HA_NOSAME);
  LKF(HA_PACK_KEY);
  LKF(HA_AUTO_KEY);
  LKF(HA_BINARY_PACK_KEY);
  LKF(HA_FULLTEXT);
  LKF(HA_UNIQUE_CHECK);
  LKF(HA_SPATIAL);
  LKF(HA_NULL_ARE_EQUAL);
  LKF(HA_GENERATED_KEY);
  DBUG_VOID_RETURN;
}

// Log-Field-Flags macro:
#define LFF(flag)   \
  if (flags & flag) \
  LOG1("%s: " #flag "\n", f->field_name.str)
void ha_handler::log_field_flags(Field *f)
{
  DBUG_ENTER("ha_handler::log_field_flags");
  int flags = f->flags;
  LFF(NOT_NULL_FLAG);
  LFF(PRI_KEY_FLAG);
  LFF(UNIQUE_KEY_FLAG);
  LFF(MULTIPLE_KEY_FLAG);
  LFF(BLOB_FLAG);
  LFF(UNSIGNED_FLAG);
  LFF(ZEROFILL_FLAG);
  LFF(BINARY_FLAG);
  LFF(ENUM_FLAG);
  LFF(AUTO_INCREMENT_FLAG);
  LFF(TIMESTAMP_FLAG);
  LFF(SET_FLAG);
  LFF(NO_DEFAULT_VALUE_FLAG);
  LFF(ON_UPDATE_NOW_FLAG);
  LFF(NUM_FLAG);
  LFF(PART_KEY_FLAG);
  LFF(GROUP_FLAG);
  LFF(UNIQUE_KEY_FLAG); //! Causes error
  LFF(BINCMP_FLAG);
  LFF(GET_FIXED_FIELDS_FLAG);
  LFF(FIELD_IN_PART_FUNC_FLAG);
  LFF(FIELD_IN_ADD_INDEX);
  LFF(FIELD_IS_RENAMED);
  DBUG_VOID_RETURN;
}

int ha_handler::external_lock(THD *thd, int lock_type) ////////////////////
{
  DBUG_ENTER1("ha_handler::external_lock [%s]", share->table_name);
  switch (lock_type)
  {
  case F_UNLCK:
    LOG1("%s: F_UNLCK\n", share->table_name);
    break;
  case F_RDLCK:
    LOG1("%s: F_RDLCK\n", share->table_name);
    break;
  case F_WRLCK:
    LOG1("%s: F_WRLCK\n", share->table_name);
    break;
  default:
    LOCALLOG(LOG2("%s: LOCK TYPE %d??\n", share->table_name, lock_type));
    CRASH("ha_handler::external_lock does not understand lock type!");
    break;
  }
  if (lock_type == F_UNLCK)
  {
    log_lock(lastlock);
    if (lastlock == TL_WRITE_ALLOW_WRITE || lastlock == TL_WRITE_DELAYED ||
        lastlock == TL_WRITE_LOW_PRIORITY || lastlock == TL_WRITE ||
        lastlock == TL_WRITE_ONLY)
    {
      BUCKET->bucket_lock();
      LOG1("F_UNLCK: save %s\n", share->table_name);
      BUCKET->save();
      BUCKET->bucket_lock_yield();
    }
    else
    {
      LOGG("F_UNLCK: nosave\n");
    }
  }

  if (lock_type == F_UNLCK)
    BUCKET->unelock();
  else
    BUCKET->elock();

  DBUG_RETURN(0);
}

void ha_handler::get_status(int concurrent_insert)
{
  DBUG_ENTER("ha_handler::get_status");
  if (concurrent_insert)
    LOGG("get_status::Concurrent insert\n");
  DBUG_VOID_RETURN;
}

void ha_handler::restore_status()
{
  DBUG_ENTER1("ha_handler::restore_status() [%s]", share->table_name);
  DBUG_VOID_RETURN;
}

void ha_handler::update_status()
{
  DBUG_ENTER1("ha_handler::update_status [%s]", share->table_name);

  BUCKET->bucket_lock();
  BUCKET->save(); // May not cause a save, but will prepare for it!
  BUCKET->bucket_lock_yield();

  DBUG_VOID_RETURN;
}

static my_bool get_status(void *status_param, my_bool concurrent_insert)
{
  ((ha_handler *)status_param)->get_status(concurrent_insert);
  return 0;
}

static void restore_status(void *status_param)
{
  ((ha_handler *)status_param)->restore_status();
}

static void update_status(void *status_param)
{
  ((ha_handler *)status_param)->update_status();
}

void ha_handler::thr_lock_data_init(void)
{
  DBUG_ENTER("ha_handler::thr_lock_data_init");
  ::thr_lock_data_init(&share->thr_lock, &lock_data, (void *)this);

  lock_data.lock->update_status = ::update_status;
  lock_data.lock->restore_status = ::restore_status;
  lock_data.lock->get_status = ::get_status;
  DBUG_VOID_RETURN;
}

THR_LOCK_DATA **ha_handler::store_lock(THD *thd, /////////////////////////////
                                       THR_LOCK_DATA **to,
                                       enum thr_lock_type lock_type)
{
  DBUG_ENTER1("ha_handler::store_lock [%s]", share->table_name);
  if (lock_type != TL_IGNORE && lock_data.type == TL_UNLOCK) // Minimal...
    lock_data.type = lock_type;

  log_lock(lock_type);
  if (lock_type == TL_WRITE_ALLOW_WRITE)
    lock_data.type = TL_WRITE;
  //  if (lock_type==TL_WRITE_ALLOW_READ) lock_data.type=TL_WRITE;
  if (lock_type == TL_WRITE_CONCURRENT_INSERT)
    lock_data.type = TL_WRITE;
  // if (lock_type==TL_WRITE_DELAYED) lock_data.type=TL_WRITE;
  // if (lock_type==TL_WRITE_LOW_PRIORITY) lock_data.type=TL_WRITE;
  if (lock_type == TL_READ)
    lock_data.type = TL_READ_NO_INSERT;
  log_lock(lock_data.type);
  *to++ = &lock_data;
  lastlock = lock_data.type;
  DBUG_RETURN(to);
}

const char *ha_handler::fft(enum ha_rkey_function find_flag) /////////////////////
{
  switch (find_flag)
  {
  case HA_READ_KEY_EXACT:
    return "ha_read_key_exact";
  case HA_READ_KEY_OR_NEXT:
    return "ha_read_key_or_next";
  case HA_READ_KEY_OR_PREV:
    return "ha_read_key_or_prev";
  case HA_READ_AFTER_KEY:
    return "ha_read_after_key";
  case HA_READ_BEFORE_KEY:
    return "ha_read_before_key";
  case HA_READ_PREFIX:
    return "ha_read_prefix";
  case HA_READ_PREFIX_LAST:
    return "ha_read_prefix_last";
  case HA_READ_PREFIX_LAST_OR_PREV:
    return "ha_read_prefix_last_or_prev";
  default:
    return "what??";
  }
}

int ha_handler::index_read(uchar *buf, const uchar *key, uint key_len,
                           enum ha_rkey_function find_flag)
{
  LOGG("Huh! ha_handler::index read!!!\n");
  return HA_ERR_UNSUPPORTED; // ? Or just 0? I suppose this must be virtual
}

int ha_handler::index_read_idx(uchar *buf, uint idx, const uchar *key,
                               uint key_len, enum ha_rkey_function find_flag)
{
  DBUG_ENTER("ha_handler::index_read_idx");
  LOG1("::index_read_idx(...%s)\n", fft(find_flag));
  ACTINX = idx;
  DBUG_RETURN(index_read(buf, key, key_len, find_flag));
}

void ha_handler::print_type(enum enum_field_types type)
{
#define LOG_IT(type)  \
  case type:          \
    LOGG(#type "\n"); \
    break;

  switch (type)
  {
    LOG_IT(MYSQL_TYPE_DECIMAL);
    LOG_IT(MYSQL_TYPE_TINY);
    LOG_IT(MYSQL_TYPE_SHORT);
    LOG_IT(MYSQL_TYPE_LONG);
    LOG_IT(MYSQL_TYPE_FLOAT);
    LOG_IT(MYSQL_TYPE_DOUBLE);
    LOG_IT(MYSQL_TYPE_NULL);
    LOG_IT(MYSQL_TYPE_TIMESTAMP);
    LOG_IT(MYSQL_TYPE_LONGLONG);
    LOG_IT(MYSQL_TYPE_INT24);
    LOG_IT(MYSQL_TYPE_DATE);
    LOG_IT(MYSQL_TYPE_TIME);
    LOG_IT(MYSQL_TYPE_DATETIME);
    LOG_IT(MYSQL_TYPE_YEAR);
    LOG_IT(MYSQL_TYPE_NEWDATE);
    LOG_IT(MYSQL_TYPE_VARCHAR);
    LOG_IT(MYSQL_TYPE_BIT);
    LOG_IT(MYSQL_TYPE_NEWDECIMAL);
    LOG_IT(MYSQL_TYPE_ENUM);
    LOG_IT(MYSQL_TYPE_SET);
    LOG_IT(MYSQL_TYPE_TINY_BLOB);
    LOG_IT(MYSQL_TYPE_MEDIUM_BLOB);
    LOG_IT(MYSQL_TYPE_LONG_BLOB);
    LOG_IT(MYSQL_TYPE_BLOB);
    LOG_IT(MYSQL_TYPE_VAR_STRING);
    LOG_IT(MYSQL_TYPE_STRING);
    LOG_IT(MYSQL_TYPE_GEOMETRY);
    LOG_IT(MYSQL_TYPE_DATETIME2);
    LOG_IT(MYSQL_TYPE_TIME2);
    LOG_IT(MYSQL_TYPE_TIMESTAMP2);
    LOG_IT(MYSQL_TYPE_BLOB_COMPRESSED);
    LOG_IT(MYSQL_TYPE_VARCHAR_COMPRESSED);
  }
}

uint ha_handler::checksum(void) const
{
  DBUG_ENTER("ha_handler::checksum");
  DBUG_RETURN(BUCKET->checksum());
}
