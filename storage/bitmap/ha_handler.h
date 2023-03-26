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
#ifndef HA_HANDLER_H
#define HA_HANDLER_H

#ifdef USE_PRAGMA_IMPLEMENTATION
#pragma interface // gcc: Class implementation
#endif

#define MYSQL_SERVER 1

#define FULL_LOGGING

/* GLOBAL mei pointers */
#ifdef HA_HANDLER_CC
#define HA_HANDLER_EXTERN
#define HA_HANDLER_INITIALIZATION(val) val
#else
#define HA_HANDLER_EXTERN extern
#define HA_HANDLER_INITIALIZATION(val)
#endif

HA_HANDLER_EXTERN MEI *scalarray_mei HA_HANDLER_INITIALIZATION(= (MEI *)0);
HA_HANDLER_EXTERN MEI *bitmap_mei HA_HANDLER_INITIALIZATION(= (MEI *)0);

// Shorthand for active_index: declaring & using a different name below,
// inside class ha_handler does not work, as some methods of class
// "handler" relies on active_index being updated by us!

#define ACTINX active_index
#define HA_HANDLER_NOTFOUND 0xffffffffffffffff // Which equals ((ulonglong)0)-1

#include "typecode_expansion.h"

template <class IT, class VT>
class ha_scalarray_toolbox;

class ha_handler : public handler
{
  enum thr_lock_type lastlock;
#define TYPECODE_EXPANSION(IT, VT, tcode, extra) \
  friend class ha_scalarray_toolbox<IT, VT>;
#include "typecode_expansion.h"
  ALL_TYPECODE_EXPANSIONS()
#undef TYPECODE_EXPANSION
  friend class ha_bitmap;
  st_bucket_share *share;

protected:
  MEI *mei;
  THR_LOCK_DATA lock_data; /* MySQL lock instance*/
  int act_fflag;           // active find_flag
  int std_errkey;          //

  int write_can_replace;
  int ignore_dup_key;
  int keyread;

protected:
  ha_handler(handlerton *h, TABLE_SHARE *t, MEI *mei);
  ~ha_handler(void);

  const char *index_type(uint key_number);
  int info(uint flag);
  int external_lock(THD *thd, int lock_type);
  void log_lock(enum thr_lock_type lock_type);
  void thr_lock_data_init(void);
  THR_LOCK_DATA **store_lock(THD *thd, THR_LOCK_DATA **to,
                             enum thr_lock_type lock_type);

  void ha_engine_lock(void)
  {
    MUTEX_LOCK(&mei->engine_mutex, mei->engine_name, mei);
  }

  void ha_engine_lock_yield(void)
  {
    MUTEX_LOCK_YIELD(&mei->engine_mutex, mei->engine_name, mei);
  }

  const char *fft(enum ha_rkey_function find_flag);

  virtual int index_read(uchar *buf, const uchar *key,
                         uint key_len, enum ha_rkey_function find_flag);
  int index_read_idx(uchar *buf, uint idx, const uchar *key,
                     uint key_len, enum ha_rkey_function find_flag);

public:
#define EOFF(f) (f == 1 ? table->field[0]->pack_length() : 0)

  virtual void get_status(int concurrent_insert);
  virtual void restore_status(void);
  virtual void update_status(void);

  void log_extra_operation(enum ha_extra_function op);

  virtual int extra(enum ha_extra_function operation);
  virtual int extra_opt(enum ha_extra_function operation, ulong cache_size);

  void log_key_flags(KEY *key);
  void log_field_flags(Field *f);

  uint checksum(void) const;

  /*
    When table is locked a statement is started by calling start_stmt
    instead of external_lock
  */
  // virtual int start_stmt(THD * thd, thr_lock_type lock_type);

public:
  void print_type(enum enum_field_types type);

  // get functions cannot be overloaded b/c return type is the only
  // difference, so we'll need to have fget/dget/lget and one macro

  inline double dget(uint fieldno, const uchar *buf)
  {
    IFACE_NO_LOG_LOCAL;
    DBUG_ENTER("ha_handler::dget");
    buf += EOFF(fieldno);
    Field *field = table->field[fieldno];

    if (field->type() == MYSQL_TYPE_DOUBLE)
    {
      LOGG("dgetting double\n");
      double d = 0.0;
      field->unpack((uchar *)&d, buf, buf + sizeof(d));
      DBUG_RETURN(d);
    }
    if (field->type() == MYSQL_TYPE_FLOAT)
    {
      LOGG("dgetting float\n");
      float f = 0.0;
      field->unpack((uchar *)&f, buf, buf + sizeof(f));
      DBUG_RETURN(f);
    }
    DBUG_RETURN(0.0);
  }

  inline longlong lget(uint fieldno, const uchar *buf)
  {
    IFACE_NO_LOG_LOCAL;
    DBUG_ENTER("ha_handler::lget");
    buf += EOFF(fieldno);
    longlong val = 0;
    Field *field = table->field[fieldno];

    if (field->type() == MYSQL_TYPE_NEWDECIMAL)
    {
      union
      {
        longlong l;
        uchar b[sizeof(ulonglong)];
      } val;
      val.l = 0;
      int packlen = field->pack_length();
      for (int i = 0; i < packlen; i++)
      {
        val.b[packlen - i - 1] = buf[i];
      }
      DBUG_RETURN(val.l);
    }

    field->unpack((uchar *)&val, buf, buf + sizeof(val));
    if (field->flags & UNSIGNED_FLAG)
      DBUG_RETURN(val);

    // It's a signed type, we may need a sign extension:

    switch (field->type())
    {
    case MYSQL_TYPE_LONGLONG:
      DBUG_RETURN(val); // Pc o'cake
    case MYSQL_TYPE_LONG:
      if (val < 0x80000000)
        DBUG_RETURN(val);
      DBUG_RETURN(val | 0xffffffff00000000); // Extend sign
    case MYSQL_TYPE_INT24:
      if (val < 0x800000)
        DBUG_RETURN(val);
      DBUG_RETURN(val | 0xffffffffff000000); // Extend sign
    case MYSQL_TYPE_SHORT:
      if (val < 0x8000)
        DBUG_RETURN(val);
      DBUG_RETURN(val | 0xffffffffffff0000); // Extend sign
    case MYSQL_TYPE_TINY:
      if (val < 0x80)
        DBUG_RETURN(val); // Extend sign
      DBUG_RETURN(val | 0xffffffffffffff00);
    default:
      DBUG_RETURN(val);
    }
  }

  inline double get(uint fieldno, const uchar *buf, double dummy)
  {
    DBUG_ENTER("ha_handler::dget");
    buf += EOFF(fieldno);
    Field *field = table->field[fieldno];

    if (field->type() == MYSQL_TYPE_DOUBLE)
    {
      LOGG("dgetting double\n");
      double d = 0.0;
      field->unpack((uchar *)&d, buf, buf + sizeof(d));
      DBUG_RETURN(d);
    }
    if (field->type() == MYSQL_TYPE_FLOAT)
    {
      LOGG("dgetting float\n");
      float f = 0.0;
      field->unpack((uchar *)&f, buf, buf + sizeof(f));
      DBUG_RETURN(f);
    }
    CRASH("inline double get() shouldn't get to this point");
  }

  // Overloaded through "val"
  inline void put(uint fieldno, uchar *buf, double val)
  {
    DBUG_ENTER("ha_handler::put(double)");
    buf += EOFF(fieldno);

    Field *field = table->field[fieldno];

    if (field->type() == MYSQL_TYPE_NEWDECIMAL)
    {
      CRASHLOG("putting newdecimal from double %f - whoooa!\n", val);
      CRASH("We shouldn't have been putting newdecimal from double!\n");
    }
    else if (field->type() == MYSQL_TYPE_DOUBLE)
    {
      LOGG("putting double\n");
      field->pack(buf, (uchar *)&val);
    }
    else if (field->type() == MYSQL_TYPE_FLOAT)
    {
      LOGG("putting float\n");
      float f = val;
      field->pack(buf, (uchar *)&f);
    }
    DBUG_VOID_RETURN;
  }

  inline void put(uint fieldno, uchar *buf, ulonglong val)
  {
    IFACE_NO_LOG_LOCAL;
    DBUG_ENTER("ha_handler::put(longlong)");
    buf += EOFF(fieldno);
    if (table->field[fieldno]->type() == MYSQL_TYPE_NEWDECIMAL)
    {
      int packlen = table->field[fieldno]->pack_length();
      union
      {
        longlong l;
        uchar b[sizeof(longlong)];
      } oval;
      oval.l = val;
      for (int i = 0; i < packlen; i++)
      {
        buf[i] = oval.b[packlen - i - 1];
      }
      LOCALLOG(LOG2("put() NEWDECIMAL(longlong) %llx (packlen %d)\n", oval.l, packlen));
      bucket_no_log_local = 1; // OFF before return (don't mess up dbug call stack)
      DBUG_VOID_RETURN;
    }

    table->field[fieldno]->pack(buf, (uchar *)&val);
    DBUG_VOID_RETURN;
  }
};

#endif
