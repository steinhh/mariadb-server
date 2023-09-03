/* Copyright (C) 2002 MySQL AB

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

/*
  CREATE FUNCTION bmp_join RETURNS INTEGER SONAME "ha_udfs.so";
  CREATE FUNCTION bmp_union RETURNS INTEGER SONAME "ha_udfs.so";
  CREATE FUNCTION sar_pick_bmp RETURNS INTEGER SONAME "ha_udfs.so";
  CREATE FUNCTION bmp_sar_in_match RETURNS INTEGER SONAME "ha_udfs.so";
  Dropping:
  DROP FUNCTION bmp_join;
*/
#include "all-mysql-includes.h"

#define FULL_LOGGING
#include "bucket.h"
#include "logging.h"
#include "ha_handler.h"
#include "bitbucket.h"
#include "scalarrbucket.h"
#include "ha_scalarray.h"
#include "ha_scalarray_toolbox.h"

extern MEI *scalarray_mei;
extern MEI *bitmap_mei;

/* These must be right or mysqld will not find the symbol! */

/*
  Simple example of how to get a bmp_joins starting from the first argument
  or 1 if no arguments have been given
*/

MEI *mei = bitmap_mei;

static my_bool bmp_x3_init(UDF_INIT *initid, UDF_ARGS *args, char *message)
{
  DBUG_ENTER("bmp_x3_init");
  if (args->arg_count < 3)
  {
    strmov(message, "Function takes 3+ arguments: dest,src1,src2,...");
    DBUG_RETURN(1);
  }

  for (unsigned int i = 0; i < args->arg_count; i++)
    args->arg_type[i] = INT_RESULT; /* Force argument to int */

  if (!(initid->ptr = (char *)malloc(sizeof(bitbucket *) * args->arg_count)))
  {
    strmov(message, "Couldn't allocate memory");
    DBUG_RETURN(1);
  }
  initid->const_item = 0;
  DBUG_RETURN(0);
}

static void x_deinit(UDF_INIT *initid)
{
  DBUG_ENTER("x_deinit");
  if (initid->ptr)
    free(initid->ptr);
  DBUG_VOID_RETURN;
}

typedef enum
{
  JOIN,
  UNION
} ACTION;

static longlong bmp_x3(UDF_INIT *initid, UDF_ARGS *args, char *is_null,
                       char *error, ACTION action)
{
  DBUG_ENTER("bmp_x3");
  bitbucket **s = (bitbucket **)initid->ptr;

  for (unsigned int i = 0; i < args->arg_count; i++)
  {
    if (!args->args[i])
      return *error = 1;
    s[i] = (bitbucket *)bucket_find_bucket(*((longlong *)args->args[i]), bitmap_mei);
    if (!s[i])
      DBUG_RETURN(*error = 1);
  }
  LOGG("Got here\n");
  if (s[0]->records())
    DBUG_RETURN(*error = 1);

  if (action == JOIN)
    s[0]->Join(s + 1, args->arg_count - 1);
  if (action == UNION)
    s[0]->Union(s + 1, args->arg_count - 1);
  DBUG_RETURN(1);
}

////////////////////////////////////////////////////////////////////////////////

extern "C"
{
  my_bool bmp_join_init(UDF_INIT *initid, UDF_ARGS *args, char *message);
  void bmp_join_deinit(UDF_INIT *initid);
  longlong bmp_join(UDF_INIT *initid, UDF_ARGS *args, char *is_null,
                    char *error);
}

my_bool bmp_join_init(UDF_INIT *initid, UDF_ARGS *args, char *message)
{
  return bmp_x3_init(initid, args, message);
}

void bmp_join_deinit(UDF_INIT *initid) { x_deinit(initid); }

longlong bmp_join(UDF_INIT *initid, UDF_ARGS *args, char *is_null, char *error)
{
  return bmp_x3(initid, args, is_null, error, JOIN);
}

///////////////////////////////////////////////////////////////////////////////

extern "C"
{
  my_bool bmp_union_init(UDF_INIT *initid, UDF_ARGS *args, char *message);
  void bmp_union_deinit(UDF_INIT *initid);
  longlong bmp_union(UDF_INIT *initid, UDF_ARGS *args, char *is_null, char *error);
}

my_bool bmp_union_init(UDF_INIT *initid, UDF_ARGS *args, char *message)
{
  return bmp_x3_init(initid, args, message);
}

void bmp_union_deinit(UDF_INIT *initid) { x_deinit(initid); }

longlong bmp_union(UDF_INIT *initid, UDF_ARGS *args, char *is_null, char *error)
{
  return bmp_x3(initid, args, is_null, error, UNION);
}

/////////////////////////////////////////////////////////////////////////////

// insert into dest select id from src where val in (v1,v2,...)
// => select bmp_sar_in_match(@cksum_dest,@cksum_src,v1,v2,...)

extern "C"
{
  my_bool bmp_sar_in_match_init(UDF_INIT *initid, UDF_ARGS *args, char *message);
  void bmp_sar_in_match_deinit(UDF_INIT *initid);
  longlong bmp_sar_in_match(UDF_INIT *initid, UDF_ARGS *args, char *is_null, char *error);
}

my_bool bmp_sar_in_match_init(UDF_INIT *initid, UDF_ARGS *args, char *message)
{
  DBUG_ENTER("bmp_sar_in_match_init");
  if (args->arg_count < 3)
  {
    strmov(message, "Function takes 3+ arguments: bmp_dest,sar_src,inval1,...");
    DBUG_RETURN(1);
  }

  for (unsigned int i = 0; i < args->arg_count; i++)
    args->arg_type[i] = INT_RESULT; /* Force arguments to int */

  if (!(initid->ptr = (char *)malloc(sizeof(longlong) * (args->arg_count - 2))))
  {
    strmov(message, "Couldn't allocate memory");
    DBUG_RETURN(1);
  }
  initid->const_item = 0;
  DBUG_RETURN(0);
}

void bmp_sar_in_match_deinit(UDF_INIT *initid)
{
  DBUG_ENTER("bmp_sar_in_match_deinit");
  x_deinit(initid);
  DBUG_VOID_RETURN;
}

// We need... like, a macro set to:

// A) Define the union { bxiv... } bucket stuff
// B) Use the right union member for calls

// But not least - a way to get the TYPECODE for the table[s]!

// Perhaps set aside a field in bucket, set to zero for bitbuckets?

// And... for this one - what to do about the arguments? What'll they
// look like if they're decimals? Or floats/doubles?
longlong bmp_sar_in_match(UDF_INIT *initid, UDF_ARGS *args, char *is_null,
                          char *error)
{
  DBUG_ENTER("bmp_sar_in_match");
  bitbucket *bmp_dest;
  // scalarrbucket<longlong> *sar_src;

  bmp_dest = (bitbucket *)bucket_find_bucket(*((longlong *)args->args[0]), bitmap_mei);
  if (!bmp_dest)
    DBUG_RETURN(*error = 1);

  /// sar_src = (Bucket*)bucket_find_bucket(*((longlong*)args->args[1]),scalarray_mei);
  /// if (!sar_src) DBUG_RETURN(*error = 1);
  /////// if (sar_src->bats->dval) DBUG_RETURN(*error = 1);

  longlong *ptr = (longlong *)initid->ptr;
  for (unsigned int i = 2; i < args->arg_count; i++)
  {
    ptr[i - 2] = *((longlong *)args->args[i]);
    LOG1("IN(%lld)\n", ptr[i - 2]);
  }
  // We'll define this as a virtual function!
  // *error = sar_src->bmp_sar_in_match(bmp_dest,args->arg_count-2,ptr);
  DBUG_RETURN(1);
}

// INSERT INTO sb3_c.c2518767 (FILE_ID_,SQ_IUMODE0)
//    SELECT STRAIGHT_JOIN sb3_c.c1.FILE_ID_,SQ_IUMODE0 from sb3_c.c1 NATURAL JOIN SQ_IUMODE0
//
// I.e. insert into A records from B that are selected by C
// A and B are *identical* in terms of format

extern "C"
{
  my_bool sar_pick_bmp_init(UDF_INIT *initid, UDF_ARGS *args, char *message);
  void sar_pick_bmp_deinit(UDF_INIT *initid);
  longlong sar_pick_bmp(UDF_INIT *initid, UDF_ARGS *args, char *is_null, char *error);
}

my_bool sar_pick_bmp_init(UDF_INIT *initid, UDF_ARGS *args, char *message)
{
  DBUG_ENTER("sar_pick_bmp_init");
  if (args->arg_count != 3)
  {
    strmov(message, "Function takes 3 arguments: sar_dest,sar_src,bmp_src");
    DBUG_RETURN(1);
  }

  for (unsigned int i = 0; i < args->arg_count; i++)
    args->arg_type[i] = INT_RESULT; /* Force argument to int */

  if (!(initid->ptr = (char *)malloc(sizeof(Bucket *) * args->arg_count)))
  {
    strmov(message, "Couldn't allocate memory");
    DBUG_RETURN(1);
  }
  initid->const_item = 0;
  DBUG_RETURN(1);
}

void sar_pick_bmp_deinit(UDF_INIT *initid) { x_deinit(initid); }

longlong sar_pick_bmp(UDF_INIT *initid, UDF_ARGS *args, char *is_null, char *error)
{
  IFACE_LOG_LOCAL;
  DBUG_ENTER("sar_pick_bmp");
  // scalarrbucket<double> *sar_dst; // MUST BE FIXED (template functions??)
  // scalarrbucket<double> *sar_src;
  // bitbucket *bmp_src;

  if (!args->args[0] || !args->args[1] || !args->args[2])
    DBUG_RETURN(*error = 1);

  // sar_dst = (scalarrbucket<double>*)bucket_find_bucket(*((longlong*)args->args[0]),scalarray_mei);
  // sar_src = (scalarrbucket<double>*)bucket_find_bucket(*((longlong*)args->args[1]),scalarray_mei);
  // bmp_src = (bitbucket*)bucket_find_bucket(*((longlong*)args->args[2]),bitmap_mei);

  LOGG("Ready3??\n");
  // if (!sar_dst || !sar_src || !bmp_src) DBUG_RETURN(*error = 1);

  // if (sar_dst->records()) DBUG_RETURN(*error = 1);

  // if (sar_dst->bats->type != sar_src->bats->type) DBUG_RETURN(*error = 1);

  LOGG("Ready??\n");
  // sar_dst->bmp_filter(sar_src,bmp_src);
  LOGG("Guess not\n");

  DBUG_RETURN(1);
}
