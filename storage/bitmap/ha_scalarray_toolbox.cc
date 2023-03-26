#define MYSQL_SERVER 1
// Changed for MySql 5.5
#include "all-mysql-includes.h"
// ----

#include "bucket.h"
#define FULL_LOGGING
#include "logging.h"
#include "ha_handler.h"
#include "ha_scalarray.h"
#include "bitbucket.h"
#include "scalarrbucket.h"
#include "ha_scalarray_toolbox.h"

#include "typecode_expansion.h" // To make explicit instantiation files work!

#define SHARE handler->share
#define HANDLER_SHARE_BUCKET ((scalarrbucket<IT, VT> *)SHARE->bucket)
#define BITS HANDLER_SHARE_BUCKET->bits
#define BATS HANDLER_SHARE_BUCKET // NOTE - SYNONYMS!

#define BATS_WAKEUP                            \
  if (HANDLER_SHARE_BUCKET->hibernated)        \
  {                                            \
    HANDLER_SHARE_BUCKET->bucket_lock();       \
    HANDLER_SHARE_BUCKET->wakeup();            \
    HANDLER_SHARE_BUCKET->bucket_lock_yield(); \
  }

#define TOOLBOX(t)              \
  template <class IT, class VT> \
  t ha_scalarray_toolbox<IT, VT>::

#define FDLGET(buf)                          \
  ((handler->val_is_d || handler->val_is_f)  \
       ? (VT)handler->dget(handler->vc, buf) \
       : (VT)handler->lget(handler->vc, buf))

#undef EOFF
#define EOFF(f) (f == 1 ? handler->table->field[0]->pack_length() : 0)

TOOLBOX(int)
open(const char *name, int mode, uint test_if_locked)
{
  IFACE_LOG_LOCAL;
  DBUG_ENTER3("TOOLBOX::open(name=%s mode=%d test_if_locked=%u)", name, mode, test_if_locked);

  // See note in bucket_get_share() about engine locking

  handler->ha_engine_lock();

  if (!(handler->share = bucket_get_share(name, mei)))
  {
    LOCALLOG(LOGG("Ooops, bucket_get_share returned NULL!"));
    handler->ha_engine_lock_yield();
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);
  }

  // The thr_lock is a MySQL thing - relates the handler (MySQL "parent"
  // class) lock_data to share->thr_lock. This thingy apparently does not need
  // to be "destroyed".

  handler->thr_lock_data_init();

  // If the share already has a bucket, we're not creating a new share - so we
  // just lock_yield & return OK. No checking of the table.

  if (handler->share->bucket)
  {
    handler->ha_engine_lock_yield();
    DBUG_RETURN(0);
  }

  // New share, new bucket!
  LOG1("Creating new bucket for share%s\n", handler->share->table_name);

  handler->share->bucket = new scalarrbucket<IT, VT>(handler->share->table_name, LOCK_LOCKED,
                                                     handler->typecode, handler->singlecol,
                                                     handler->bas_ext(), mei);

  LOGG("Share ready for registering\n");

  // The bucket is ready - register, but keep engine locked
  bucket_register_share(handler->share, mei);

  // We want to check it before anyone else gets their hands on it, then
  // lock_yield & return if everything's ok

  // But remember to tell bucket about the opened engine!  This must
  // occur *before* engine is unlocked: the bitbucket might get
  // used by other threads for ops that require locking, resulting in
  // disaster (i.e.: it doesn't even attempt to lock, b/c it's been
  // told that it's already locked; we unlock it here, and it knows
  // nothing about it...

  if (!HANDLER_SHARE_BUCKET->is_crashed())
  {
    handler->share->bucket->bucket_engine_lock_set(LOCK_OPEN);
    handler->ha_engine_lock_yield();
    DBUG_RETURN(0);
  }

  // Oops. No, something went wrong!
  LOCALLOG(LOG1("Table crash on open: %s\n", name));
  LOCALLOG(LOGG("We should implement a repair table???\n"));
  LOCALLOG(LOGG("Something about open-for-repair\n"));

  // There won't be a "close" coming, so delete the bucket & free the share

  delete HANDLER_SHARE_BUCKET;

  if (bucket_free_share(handler->share, mei) != 0)
  {
    CRASHLOG("HEY - Other users of just opened %s? Doh!\n", name);
    CRASH("Won't continue\n");
  }

  // No bucket left to inform about the lock opening, just do it:
  handler->ha_engine_lock_yield();
  DBUG_RETURN(HA_ERR_CRASHED_ON_USAGE);
}

TOOLBOX()
ha_scalarray_toolbox(ha_scalarray *handler_in, MEI *mei_in)
    : mei(mei_in), handler(handler_in)
{
  DBUG_ENTER("TOOLBOX::toolbox");
  DBUG_VOID_RETURN;
}

TOOLBOX()
~ha_scalarray_toolbox()
{
  DBUG_ENTER("TOOLBOX::~toolbox");
  DBUG_VOID_RETURN;
}

TOOLBOX(int)
close(void)
{
  IFACE_LOG_LOCAL;
  DBUG_ENTER("TOOLBOX::close");

  // If bucket_free_share() decides we're the last user, it'll delete the
  // share, so we can't reach the bucket. Need to keep pointer safe:

  scalarrbucket<IT, VT> *bucket = HANDLER_SHARE_BUCKET;

  handler->ha_engine_lock();
  LOCALLOG(LOG1("Freeing sharecount %s\n", handler->share->table_name));
  int users = bucket_free_share(handler->share, mei);
  handler->ha_engine_lock_yield();

  if (users == 0)
    delete bucket;
  DBUG_RETURN(0);
}

TOOLBOX(int)
repair(THD *thd, HA_CHECK_OPT *check_opt)
{
  BATS_WAKEUP;
  return HANDLER_SHARE_BUCKET->repair();
}

TOOLBOX(int)
check(THD *thd, HA_CHECK_OPT *check_opt)
{
  BATS_WAKEUP;
  return HANDLER_SHARE_BUCKET->check();
}

TOOLBOX(void)
start_bulk_insert(ha_rows rows)
{
  DBUG_ENTER("TOOLBOX::start_bulk_insert");
  BATS_WAKEUP;
  if (BATS->have_inserted)
  {
    LOGG("HEY, WE CAN't HAVE INSERTIONS W/PRIOR INSERTIONS!!\n");
  }
  LOG2("records: %llu rows: %lu\n", HANDLER_SHARE_BUCKET->records(), (unsigned long)rows);
  DBUG_VOID_RETURN;
}

TOOLBOX(int)
end_bulk_insert(void)
{
  DBUG_ENTER("TOOLBOX::end_bulk_insert");
  HANDLER_SHARE_BUCKET->save();
  DBUG_RETURN(0);
}

TOOLBOX(int)
write_row(const uchar *buf)
{
  DBUG_ENTER("TOOLBOX::write_row");
  BATS_WAKEUP;
  handler->table->status = 0;
  if (HANDLER_SHARE_BUCKET->is_crashed())
  {
    LOCALLOG(LOG1("%s::write_row is_crashed()\n", SHARE->table_name));
    DBUG_RETURN(HA_ERR_CRASHED_ON_USAGE);
  }

  ulonglong primary; // Always good enough for primary
  VT value;

  int NB = handler->table->s->null_bytes;

  if (!handler->singlecol)
  {
    // Seems to be all that's required to make auto-increment work -
    // sweet!  We don't even need to set next_insert_id upon opening a
    // new file, since MySQL seems to do that for us by calling
    // index_last!

    if (handler->table->next_number_field && buf == handler->table->record[0])
    {
      LOGG("Auto-increment trap detected\n");
      if (int res = handler->update_auto_increment())
        DBUG_RETURN(res);
    }

    primary = handler->lget(0, buf + NB);
    LOGG("lgot primary\n");
  }
  else
  {
    primary = BITS->max_value + 1;
  }

  value = FDLGET(buf + NB);
  LOG3("%s: primary=%lld, value=%.8g\n", SHARE->table_name, primary, (double)value);

  handler->errkey = 0;
  LOG1("Unset handler->errkey (%s)\n", SHARE->table_name);

  if (BITS->isset(primary))
  {
    *(ulonglong *)handler->dup_ref = primary; // Store duplicate row reference!
    *(ulonglong *)handler->ref = primary;     // Uhh... can't hurt, right?
    LOGG("It was already there! BITS->isset\n");

    if (handler->write_can_replace || handler->ignore_dup_key)
    { // UPDATE!
      LOGG("We can ignore dup - doing update if write-can-replace\n");
      if (BATS->buffer[primary] == value)
      {
        LOGG("No change - returning\n");
        handler->table->status = HA_ERR_FOUND_DUPP_KEY; /* ?? */
        DBUG_RETURN(HA_ERR_FOUND_DUPP_KEY);
      }
      if (handler->write_can_replace)
      {
        LOGG("Will unset existing entry, add new in inserted\n");
        BATS->unset(primary);
        BATS->set(primary, value);
      }
      // handler->table->status???????????????????????????????????
      DBUG_RETURN(HA_ERR_FOUND_DUPP_KEY);
    }

    if (BATS->have_inserted && BATS->inserted->isset(primary))
    {
      LOGG("was inserted\n");
    }
    handler->table->status = HA_ERR_FOUND_DUPP_KEY;
    DBUG_RETURN(HA_ERR_FOUND_DUPP_KEY);
  }

  BITS->set(primary);
  BATS->set(primary, value);
  handler->stats.records = BITS->records();
  DBUG_RETURN(0);
}

TOOLBOX(bool)
start_bulk_update()
{
  DBUG_ENTER("TOOLBOX::start_bulk_update");
  BATS_WAKEUP;
  if (BATS->have_inserted || BATS->have_deleted)
  {
    LOGG("HEY - WE CAN'T START THIS W/ALREADY-DELETED STUFF?\n");
  }
  DBUG_RETURN(0);
}

TOOLBOX(int)
bulk_update_row(const uchar *old_data, const uchar *new_data, ha_rows *dup_key_found)
{
  DBUG_ENTER("TOOLBOX::bulk_update_row");
  *dup_key_found = 0;
  int update_or_unchanged = update_row_internal(old_data, new_data);
  DBUG_RETURN(update_or_unchanged);
}

TOOLBOX(int)
exec_bulk_update(ha_rows *dup_key_found)
{
  DBUG_ENTER("TOOLBOX::exec_bulk_update");
  HANDLER_SHARE_BUCKET->save();
  *dup_key_found = 0;
  DBUG_RETURN(0);
}

TOOLBOX(int)
end_bulk_update()
{
  DBUG_ENTER("TOOLBOX::end_bulk_update");
  HANDLER_SHARE_BUCKET->save();
  DBUG_RETURN(0); //! Don't know what return value means
}

TOOLBOX(int)
update_row_internal(const uchar *old_data, const uchar *new_data)
{
  IT primary, new_primary;
  VT value, new_value;
  DBUG_ENTER("TOOLBOX::update_row_internal");
  if (handler->singlecol)
  {
    my_error(HA_ADMIN_NOT_IMPLEMENTED, MYF(0),
             "updating rows in value-only tables");
    DBUG_RETURN(HA_ERR_UNSUPPORTED);
  }
  BATS_WAKEUP;
  int NB = handler->table->s->null_bytes;

  primary = handler->lget(0, old_data + NB);
  value = FDLGET(old_data + NB);

  new_primary = handler->lget(0, new_data + NB);
  new_value = FDLGET(new_data + NB);

  LOG4("Update(%llu,%llx)=>(%llu,%llx)\n", (ulonglong)primary, (ulonglong)value, (ulonglong)new_primary, (ulonglong)new_value);

  handler->table->status = 0; // ? Testing, testing... update ... in (.. , ..)
  if (new_value == BATS->buffer[primary])
  {
    DBUG_RETURN(HA_ERR_RECORD_IS_THE_SAME); // Signals no change?
  }

  BATS->unset(primary); // To avoid too many index entries!!
  BATS->set(new_primary, new_value);
  DBUG_RETURN(0);
}

TOOLBOX(int)
update_row(const uchar *old_data, const uchar *new_data)
{
  DBUG_ENTER("TOOLBOX::update_row");
  int res = update_row_internal(old_data, new_data);
  DBUG_RETURN(res);
}

TOOLBOX(bool)
start_bulk_delete()
{
  DBUG_ENTER("TOOLBOX::start_bulk_delete");
  BATS_WAKEUP;
  if (BATS->have_deleted)
  {
    LOGG("HEY, WE CAN't START DELETING W/PRIOR DELETIONS!\n");
  }
  DBUG_RETURN(0);
}

TOOLBOX(int)
end_bulk_delete()
{
  DBUG_ENTER("TOOLBOX::end_bulk_delete");
  HANDLER_SHARE_BUCKET->save();
  DBUG_RETURN(0);
}

TOOLBOX(int)
delete_row(const uchar *buf)
{
  DBUG_ENTER("TOOLBOX::delete_row");
  if (handler->singlecol)
  {
    my_error(HA_ADMIN_NOT_IMPLEMENTED, MYF(0),
             "updating rows in value-only tables");
    DBUG_RETURN(HA_ERR_UNSUPPORTED);
  }
  BATS_WAKEUP;
  longlong primary;
  VT value;
  primary = handler->lget(0, buf + handler->table->s->null_bytes);
  value = FDLGET(buf + handler->table->s->null_bytes);

  if (BATS->have_inserted && BATS->inserted->isset(primary))
  {
    LOG2("Replace_delete(%lld,%.8g)\n", primary, (double)value);
    CRASH("delete_row() replace/deleted thingy"); // Bad, bad thing!
    DBUG_RETURN(0);
  }
  // LOG2("Unset(%d,%d)\n",primary,value);
  BATS->unset(primary); // BITS before BATS => deleting nonexisting value!
  BITS->unset(primary);
  // THD *thd = ha_thd();
  // if (thd->system_thread == SYSTEM_THREAD_SLAVE_SQL && thd->query == NULL)
  //   DBUG_RETURN(0);

  handler->stats.deleted += 1; // Not *quite* sure what these do...
  handler->stats.records -= 1;
  handler->table->status = 0;

  DBUG_RETURN(0);
}

TOOLBOX(int)
rnd_init(bool scan)
{
  DBUG_ENTER("TOOLBOX::rnd_init");
  LOG1("::rnd_init(%d)\n", scan);
  BATS_WAKEUP;
  curpos = -1;
  handler->ACTINX = 0;
  handler->act_fflag = -1;
  handler->table->status = 0;
  DBUG_RETURN(0);
}

// Used by all methods that read into buffer.
// Buf will be corrected for null_bytes!
TOOLBOX(void)
putrow(uchar *buf, longlong cpos, VT cval)
{
  int VTput;
  DBUG_ENTER("TOOLBOX::putrow");
  memset(buf, 0, handler->table->s->null_bytes);
  buf += handler->table->s->null_bytes;
  if (!handler->singlecol)
    IT_put(0, buf, cpos);

  // VT_put if bitmap indicates it, OR if keyread is off!

  ulonglong keyread_bitmap = // Shorthand
      *handler->table->read_set->bitmap;

  VTput = (1 << handler->vc) & keyread_bitmap;
  if (!handler->keyread)
    VTput = 1;

  if (VTput)
    VT_put(handler->vc, buf, cval);

  DBUG_VOID_RETURN;
}

TOOLBOX(int)
rnd_next(uchar *buf)
{
  DBUG_ENTER("TOOLBOX::rnd_next");

  if (handler->ACTINX == 0 || handler->ACTINX == 2)
  {
    curpos = BITS->next(curpos);
    if (curpos != HA_HANDLER_NOTFOUND)
      curval = BATS->buffer[curpos];
    LOG2("curpos=%lld, curval=%.8g\n", curpos, (double)curval);
  }
  else
  {
    curpos = HA_HANDLER_NOTFOUND; // Flags EOF if unchanged
    if (++curixix < HANDLER_SHARE_BUCKET->records())
    {
      curpos = BATS->index[curixix];
      curval = BATS->buffer[curixix];
    }
  }

  if (curpos == HA_HANDLER_NOTFOUND)
    DBUG_RETURN(HA_ERR_END_OF_FILE);
  putrow(buf, curpos, curval);
  DBUG_RETURN(0);
}

TOOLBOX(int)
rnd_prev(uchar *buf)
{
  DBUG_ENTER("TOOLBOX::rnd_prev");

  if (handler->ACTINX == 0 || handler->ACTINX == 2)
  {
    curpos = BITS->prev(curpos);
    if (curpos != HA_HANDLER_NOTFOUND)
      curval = BATS->buffer[curpos];
  }
  else
  {
    curpos = HA_HANDLER_NOTFOUND; // Flags EOF if unchanged
    if (--curixix != HA_HANDLER_NOTFOUND)
    {
      curpos = BATS->index[curixix];
      curval = BATS->buffer[curpos];
    }
  }

  if (curpos == HA_HANDLER_NOTFOUND)
    DBUG_RETURN(HA_ERR_END_OF_FILE);
  putrow(buf, curpos, curval);
  DBUG_RETURN(0);
}

TOOLBOX(int)
rnd_pos(uchar *buf, uchar *pos)
{
  DBUG_ENTER("TOOLBOX::rnd_pos");
  LOG1("pos=%llx\n", (ulonglong)pos);
  curpos = *(longlong *)pos;
  LOG1("curpos=%lld\n", curpos);
  curval = BATS->buffer[curpos];
  LOG3("curpos: %llu, curval: %llx(hex) %.10g(dec)\n", curpos, (ulonglong)curval, (double)curval);
  putrow(buf, curpos, curval);
  handler->table->status = 0;
  DBUG_RETURN(0);
}

TOOLBOX(void)
position(const uchar *record)
{
  DBUG_ENTER("TOOLBOX::position");
  LOG2("Storing curpos=%lld at %llx\n", curpos, (ulonglong)handler->ref);
  *(longlong *)handler->ref = curpos;
  DBUG_VOID_RETURN;
}

TOOLBOX(int)
index_init(uint idx, bool sorted)
{
  DBUG_ENTER3("TOOLBOX::index_init(idx=%u; sorted=%d) [%s]", idx, (int)sorted, SHARE->table_name);
  LOG2("idx=%d, sorted=%d\n", idx, sorted);
  handler->ACTINX = idx;
  handler->act_fflag = -1;
  BATS_WAKEUP;
  DBUG_RETURN(0);
}

#define DOUBLEPREC 1.80143e16
#define SINGLEPREC 3.35544e7
#define VARPREC(var) (sizeof(var) == sizeof(double) ? DOUBLEPREC : SINGLEPREC)
#define FABS(var) (sizeof(var) == sizeof(double) ? fabs((double)var) : fabsf((float)var))

TOOLBOX(void)
add_one_ulp(VT &value)
{
  DBUG_ENTER("add_one_ulp");
  if (!(handler->val_is_d || handler->val_is_f))
  {
    value++;
    DBUG_VOID_RETURN;
  }

  VT aval = (VT)(FABS(value) / VARPREC(value));
  while (value + aval == value)
  {
    aval *= 2;
    LOCALLOG(LOGG("Hey-going around\n"));
  }
  value += aval;
  DBUG_VOID_RETURN;
}

TOOLBOX(void)
sub_one_ulp(VT &value)
{
  DBUG_ENTER("sub_one_ulp");
  if (!(handler->val_is_d || handler->val_is_f))
  {
    value--;
    DBUG_VOID_RETURN;
  }
  VT aval = (VT)(FABS(value) / VARPREC(value));
  while (value - aval == value)
  {
    aval *= 2;
    LOCALLOG(LOGG("Hey-going around\n"));
  }
  value -= aval;
  DBUG_VOID_RETURN;
}

TOOLBOX(ha_rows)
records_in_range(uint inx,
                 const key_range *min_key,
                 const key_range *max_key, page_range *pages)
{
  DBUG_ENTER1("TOOLBOX::records_in_range [%s]", SHARE->table_name);
  LOG2("For %s, inx=%u\n", SHARE->table_name, inx);
  BATS_WAKEUP;

  if (!BITS->records())
    DBUG_RETURN(0); // Easy :-)

  if (inx == 0 || inx == 2)
  {
    LOGG("Non-empty table, primary key\n");
    ulonglong lower = 0, upper = BITS->max_value;

    if (min_key && min_key->key)
    {
      LOG1("Reading min_key, find_flag=%s\n", handler->fft(min_key->flag));
      lower = handler->lget(0, min_key->key);
      if (min_key->flag == HA_READ_AFTER_KEY)
        lower++; // Plus-one-ULP
      if (min_key->keypart_map & 2)
      {
        // The *first* column alone must be unique in this engine, so
        // 2nd part of primary key is irrelevant
        LOGG("Ignoring second part of IT key!\n");
      }
      if (lower > BITS->max_value)
      {
        LOGG("One\n");
        DBUG_RETURN(0);
      }
      // if (lower < 0) lower = 0; // READ_AFTER_KEY already done!
    }

    if (max_key && max_key->key)
    {
      LOG1("Reading max_key, find_flag=%s\n", handler->fft(max_key->flag));
      upper = handler->lget(0, max_key->key);
      if (max_key->flag == HA_READ_BEFORE_KEY)
        upper--; // Minus-one-ULP
      if (max_key->keypart_map & 2)
      {
        // The *first* column alone must be unique in this engine, so
        // 2nd part of primary key is irrelevant
        LOGG("Ignoring second part of IT key!\n");
      }
      // if (upper < 0) { LOGG("One-B\n"); DBUG_RETURN( 0 );}
      if (upper > BITS->max_value)
        upper = BITS->max_value;
    }

    double average_density = BITS->records() / (double)BITS->max_value;
    longlong nvals = (ulonglong)((upper - lower) * average_density);
    LOG2("average=%.8g, nvals=%llu\n", average_density, nvals);

    if (nvals <= 1)
      nvals = 2; // nvals==0 or 1 may be taken literally by MySQL

    LOG4("%s records_in_range[0](%lld,%lld) = %lld\n", SHARE->table_name, lower, upper, nvals);
    DBUG_RETURN(nvals);
  }
  else
  { // NOTE: REVERSE INDEX!
    LOGG("Non-empty table, REVERSE key\n");
    VT lower = BATS->min_value, upper = BATS->max_value;
    LOG2("initial lower: %.12g, upper: %.12g\n", (double)lower, (double)upper);

    if (min_key && min_key->key)
    {
      LOG1("Reading min_key, find_flag=%s\n", handler->fft(min_key->flag));
      lower = FDLGET(min_key->key - EOFF(1));
      if (min_key->flag == HA_READ_AFTER_KEY)
        add_one_ulp(lower); // Plus-one-ULP
      if (min_key->keypart_map & 2)
      {
        LOGG("Can't use both parts of an index [in range ops]!\n");
        my_error(HA_ADMIN_NOT_IMPLEMENTED, MYF(0),
                 "using both parts of index in range operations");
      }
      if (lower < BATS->min_value)
        lower = BATS->min_value;
      LOG3("lower=%.10g, min_value=%.10g, max_value=%.10g\n", (double)lower, (double)BATS->min_value, (double)BATS->max_value);
      if (lower > BATS->max_value)
      {
        LOGG("Two\n");
        DBUG_RETURN(0);
      }
    }

    if (max_key && max_key->key)
    {
      LOG1("Reading max_key, find_flag=%s\n", handler->fft(max_key->flag));
      upper = FDLGET(max_key->key - EOFF(1));
      if (max_key->flag == HA_READ_BEFORE_KEY)
        sub_one_ulp(upper); // Minus-one-ULP
      if (max_key->keypart_map & 2)
      {
        LOGG("Can't use both parts of an index [in range ops]!\n");
        my_error(HA_ADMIN_NOT_IMPLEMENTED, MYF(0),
                 "using both parts of index in range operations");
      }
      if (upper > BATS->max_value)
        upper = BATS->max_value;
      LOG3("upper=%.10g, min_value=%.10g, max_value=%.10g\n", (double)upper, (double)BATS->min_value, (double)BATS->max_value);
      if (upper < BATS->min_value)
      {
        LOGG("Three\n");
        DBUG_RETURN(0);
      }
    }

    // For this index, we *can* find an *exact* count, since the index
    // array is compact, and all matching values are adjacent!

    LOG3("lower=%.10g, upper=%.10g, max_value=%.10g\n", (double)lower, (double)upper, (double)BATS->max_value);
    ulonglong minixix, maxixix, nvals;
    LOG2("Searching [%.12g, %.12g]\n", (double)lower, (double)upper);
    BATS->isset_first(lower, &minixix);
    BATS->isset_last(upper, &maxixix);
    if (minixix == HA_HANDLER_NOTFOUND || maxixix == HA_HANDLER_NOTFOUND)
      nvals = 0;
    else
      nvals = maxixix - minixix + 1;
    LOG4("%s records_in_range[1](%.8g,%.8g) = %lld\n", SHARE->table_name, (double)lower, (double)upper, nvals);
    DBUG_RETURN(nvals);
  }
}

TOOLBOX(int)
index_read(uchar *buf, const uchar *key, uint key_len, enum ha_rkey_function find_flag)
{
  DBUG_ENTER1("TOOLBOX::index_read [%s]", SHARE->table_name);
  int field = (handler->ACTINX == 1) ? 1 : 0;

  handler->act_fflag = find_flag;
  handler->table->status = STATUS_NOT_FOUND;

  if (field == 0)
  {
    curpos = handler->lget(0, key);
    LOG4("::index_read(inx=%d fflag=%s keylen=%d curpos=%lld)\n", handler->ACTINX, handler->fft(find_flag), key_len, curpos);

    switch (find_flag)
    {

    case HA_READ_KEY_EXACT:
      if (BITS->isset(curpos))
        break;
      DBUG_RETURN(HA_ERR_KEY_NOT_FOUND);
      //--------------------------------

    case HA_READ_KEY_OR_NEXT:
      if (BITS->isset(curpos))
        break; // Finish if found, go on if not:
               // fall through
    case HA_READ_AFTER_KEY:
      curpos = BITS->next(curpos);
      if (curpos != HA_HANDLER_NOTFOUND)
        break;
      DBUG_RETURN(HA_ERR_END_OF_FILE);
      //--------------------------------

    case HA_READ_KEY_OR_PREV:
      if (BITS->isset(curpos))
        break; // Fine if found, if not try before:
               // fall through

    case HA_READ_BEFORE_KEY:
      curpos = BITS->prev(curpos);
      if (curpos != HA_HANDLER_NOTFOUND)
        break;
      DBUG_RETURN(HA_ERR_END_OF_FILE);
      //--------------------------------

    default:
      LOCALLOG(LOGG("index_read() with unknown flag! Crashing!!\n"));
      CRASH("index_read() with unknown flag");
    }

    curval = BATS->buffer[curpos];
    if (find_flag == HA_READ_KEY_EXACT)
    {
      LOCALLOG(LOG1("ixr: buffer[%s]\n", SHARE->table_name));
    }

    if (key_len > handler->table->field[0]->pack_length())
    {
      VT value;
      if (find_flag == HA_READ_KEY_EXACT)
      {
        LOGG("FDLGETTING\n");
        value = FDLGET(key);
        LOG1("value=%.8g\n", (double)value);
        if (value != curval)
          DBUG_RETURN(HA_ERR_END_OF_FILE); // Why EOF not NOT FOUND???
      }
      else
      {
        LOCALLOG(LOGG("Chicken out - full key, find_flag!=READ_KEY_EXACT!\n"));
        CRASH("index_read() with full key, find_flag <> READ_KEY_EXACT");
      }
    }
    if (find_flag == HA_READ_KEY_EXACT)
      LOCALLOG(LOGG("ixr: proceed\n"));
  }
  else
  { // field=1, using index where val is first
    VT val;
    val = FDLGET(key - EOFF(1));
    LOG4("::index_read(inx=%d fflag=%s keylen=%d curpos=%.8g)\n", handler->ACTINX, handler->fft(find_flag), key_len, (double)val);

    curval = val;
    curixix = -1;
    switch (find_flag)
    {

    case HA_READ_PREFIX:
    case HA_READ_KEY_EXACT:
      if (BATS->isset_first(curval, &curixix))
        break; // Found
      DBUG_RETURN(HA_ERR_KEY_NOT_FOUND);
      //==================================

      // select * from tsc where val >= 9.999 order by val, id limit 2;
    case HA_READ_KEY_OR_NEXT:
      if (BATS->isset_first(curval, &curixix))
        break; // Found
      if (curixix < HANDLER_SHARE_BUCKET->records())
        break; // Already pointing to next
      DBUG_RETURN(HA_ERR_KEY_NOT_FOUND);
      //===================================

      // select * from tsc where val > 9.999 order by val, id limit 2;
    case HA_READ_AFTER_KEY:
      // If curval exists, we'll point to last entry with that value, so we
      // want the next one again. If curval does not exist, we've
      // overrun... backwards (pointing to first entry smaller than curval) so
      // we want the next one in this case too! (see READ_BEFORE_KEY, analogous)
      BATS->isset_last(curval, &curixix);
      ++curixix;
      if (curixix < HANDLER_SHARE_BUCKET->records())
        break;
      DBUG_RETURN(HA_ERR_END_OF_FILE);
      //===================================

    case HA_READ_BEFORE_KEY:
      // If curval exists, &curixix points to it and we want the previous
      // entry. If curval does *not* exist, we're pointing to the first entry
      // bigger than curval, so we want the previous one in this case too!
      BATS->isset_first(curval, &curixix);
      --curixix;
      if (curixix != HA_HANDLER_NOTFOUND)
        break;
      DBUG_RETURN(HA_ERR_END_OF_FILE);
      //================================

    case HA_READ_PREFIX_LAST:
      if (BATS->isset_last(curval, &curixix))
        break;
      DBUG_RETURN(HA_ERR_END_OF_FILE);
      //================================

      // Used for e.g. val <= N ORDER BY val DESC
    case HA_READ_PREFIX_LAST_OR_PREV:
    case HA_READ_KEY_OR_PREV: // Not used b/c field 1 isn't unique?
      if (BATS->isset_last(curval, &curixix))
        break;
      if (curixix != HA_HANDLER_NOTFOUND)
        break;
      DBUG_RETURN(HA_ERR_END_OF_FILE);

    default:
      LOGG("Ouch!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n");
      DBUG_RETURN(HA_ERR_UNSUPPORTED);
    }
    curpos = BATS->index[curixix];
    curval = BATS->buffer[curpos];

    if (key_len > handler->table->field[1]->pack_length())
    {
      IFACE_LOG_LOCAL;
      longlong val2;
      val2 = handler->lget(0, key + handler->table->field[1]->pack_length());
      LOG2("val=%.8g, val2=%lld\n", (double)curval, val2);
      LOGG("Chickening out\n");
      my_error(HA_ADMIN_NOT_IMPLEMENTED, MYF(0),
               "using 2 parts of reverse index(1) in search ops");
      CRASH("index_read() using 2 parts of reverse index(1) in search ops");
      DBUG_RETURN(HA_ERR_UNSUPPORTED);
    }
  }

  handler->table->status = 0;
  if (find_flag == HA_READ_KEY_EXACT)
    LOCALLOG(LOGG("ixr: status=0\n"));
  putrow(buf, curpos, curval);
  if (find_flag == HA_READ_KEY_EXACT)
    LOCALLOG(LOGG("ixr: putrow ok\n"));
  LOG2("curpos: %lld, curval: %.8g\n", curpos, (double)curval);
  DBUG_RETURN(0);
}

// select * from tsc where val = 943.718 order by val desc, id desc limit 2;
TOOLBOX(int)
index_read_last(uchar *buf, const uchar *key, uint key_len)
{
  DBUG_ENTER1("TOOLBOX::index_read_last [%s]", SHARE->table_name);
  BATS_WAKEUP;
  int index_read_result =
      (handler->ACTINX == 0 || handler->ACTINX == 2)
          ? index_read(buf, key, key_len, HA_READ_KEY_EXACT)
          : index_read(buf, key, key_len, HA_READ_PREFIX_LAST); // Reverse index
  DBUG_RETURN(index_read_result);
}

TOOLBOX(int)
index_first(uchar *buf)
{
  DBUG_ENTER1("TOOLBOX::index_first [%s]", SHARE->table_name);
  BATS_WAKEUP;
  LOG1("actinx=%d\n", handler->ACTINX);

  if (!BITS->records())
    DBUG_RETURN(HA_ERR_END_OF_FILE);
  curpos = -1;
  curixix = -1;
  int tmp = index_next(buf);
  LOG3("curixix=%lld, curpos=%lld, curval=%.8g\n", curixix, curpos, (double)curval);

  DBUG_RETURN(tmp); // Which index???
}

TOOLBOX(int)
index_next(uchar *buf)
{
  const char *fft = handler->fft((enum ha_rkey_function)handler->act_fflag);
  DBUG_ENTER3("TOOLBOX::index_next actinx=%d fft=%s [%s]", handler->ACTINX, fft, SHARE->table_name);

  if (handler->ACTINX == 0 || handler->ACTINX == 2)
  {
    int rnd_next_result = rnd_next(buf);
    DBUG_RETURN(rnd_next_result);
  }

  int ret_code = HA_ERR_END_OF_FILE;
  if (++curixix < BITS->records() + BATS->have_deleted)
  { //!!Concurent index-read + deletes!!
    curpos = BATS->index[curixix];
    curval = BATS->buffer[curpos];
    putrow(buf, curpos, curval);
    ret_code = 0;
  }
  LOG4("ret_code=%d, curixix=%lld, curpos=%lld, curval=%.8g\n", ret_code, curixix, curpos, (double)curval);
  DBUG_RETURN(ret_code);
}

TOOLBOX(int)
index_last(uchar *buf)
{
  DBUG_ENTER("TOOLBOX::index_last");
  BATS_WAKEUP;
  LOG2("actinx=%d, act_fflag=%s\n", handler->ACTINX,
       handler->fft((enum ha_rkey_function)handler->act_fflag));

  if (!BITS->records())
    DBUG_RETURN(HA_ERR_END_OF_FILE);
  LOGG("Hello1\n");
  if (handler->ACTINX == 0 || handler->ACTINX == 2)
  {
    curpos = BITS->max_value;
    curval = BATS->buffer[curpos];
  }
  else
  {
    LOGG("This is the suspect branch\n");
    curixix = HANDLER_SHARE_BUCKET->records() - 1;
    LOG1("curixix=%llu\n", curixix);
    LOG1("BATS->index=%llx\n", (ulonglong)BATS->index);
    curpos = BATS->index[curixix];
    LOG1("curpos=%llu\n", curpos);
    curval = BATS->buffer[curpos];
    LOGG("curval=fetched\n");
    LOG3("curixix=%lld, curpos=%lld, curval=%.12g\n", curixix, curpos, (double)curval);
  }

  putrow(buf, curpos, curval);
  LOGG("DBUG_RETURNing from index_last\n");
  DBUG_RETURN(0 /*TOOLBOX::index_last*/);
}

TOOLBOX(int)
index_prev(uchar *buf)
{
  DBUG_ENTER("TOOLBOX::index_prev");
  LOG2("%s::index_prev[%s]\n", SHARE->table_name,
       handler->fft((enum ha_rkey_function)handler->act_fflag));

  if (handler->ACTINX == 0 || handler->ACTINX == 2)
  {
    int rnd_prev_result = rnd_prev(buf);
    DBUG_RETURN(rnd_prev_result);
  }
  ulonglong ret_code = HA_ERR_END_OF_FILE;
  if (--curixix != HA_HANDLER_NOTFOUND)
  {
    curpos = BATS->index[curixix];
    curval = BATS->buffer[curpos];
    putrow(buf, curpos, curval);
    ret_code = 0;
  }
  LOG4("ret_code=%llu, curixix=%llu, curpos=%llu, curval=%.12g\n", ret_code, curixix, curpos, (double)curval);
  DBUG_RETURN(ret_code);
}
