#ifdef USE_PRAGMA_IMPLEMENTATION
#pragma implementation // gcc: Class implementation
#endif

#define MYSQL_SERVER 1
#include "all-mysql-includes.h"

#define IFACE_CC
#define FULL_LOGGING
#include "bucket.h"
#include "logging.h"
#include "bitbucket.h"

static bitbucket *free_checksum_ids = 0;

static void bucket_init_free_checksum_ids(MEI *mei) // Called only once: bucket_init()
{
  IFACE_LOG_LOCAL;
  DBUG_ENTER("bucket_init_free_checksum_ids");
  free_checksum_ids = new bitbucket(LOCK_OPEN, mei);
  free_checksum_ids->bucket_lock();
  for (uint i = 20000; i > 0; i--)
    free_checksum_ids->set(i);
  free_checksum_ids->bucket_lock_yield();
  DBUG_VOID_RETURN;
}

static uint bucket_get_id(MEI *mei)
{
  IFACE_LOG_LOCAL;
  DBUG_ENTER("bucket_get_id");
  free_checksum_ids->bucket_lock();
  uint id = free_checksum_ids->next(0); // Note - start from 0 every time!
  free_checksum_ids->unset(id);
  free_checksum_ids->bucket_lock_yield();
  DBUG_RETURN(id);
}

static void bucket_free_id(uint i, MEI *mei)
{
  DBUG_ENTER("bucket_free_id");
  LOG1("Freeing id: %d\n", i);
  if (free_checksum_ids)
    free_checksum_ids->set(i);
  DBUG_VOID_RETURN;
}

Bucket *bucket_find_bucket(uint no, MEI *mei)
{
  IFACE_LOG_LOCAL;

  DBUG_ENTER("bucket_find_bucket");
  MUTEX_LOCK(&mei->engine_mutex, mei->engine_name, mei);

  ulong idx = 0;
  st_bucket_share *share;
  Bucket *bucket = 0;
  Bucket *found = 0;

  while ((share = (st_bucket_share *)my_hash_element(&mei->open_tables, idx)))
  {
    LOCALLOG(LOG2("bucket_find_bucket idx=%lu, share=%llx\n", idx, (ulonglong)share));
    bucket = share->bucket;
    LOCALLOG(LOG1("bucket_find_bucket bucket=%llx\n", (ulonglong)bucket));
    if (bucket->checksum() == no)
    {
      LOG2("Trying %s [%s]\n", share->table_name, mei->engine_name);
      if (bucket->elocked())
      {
        LOG2("%s:[%s] Was elocked\n", share->table_name, mei->engine_name);
        found = bucket;
        break;
      }
      else
      {
        LOCALLOG(LOG2("%s:[%s] Was NOT elocked!!\n", share->table_name, mei->engine_name));
        break;
      }
    }
    idx++;
  }
  MUTEX_LOCK_YIELD(&mei->engine_mutex, mei->engine_name, mei);
  LOG1("Found: %llx\n", (longlong)found);
  DBUG_RETURN(found);
}

void MUTEX_LOCK(pthread_mutex_t *m, const char *s, MEI *mei)
{
  // IFACE_LOG_LOCAL;
  pthread_mutex_lock(m);
  LOG2("Lock       mtx %s=%llx\n", s, (ulonglong)m);
}

void MUTEX_LOCK_YIELD(pthread_mutex_t *m, const char *s, MEI *mei)
{
  // IFACE_LOG_LOCAL;
  LOG2("Lock Yield mtx %s=%llx\n", s, (ulonglong)m);
  pthread_mutex_unlock(m);
}

int MUTEX_LOCK_TRY(pthread_mutex_t *m, const char *s, MEI *mei)
{
  IFACE_LOG_LOCAL;
  int success = pthread_mutex_trylock(m) == 0;
  LOG3("Lock Try   mtx %s=%llx -> %s\n", s, (ulonglong)m, success ? "ok" : "busy");
  return success;
}

Bucket::Bucket(const char *_name, EngineLocked engine_locked, int n_bucket_children, MEI *_mei)
    : bucket_engine_locked(engine_locked),
      n_bucket_children(n_bucket_children),
      mei(_mei), elock_count(0),
      hibernated(1) /*Not yet loaded!*/,
      crashed(0) /*Subclass must decide*/,
      Nrecords(0), ctime(0), mtime(0), atime(0), ltime(0)
{
  IFACE_LOG_LOCAL;

  static volatile ulonglong anon = 0; // Whoa! Access w/__sync_fetch_and_add!

  DBUG_ENTER1("Bucket::Bucket [%s]", _name);

  if (n_bucket_children)
  {
    int alloc_size = n_bucket_children * sizeof(Bucket *);
    bucket_children = (Bucket **)my_malloc((PSI_memory_key)0, alloc_size, MYF(MY_WME | MY_ZEROFILL));
    if (!bucket_children)
    {
      CRASH("Could not allocate memory for bucket_children!\n");
    }
  }

  strmov(name, _name);
  if (!strcmp(name, "anon_bb"))
    snprintf(name + 7, 19, "%015llu", __sync_fetch_and_add(&anon, (ulonglong)1));

  LOG1("bucket(%s)\n", name);
  pthread_mutex_init(&bucket_mutex, MY_MUTEX_INIT_FAST);

  bucket_lock();

  // Now, we want to get the checksum id for this table.

  // NOTE SPECIAL CASE! bucket_get_id uses the bit *BUCKET* free_checksum_ids
  // to find an ID. We solve it this way: The very first bitbucket to be
  // created is always the free_checksum_ids one. Special case, checksum=0.
  // Also, when this special bitbucket is destroyed, it should *NOT* set it's
  // id as free (because that would require a setting of a bit in the bucket
  // that's being destroyed!!)

  if (!free_checksum_ids)
    checksum_id = 0;
  else
    checksum_id = bucket_get_id(mei);

  bucket_lock_yield();
  DBUG_VOID_RETURN;
}

Bucket::~Bucket()
{
  DBUG_ENTER("Bucket::~Bucket");
  pthread_mutex_destroy(&bucket_mutex);
  // SPECIAL CASE: checksum_id == 0 should not be free'd. See above
  if (checksum_id)
    bucket_free_id(checksum_id, mei);
  DBUG_VOID_RETURN;
}

uint Bucket::checksum(void) const
{
  DBUG_ENTER("Bucket::checksum");
  DBUG_RETURN(checksum_id);
}

void Bucket::bucket_lock(void) { MUTEX_LOCK(&bucket_mutex, name, mei); }
void Bucket::bucket_lock_yield(void) { MUTEX_LOCK_YIELD(&bucket_mutex, name, mei); }
int Bucket::bucket_lock_try(void) { return MUTEX_LOCK_TRY(&bucket_mutex, name, mei); }

void Bucket::bucket_engine_lock(void)
{
  IFACE_LOG_LOCAL;
  DBUG_ENTER("bucket::bucket_engine_lock");
  // If engine already locked, we have a parent that has already done it! And
  // we *did* inform our children at that point. I.e. nothing to do!

  if (bucket_engine_locked == LOCK_LOCKED)
  {
    LOGG("Engine already locked by parent\n");
    DBUG_VOID_RETURN;
  }

  if (bucket_engine_locked == LOCK_OURS)
  {
    CRASH("What?? *We* have the engine lock (LOCK_OURS)? Bad programming!\n");
  }

  MUTEX_LOCK(&mei->engine_mutex, mei->engine_name, mei);

  bucket_engine_locked = LOCK_OURS; // We got it now!

  for (int i = 0; i < n_bucket_children; i++)
  {
    if (bucket_children[i])
      bucket_children[i]->bucket_engine_lock_set(LOCK_LOCKED);
  }

  DBUG_VOID_RETURN;
}

void Bucket::bucket_engine_lock_yield(void)
{
  IFACE_LOG_LOCAL;
  DBUG_ENTER("Bucket::bucket_engine_lock_yield");

  if (bucket_engine_locked == LOCK_LOCKED)
  {
    LOGG("Engine locked by parent - do nothing\n");
    DBUG_VOID_RETURN;
  }

  if (bucket_engine_locked == LOCK_OPEN)
  {
    CRASH("Hey - lock is already open! Bad programming!\n");
    DBUG_VOID_RETURN;
  }

  for (int i = 0; i < n_bucket_children; i++)
  {
    if (bucket_children[i])
      bucket_children[i]->bucket_engine_lock_set(LOCK_OPEN);
  }

  bucket_engine_locked = LOCK_OPEN;

  MUTEX_LOCK_YIELD(&mei->engine_mutex, mei->engine_name, mei);
  DBUG_VOID_RETURN;
}

void Bucket::bucket_engine_lock_set(EngineLocked engine_locked)
{
  IFACE_LOG_LOCAL;
  DBUG_ENTER1("Bucket::bucket_engine_lock_set(%d)", (int)engine_locked);
  bucket_engine_locked = engine_locked;
  for (int i = 0; i < n_bucket_children; i++)
  {
    LOG1("Setting child %d\n", i);
    if (bucket_children[i])
      bucket_children[i]->bucket_engine_lock_set(engine_locked);
  }
  DBUG_VOID_RETURN;
}

void Bucket::elock(void)
{
  IFACE_LOG_LOCAL;
  DBUG_ENTER("Bucket::elock");
  bucket_lock();
  if (++elock_count == 1)
    wakeup();
  bucket_lock_yield();
  atime = time(NULL);
  DBUG_VOID_RETURN;
}

void Bucket::unelock(void)
{
  IFACE_LOG_LOCAL;
  DBUG_ENTER("Bucket::unelock");
  bucket_lock();
  --elock_count;
  bucket_lock_yield();
  atime = time(NULL);
  DBUG_VOID_RETURN;
}

int Bucket::elocked(void) { return elock_count; }
int Bucket::is_crashed(void)
{
  // IFACE_LOG_LOCAL;
  DBUG_ENTER1("Bucket::is_crashed[%s]", name);
  bucket_lock();
  int crash = crashed;
  bucket_lock_yield();
  if (crash)
  {
    IFACE_LOG_LOCAL;
    LOCALLOG(LOG1("Bucket::is_crashed : %s\n", name));
    LOG1("elock_count: %d\n", elock_count);
  }
  DBUG_RETURN(crash);
}

int Bucket::sleeping() { return hibernated; }

void Bucket::reg_wakeup(void)
{
  IFACE_LOG_LOCAL;
  DBUG_ENTER("Bucket::reg_wakeup");
  bucket_engine_lock();
  mei->wakeups++;
  bucket_engine_lock_yield();
  DBUG_VOID_RETURN;
}

// To be called when engine mutex is already known to be locked by
// caller: in particular, when freeing data during hibernation

void Bucket::memcount_locked(longlong bytes)
{
  mei->mem_used += bytes;
  LOG3("%s:memcount_locked(%lld) => %ld\n", name, bytes, mei->mem_used);
}

void Bucket::memcount(longlong bytes)
{
  IFACE_LOG_LOCAL;
  DBUG_ENTER("Bucket::memcount");
  if (bucket_engine_locked == LOCK_OURS)
  {
    memcount_locked(bytes);
    DBUG_VOID_RETURN;
  }
  bucket_engine_lock();
  mei->mem_used += bytes;
  LOG3("%s:memcount(%lld) => %ld\n", name, bytes, mei->mem_used);
  while (bytes > 0 && mei->mem_used > (longlong)mei->max_mem)
  {
    IFACE_LOG_LOCAL;
    LOG3("memcount(%lld) => %ld > %llu, hibernating!!\n", bytes, mei->mem_used, mei->max_mem);
    ulong idx = 0;
    st_bucket_share *share; // => SHAREBUCKET macro works
    time_t mintime = time(NULL);
    Bucket *mintime_bucket = NULL;
    while ((share = (st_bucket_share *)my_hash_element(&mei->open_tables, idx)))
    {
      Bucket *bucket = share->bucket;
      if (bucket && bucket->bucket_lock_try())
      {
        if (!bucket->sleeping() && !bucket->elocked() && bucket->atime < mintime)
        {
          if (mintime_bucket)
            mintime_bucket->bucket_lock_yield();
          mintime = bucket->atime;
          mintime_bucket = bucket;
        }
        else
        {
          if (bucket->elocked())
            LOG1("%s:Was elocked\n", share->table_name);
          bucket->bucket_lock_yield();
        }
      }
      idx++;
    }
    if (!mintime_bucket)
    {
      LOGG("Can't hibernate anything!\n");
      break;
    }
    mintime_bucket->hibernate();
    mei->hibernations++;
    mintime_bucket->bucket_lock_yield();
  }
  bucket_engine_lock_yield();
  DBUG_VOID_RETURN;
}

// Hmm... would it not be better if all files were opened (once for
// all) and closed (once for all) inside class bucket? It would have to
// be a parameter class taking the number of files to leave open as a
// parameter... for later!

void Bucket::log_mmap_failure(void)
{
  const char *errtxt;
  switch (errno)
  {
  case EACCES:
    errtxt = "EACCESS";
    break;
  case EAGAIN:
    errtxt = "EAGAIN";
    break;
  case EBADF:
    errtxt = "EBADF";
    break;
  case EINVAL:
    errtxt = "EINVAL";
    break;
  case ENFILE:
    errtxt = "ENFILE";
    break;
  case ENODEV:
    errtxt = "ENODEV";
    break;
  case ENOMEM:
    errtxt = "ENOMEM";
    break;
  case EPERM:
    errtxt = "EPERM";
    break;
    // case ETXTBUSY  : errtxt = "ETXTBUSY";  break;
  case EMFILE:
    errtxt = "EMFILE";
    break;
  case ENXIO:
    errtxt = "ENXIO";
    break;
  case EOVERFLOW:
    errtxt = "EOVERFLOW";
    break;
  default:
    errtxt = "???";
  }
  LOG1("mmap_failure: %s\n", errtxt);
  sleep(60);
}

// old XOR old_size should be zero for consistency!
// Unmap existing mapping if old & old_size are nonzero
// If old_size AND new_size == 0, use/return existing file size
// On return, new_size/newstat contains current info, unless:
// Returns (void*)0 if new_size ends up zero,

// Wrapper for overloading:
void *Bucket::resize_file_map(char *fname, void *old, size_t old_size, size_t &new_size)
{
  DBUG_ENTER("Bucket::resize_file_map(fname,old,old_size,&new_size)");
  MY_STAT newstat;
  void *tmpres;
  newstat.st_size = new_size;
  tmpres = resize_file_map(fname, old, old_size, &newstat);
  new_size = newstat.st_size;
  DBUG_RETURN(tmpres);
}

void *Bucket::resize_file_map_ro(char *fname, void *old, size_t old_size, size_t new_size)
{
  DBUG_ENTER("Bucket::resize_file_map - one-way");
  DBUG_RETURN(resize_file_map(fname, old, old_size, new_size));
}

void *Bucket::resize_file_map(char *fname, void *old, size_t old_size, MY_STAT *newstat)
{
  File file;
  DBUG_ENTER("Bucket::resize_file_map");
  LOG1("resizing: %s\n", fname);

#define new_size newstat->st_size

  if ((old ? 1 : 0) ^ (old_size ? 1 : 0))
  {
    LOG1("Bucket::resize_file_map(%s): old XOR old_size!\n", name);
    LOG2("old=%llu, old_size=%llu\n", (ulonglong)old, (ulonglong)old_size);
    my_error(HA_ADMIN_NOT_IMPLEMENTED, MYF(0), "inconsistency handling\n");
    CRASH("Bucket::resize_file_map inconsistency!");
  }

  LOG2("%s new_size: %llu\n", fname, (ulonglong)new_size);
  if (new_size && (ulonglong)new_size == old_size)
    DBUG_RETURN(old);

  if (!strncmp(fname, "anon_bb", 7))
  {
    LOGG("Oh... ANONYMOUS!\n");
    if (!new_size)
    {
      my_free(old);
      memcount(-old_size);
      LOGG("NOTE: RETURNING ZERO POINTER TO BUFFER/INDEX\n");
      DBUG_RETURN((void *)0);
    }
    void *p = my_realloc((PSI_memory_key)0, old, new_size, MY_ALLOW_ZERO_PTR);
    memcount(new_size - old_size);
    // Init new mem to zero!!
    for (char *z = old_size + (char *)p; z < new_size + (char *)p; z++)
      *z = (char)0;
    if (!p)
      my_error(HA_ADMIN_NOT_IMPLEMENTED, MYF(0), "failed realloc");
    DBUG_RETURN(p);
  }

  // Will only get here if we're mmap'ing to a file!
  if (old && old_size)
  {
    memcount(-old_size);
    if (munmap(old, old_size))
    {
      my_error(HA_ADMIN_NOT_IMPLEMENTED, MYF(0), "unmap error handling");
      DBUG_RETURN(MAP_FAILED);
    }
  }

  if ((file = my_open(fname, O_RDWR | O_CREAT, MYF(MY_WME))) == -1)
  {
    LOG1("File %s could not be opened\n", fname);
    my_error(HA_ADMIN_NOT_IMPLEMENTED, MYF(0), "file open error handling");
    DBUG_RETURN(MAP_FAILED);
  }

  MY_STAT curstat;
  fstat(file, &curstat);

  if ((old_size || new_size) && curstat.st_size != new_size)
  {
    int truncate_result = ftruncate(file, new_size);
    LOG3("Set file %s to %llu bytes, result %d\n", fname, (ulonglong)new_size, truncate_result);
  }
  fstat(file, newstat);

  if (new_size)
    memcount(new_size);
  void *p = new_size ? mmap((void *)0, new_size, PROT_READ | PROT_WRITE, MAP_FILE | MAP_SHARED, file, 0)
                     : 0L;
  my_close(file, MYF(0));
  if (p == MAP_FAILED)
  {
    LOGG("Hmm... mmap'ing does not seem to have worked!\n");
    my_error(HA_ADMIN_NOT_IMPLEMENTED, MYF(0), "unmap error handling");
  }
  if (!p)
    LOGG("NOTE: RETURNING ZERO POINTER TO BUFFER/INDEX\n");
  DBUG_RETURN((void *)p);
}

// Non-methods - used by handlers but only bucket parts needed

st_bucket_share *bucket_get_share(const char *table_name, MEI *mei)
{
  IFACE_LOG_LOCAL;
  DBUG_ENTER1("bucket_get_share [%s]", table_name);
  st_bucket_share *share;
  uint length;

  // Engine needs to be locked during this operation and *UNTIL* the share is
  // registered (to prevent two shares for the same table). And the share
  // should not be registered until it's ready for use by others. That does
  // not occur until the share->bucket pointer has been initialized.

  // The share->bucket can't be initialized here, since we don't know what
  // type it should be.  And the initialization value for share->bucket can't
  // be created before the call to us, since that would risk creating two
  // buckets for a single table - precisely what the share mechanism is
  // supposed to prevent!  Ah...

  // So, we leave it up to the caller to lock the engine and call us. We
  // "always" return a share pointer, but if share->bucket is null, it must be
  // initialized and then registered. Finally, the engine lock can be
  // released.

  length = (uint)strlen(table_name);

  share = (st_bucket_share *)my_hash_search(&mei->open_tables,
                                            (uchar *)table_name, length);

  if (!share)
  { // New share!

    // Set aside space for table_name (last struct elem): no extra free() call
    int alloc_size = sizeof(st_bucket_share) + length;
    share = (st_bucket_share *)my_malloc((PSI_memory_key)0, alloc_size, MYF(MY_WME | MY_ZEROFILL));

    if (!share)
      goto error;

    share->table_name_length = length;
    strmov(share->table_name, table_name);

    LOG1("CREATED NEW SHARE: %s\n", table_name);
    thr_lock_init(&share->thr_lock);
  }

  share->use_count++;

  LOG2("use_count=%d for %s\n", share->use_count, share->table_name);

error:

  DBUG_RETURN(share);
}

void bucket_register_share(st_bucket_share *share, MEI *mei)
{
  IFACE_LOG_LOCAL;
  DBUG_ENTER1("bucket_register_share: %llx", (ulonglong)share);
  if (my_hash_insert(&mei->open_tables, (uchar *)share))
  {
    CRASH("Couldn't insert share in hash!");
  }
  LOGG("INSERTED NEW SHARE IN HASH\n");
  DBUG_VOID_RETURN;
}

// Returns use_count - delete share bucket if zero!
// CALL WITH ENGINE LOCKED!

int bucket_free_share(st_bucket_share *share, MEI *mei)
{
  IFACE_LOG_LOCAL;
  int tmp;
  DBUG_ENTER1("bucket_free_share [%s]", share->table_name);

  // If there are more users - just lock_yield engine & return
  if ((tmp = --share->use_count))
  {
    DBUG_RETURN(tmp);
  }

  LOGG("No more share users - DELETING HASH ENTRY! "
       "TABLE BUCKET SHOULD BE DELETED TOO!\n");

  my_hash_delete(&mei->open_tables, (uchar *)share); // Can no longer be found!
  thr_lock_delete(&share->thr_lock);
  // mysql_mutex_delete() not required - share lock is inside bucket!
  my_free(share);

  DBUG_RETURN(0);
}

void bucket_free_key(st_bucket_share *share);
uchar *bucket_get_key(st_bucket_share *share, size_t *length,
                      my_bool not_used __attribute__((unused)));

/*
   First things first:

   The *handlers* are initialized through their <handler_name>_init functions
   - as per the mysql_declare_plugin(<handler_name>) MACRO (sic: lowercase!).

   Both of them are set to:

   Init the log file

   Call their respective static <engine>_mei_init() funcs (engine_name,
   eng_name, engine_mutex initialized)

   Call bucket_init().

   UDFs refer to global symbols bitmap_mei *or* scalarray_mei
*/

void bucket_init(MEI *mei)
{
  DBUG_ENTER("bucket_init");

  // No race conditions here, this is startup!
  if (!free_checksum_ids)
    bucket_init_free_checksum_ids(mei);

  DBUG_VOID_RETURN;
}

void bucket_fini(MEI *mei)
{
  DBUG_ENTER("bucket_fini");

  // During shutdown - no race condition possible! So we don't need to use
  // pthread_once for free_checksum_ids destruction, just a simple nulling.

  // But... ho-hum. When free_checksum_ids is destroyed, its bucket destructor
  // is called, which would set id 0 to free (i.e. bit 0 to 1). Which triggers
  // an allocation of space!!  Yikes. So, never allow *freeing* of bucket 0!

  if (free_checksum_ids)
    delete free_checksum_ids;
  free_checksum_ids = 0;

  DBUG_VOID_RETURN;
}
// Non-methods without handler-supplied "mei": Global copy ok, LOGGING only!!

void bucket_free_key(st_bucket_share *share)
{
  MEI *mei = share->bucket->mei;
  IFACE_LOG_LOCAL;
  DBUG_ENTER1("bucket_free_key[%s]", share->table_name);
  LOGG("Nothing to be done - hash key freed as part of share\n");
  DBUG_VOID_RETURN;
}

uchar *bucket_get_key(st_bucket_share *share, size_t *length,
                      my_bool not_used __attribute__((unused)))
{
  MEI *mei = share->bucket->mei;
  IFACE_LOG_LOCAL;
  DBUG_ENTER1("bucket_get_key [%s]", share->table_name);
  *length = share->table_name_length;
  DBUG_RETURN((uchar *)share->table_name);
}
