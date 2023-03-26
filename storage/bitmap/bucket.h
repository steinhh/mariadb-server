#ifndef IFACE_H
#define IFACE_H

#include <sys/timeb.h>

#define INTERVAL(a, b) (b.time - a.time + 0.001 * (b.millitm - a.millitm))

typedef struct
{
  const char *engine_name;
  const char *eng_name;
  ulonglong max_mem;
  ulonglong logging;
  void *thread_logging_sysvar_ptr;
  volatile long mem_used;
  volatile long hibernations;
  volatile long wakeups;
  volatile long locks;
  pthread_mutex_t engine_mutex;
  HASH open_tables;
} memory_engine_interface;

typedef memory_engine_interface MEI;

class Bucket;

Bucket *bucket_find_bucket(uint no, MEI *mei);

// We name our mutex operations such that we can easily grep for them using
// patterns [ prefix ]_lock[ _operation ] or [ PREFIX ]_LOCK[ _OPERATION ]

void MUTEX_LOCK(pthread_mutex_t *m, const char *s, MEI *mei);
void MUTEX_LOCK_YIELD(pthread_mutex_t *m, const char *s, MEI *mei);
int MUTEX_LOCK_TRY(pthread_mutex_t *m, const char *s, MEI *mei);

typedef enum enginelocked
{
  LOCK_OPEN,
  LOCK_LOCKED,
  LOCK_OURS
} EngineLocked;

class Bucket
{
  struct timeb start, end;
  uint checksum_id;

public:
  char name[FN_REFLEN]; // Name is common to the entire table...
                        // File names, on the other hand, are NOT!!!

  EngineLocked bucket_engine_locked;
  Bucket **bucket_children;
  int n_bucket_children;

  MEI *mei;

  pthread_mutex_t bucket_mutex; // Locked: we may be in undefined state!

  int elock_count;
  int hibernated;
  int dirty;
  int crashed;
  ulonglong Nrecords; // NOT signed any more - beware!
  time_t ctime, mtime, atime, ltime;

  Bucket(void) {} // Dummy

  Bucket(const char *_name, EngineLocked _engine_is_locked, int _n_bucket_children, MEI *mei);
  ~Bucket(void);

  virtual void load(void) = 0;

  void starttimer(void) { ftime(&start); }
  double interval(void)
  {
    ftime(&end);
    return INTERVAL(start, end);
  }

  void log_mmap_failure(void);

  // Overloaded:
  void *resize_file_map(char *fname, void *old, size_t old_size, MY_STAT *s);
  void *resize_file_map(char *fname, void *old, size_t old_size, size_t &new_size);
  void *resize_file_map_ro(char *fname, void *old, size_t old_size, size_t new_size);
  virtual ulonglong records(void) { return Nrecords; }

  void bucket_lock(void);
  void bucket_lock_yield(void);
  int bucket_lock_try(void);

  // These will inform the bucket_children about their parent's locking
  // status. When parent has locked engine, locking/yielding should not be
  // done!

  void bucket_engine_lock();
  void bucket_engine_lock_yield();
  void bucket_engine_lock_set(EngineLocked engine_locked);

  virtual void hibernate(void) = 0;
  virtual void wakeup(void) = 0;
  int sleeping(void);

  virtual ulonglong autoincrement_value(void) = 0;
  virtual ulonglong deleted(void) = 0;
  virtual ulonglong mean_rec_length(void) = 0;
  virtual ulonglong data_file_length() = 0;
  virtual void save(void) = 0;

  void memcount(longlong bytes);
  void memcount_locked(longlong bytes);
  void reg_wakeup(void);

  void elock(void);
  void unelock(void);
  int elocked(void);

  int is_crashed(void);

  uint checksum(void) const;
};

/*
  Shared structure for correct LOCK operation
*/

struct st_bucket_share
{
  THR_LOCK thr_lock;
  uint use_count; // No. of handlers using this bucket
  Bucket *bucket; // Bucket contains share's mutex!
  uint table_name_length;
  char table_name[1]; // Last item: allocate sizeof(st_bucket_share)+table_name_length
};

// Non-methods - used by handlers but only bucket parts needed, and MEI
// is supplied as required

st_bucket_share *bucket_get_share(const char *table_name, MEI *mei);

void bucket_register_share(st_bucket_share *share, MEI *mei);

int bucket_free_share(st_bucket_share *share, MEI *mei);

void bucket_free_key(st_bucket_share *share);

uchar *bucket_get_key(st_bucket_share *share, size_t *length,
                      my_bool not_used __attribute__((unused)));

void bucket_init(MEI *mei);

void bucket_fini(MEI *mei);

#ifndef IFACE_CC
#define IFACE_CC_EXTERN extern
#define IFACE_STORAGE_ENGINE_VAL
#else
#define IFACE_CC_EXTERN

#if __APPLE__
#define MYSQL_VERSION_ID_HACK 100025
#else
#define MYSQL_VERSION_ID_HACK MYSQL_VERSION_ID
#endif

#define MYSQL_HANDLERTON_INTERFACE_VERSION_HACK (MYSQL_VERSION_ID_HACK << 8)
#define IFACE_STORAGE_ENGINE_VAL = {MYSQL_HANDLERTON_INTERFACE_VERSION_HACK}

#endif

IFACE_CC_EXTERN
struct st_mysql_storage_engine bucket_storage_engine IFACE_STORAGE_ENGINE_VAL;

#endif
