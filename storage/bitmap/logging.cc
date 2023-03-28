// MySql 5.5 changes:
#include "all-mysql-includes.h"

#include "bucket.h"

#define LOGGING_CC // Controls instantiation/initialization of variables
#include "logging.h"

#ifndef __APPLE__
pthread_spinlock_t logfile_spin;
void logfile_spin_lock() { pthread_spin_lock(&logfile_spin); }
void logfile_spin_lock_yield() { pthread_spin_unlock(&logfile_spin); }
#endif

#ifdef __APPLE__ // Silly, lacks pthread_spin_locks

#include <libkern/OSAtomic.h>
OSSpinLock logfile_spin = 0;
void logfile_spin_lock() { OSSpinLockLock(&logfile_spin); }
void logfile_spin_lock_yield() { OSSpinLockUnlock(&logfile_spin); }

#endif

extern MEI *mei; // Will be set by loginfo
// static MEI ** mei2=&mei; // through this pointer

// Ensures that loginfo structure is gets freed
void loginfo_destructor(void *loginfo_ptr)
{
  LogInfo *loginfo = (LogInfo *)loginfo_ptr;
  fprintf(logfile, "%s Destroying LogInfo %llx\n", loginfo->prefix_buffer, (ulonglong)loginfo_ptr);
  fflush(logfile);

  my_free(loginfo_ptr); // my_alloc() system done it already during shutdown?
}

// Returns thread's value of <engine>_thread_logging
//
// We declare/reserve space for the thread variable here; upon request
// we will copy the *actual* thread variable for the given storage
// engine into this space, then use THDVAR(current_thd,thread_logging)
// to get the value.

static MYSQL_THDVAR_ULONGLONG(thread_logging, PLUGIN_VAR_RQCMDARG, "???", NULL,
                              NULL, 1, 0, 128 * 1024 * 1024ULL * 1024, 0);

int thread_logging(MEI *mei)
{
  return 0;
  // Overwrite contents from the thread variable pointed to by the MEI
  memcpy(&mysql_sysvar_thread_logging, mei->thread_logging_sysvar_ptr, sizeof(mysql_sysvar_thread_logging));

  // Access and return
  return THDVAR(current_thd, thread_logging);
}

// loginfo(): return thread-specific loginfo pointer from loginfo_key
//
// We need an initializer for loginfo_key - run w/pthread_once!
//
// If "our" loginfo_key does not contain ptr to loginfo struct, create
// one, initialize, and assign to key.

/** From 'man pthread_key_create':
    Although the same key value may be used by different threads, the
    values bound to the key by pthread_setspecific() are maintained on a
    per-thread basis and persist for the life of the calling thread.  **/

static pthread_key_t loginfo_key;
static void loginfo_key_create()
{
  pthread_key_create(&loginfo_key, loginfo_destructor);
}

LogInfo *loginfo_fetch(MEI *mei)
{
  static pthread_once_t loginfo_key_created = PTHREAD_ONCE_INIT;
  pthread_once(&loginfo_key_created, loginfo_key_create);

  // We cannot use pthread_setspecific during shutdown - well, at
  // least not in the shutdown thread. So we keep a static one for
  // that use.

  // Let's *assume* that we can use pthread_getspecific,
  // though. Hopefully, MySQL first kills all other threads before
  // anything which might cause a log system call, and we want those
  // threads to be able to use their pre-existing loginfos.
  static std::atomic<bool> shutdown_in_progress;

  LogInfo *loginfo = (LogInfo *)pthread_getspecific(loginfo_key);
  if (!loginfo)
  {

    static LogInfo shutdown_loginfo = {0, 0, ""};
    if (shutdown_in_progress)
      return &shutdown_loginfo;

    // Can't use LOGG since it needs the LogInfo item!
    ulong dbug_id = my_thread_dbug_id();
    loginfo = (LogInfo *)my_malloc((PSI_memory_key)0, sizeof(LogInfo), MYF(0));
    if (!loginfo)
      fprintf(logfile, "LogInfo malloc failed!\n");
    fprintf(logfile, "Created LogInfo for thd %lu : %llx\n", dbug_id, (ulonglong)loginfo);
    fflush(logfile);
    pthread_setspecific(loginfo_key, (void *)loginfo);

    loginfo->pref = 0;
    loginfo->dbug_id = dbug_id;
  }

  return loginfo;
}

// Limit size of log - could grow enormously fast!
//
// But we need to safeguard for thread safety - logfile is
// after all pointing to a global entity!!

void loglimit(MEI *mei)
{

  if (ftell(logfile) > 1 * 1024 * (long)1024 * 1024)
  {
    int ftruncate_result = ftruncate(fileno(logfile), 0);
    fprintf(logfile, "Logfile truncated, result %d\n", ftruncate_result);
    fflush(logfile);
  }
}

void log_indent_increment(MEI *mei) //
{
  loginfo_fetch(mei)->pref += 2;
}

void log_indent_decrement(MEI *mei) //
{
  LogInfo *loginfo = loginfo_fetch(mei);
  loginfo->pref -= 2;
  if (loginfo->pref < 0)
  {
    fprintf(logfile, "\nLogInfo prefix < 0 - bounded!!!!!!!!!!!!!\n");
    fflush(logfile);
    loginfo->pref = 0;
  }
}

char *log_indent(MEI *mei) // Log prefix for this thread
{
  LogInfo *loginfo = loginfo_fetch(mei);
  loglimit(mei);
  loginfo->prefix_buffer[0] = 0;
  for (int i = 0; i < loginfo->pref / 2 && i < 1000; i++)
    strcat(loginfo->prefix_buffer, "| ");
  return loginfo->prefix_buffer;
}

//-------------------
// pthread_once'd by <engine>_init, which is called on startup ---------------
// per mysql_declare_plugin(<engine>)

static void logfile_init_once(void)
{
  const char *logfilename = "../bucket.log";
  rename("../bucket.log.1", "../bucket.log.2");
  rename("../bucket.log.0", "../bucket.log.1");
  rename("../bucket.log", "../bucket.log.0");
#ifndef __APPLE__
  pthread_spin_init(&logfile_spin, 0);
#endif

  logfile = fopen(logfilename, "a");
  int mode = (S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH);
  chmod(logfilename, mode);
  fprintf(logfile, "Logfile inited!\n");
  fflush(logfile);
}

void logfile_init(void)
{
  static pthread_once_t logfile_inited = PTHREAD_ONCE_INIT;
  pthread_once(&logfile_inited, logfile_init_once);
}
