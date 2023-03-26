#ifndef LOGGING_H
#define LOGGING_H

#ifdef DBUG_ENTER // Need to undo my_global defs
#undef DBUG_ENTER
#undef DBUG_RETURN
#undef DBUG_VOID_RETURN
#endif

int thread_logging(MEI *mei);
void logfile_init(void);
void logfile_spin_lock(void);
void logfile_spin_lock_yield(void);
void log_indent_increment(MEI *mei);
void log_indent_decrement(MEI *mei);
char *log_indent(MEI *mei);

typedef struct
{
  int pref; // Prefix value
  uint dbug_id;
  char prefix_buffer[1024];
} LogInfo;

#ifdef LOGGING_CC
#define IFACE_EXTERN
#define NULL_INIT = 0
#else
#define IFACE_EXTERN extern
#define NULL_INIT
#endif

IFACE_EXTERN FILE *logfile NULL_INIT;
IFACE_EXTERN int bucket_log_n NULL_INIT;
IFACE_EXTERN int bucket_logc NULL_INIT;
IFACE_EXTERN int bucket_log_local NULL_INIT;
IFACE_EXTERN int bucket_no_log_local NULL_INIT;

#ifndef SPEEDTEST
#define SPEEDTEST 0
#endif

// HELPER MACROS - can always be defined w/o causing actual logging
#define LOCALLOG(a)                            \
  {                                            \
    int bucket_no_log_local, bucket_log_local; \
    bucket_log_local = 1;                      \
    bucket_no_log_local = 0;                   \
    a;                                         \
  }
#define IFACE_LOG_LOCAL                      \
  int bucket_no_log_local, bucket_log_local; \
  bucket_no_log_local = 0;                   \
  bucket_log_local = 1
#define IFACE_NO_LOG_LOCAL \
  int bucket_no_log_local; \
  bucket_no_log_local = 1
#define IFACE_NLL_ON_SPEEDTEST \
  int bucket_no_log_local;     \
  bucket_no_log_local = SPEEDTEST

#define LOGGING ((bucket_log_local || mei->logging) & !bucket_no_log_local && thread_logging(mei))
#define PREF                                                                              \
  {                                                                                       \
    fprintf(logfile, "%s %03lu %s", mei->eng_name, my_thread_dbug_id(), log_indent(mei)); \
  }
#define FLUSH fflush(logfile)

#define PREFS "%s %05llu %s"
#define PREFP mei->eng_name, my_thread_dbug_id(), log_indent(mei)
#define DEC                    \
  {                            \
    log_indent_decrement(mei); \
  }
#define INC                    \
  {                            \
    log_indent_increment(mei); \
  }

#define LOGG_(a)                        \
  {                                     \
    if (LOGGING)                        \
    {                                   \
      fprintf(logfile, PREFS a, PREFP); \
      FLUSH;                            \
    }                                   \
  }
#define LOG1_(a, b)                        \
  {                                        \
    if (LOGGING)                           \
    {                                      \
      fprintf(logfile, PREFS a, PREFP, b); \
      FLUSH;                               \
    }                                      \
  }
#define LOG2_(a, b, c)                        \
  {                                           \
    if (LOGGING)                              \
    {                                         \
      fprintf(logfile, PREFS a, PREFP, b, c); \
      FLUSH;                                  \
    }                                         \
  }
#define LOG3_(a, b, c, d)                        \
  {                                              \
    if (LOGGING)                                 \
    {                                            \
      fprintf(logfile, PREFS a, PREFP, b, c, d); \
      FLUSH;                                     \
    }                                            \
  }
#define LOG4_(a, b, c, d, e)                        \
  {                                                 \
    if (LOGGING)                                    \
    {                                               \
      fprintf(logfile, PREFS a, PREFP, b, c, d, e); \
      FLUSH;                                        \
    }                                               \
  }
#define LOG5_(a, b, c, d, e, f)                        \
  {                                                    \
    if (LOGGING)                                       \
    {                                                  \
      fprintf(logfile, PREFS a, PREFP, b, c, d, e, f); \
      FLUSH;                                           \
    }                                                  \
  }

#define DBUG_ENTER_1(a) \
  {                     \
    if (LOGGING)        \
    {                   \
      LOGG_(a "\n");    \
      INC;              \
    }                   \
  }
#define DBUG_ENTER_2(a, b) \
  {                        \
    if (LOGGING)           \
    {                      \
      LOG1_(a "\n", b);    \
      INC;                 \
    }                      \
  }
#define DBUG_ENTER_3(a, b, c) \
  {                           \
    if (LOGGING)              \
    {                         \
      LOG2_(a "\n", b, c);    \
      INC;                    \
    }                         \
  }
#define DBUG_ENTER_4(a, b, c, d) \
  {                              \
    if (LOGGING)                 \
    {                            \
      LOG3_(a "\n", b, c, d);    \
      INC;                       \
    }                            \
  }
#define DBUG_ENTER_5(a, b, c, d, e) \
  {                                 \
    if (LOGGING)                    \
    {                               \
      LOG4_(a "\n", b, c, d, e);    \
      INC;                          \
    }                               \
  }
#define DBUG_RETURN_(a)            \
  {                                \
    if (LOGGING)                   \
    {                              \
      DEC;                         \
      LOGG_("Return(" #a ") -\n"); \
    }                              \
    return a;                      \
  }
#define DBUG_VOID_RETURN_  \
  {                        \
    if (LOGGING)           \
    {                      \
      DEC;                 \
      LOGG_("Return -\n"); \
    }                      \
    return;                \
  }

// #define FULL_LOGGING
// #define NO_LOGGING
// #define LOG_ENTER_RETURN
// #define LOG_LOGGING

#ifdef FULL_LOGGING

#define LOGG(a) LOGG_(a)
#define LOG1(a, b) LOG1_(a, b)
#define LOG2(a, b, c) LOG2_(a, b, c)
#define LOG3(a, b, c, d) LOG3_(a, b, c, d)
#define LOG4(a, b, c, d, e) LOG4_(a, b, c, d, e)
#define LOG5(a, b, c, d, e, f) LOG5_(a, b, c, d, e, f)

#define DBUG_ENTER(a) DBUG_ENTER_1(a)
#define DBUG_ENTER1(a, b) DBUG_ENTER_2(a, b)
#define DBUG_ENTER2(a, b, c) DBUG_ENTER_3(a, b, c)
#define DBUG_ENTER3(a, b, c, d) DBUG_ENTER_4(a, b, c, d)
#define DBUG_ENTER4(a, b, c, d, e) DBUG_ENTER_5(a, b, c, d, e)
#define DBUG_RETURN(a) DBUG_RETURN_(a)
#define DBUG_VOID_RETURN DBUG_VOID_RETURN_

#endif // FULL_LOGGING

#ifdef NO_LOGGING

// You *can* - for debugging the logging system itself - choose to
// honor LOGx() and/or ENTER/RETURN invocations separately. Hopefully
// we won't have any need for it again :-)

#ifndef LOG_LOGGING
#define LOGG(a) ((void)1)
#define LOG1(a, b) ((void)1)
#define LOG2(a, b, c) ((void)1)
#define LOG3(a, b, c, d) ((void)1)
#define LOG4(a, b, c, d, e) ((void)1)
#else
#define LOGG(a) LOGG_(a)
#define LOG1(a, b) LOG1_(a, b)
#define LOG2(a, b, c) LOG2_(a, b, c)
#define LOG3(a, b, c, d) LOG3_(a, b, c, d)
#define LOG4(a, b, c, d, e) LOG4_(a, b, c, d, e)
#define LOG5(a, b, c, d, e, f) LOG5_(a, b, c, d, e, f)
#endif

#ifndef LOG_ENTER_RETURN
#define DBUG_ENTER(a) ((void)1)
#define DBUG_ENTER1(a, b) ((void)1)
#define DBUG_ENTER2(a, b, c) ((void)1)
#define DBUG_ENTER3(a, b, c, d) ((void)1)
#define DBUG_ENTER4(a, b, c, d, e) ((void)1)
#define DBUG_RETURN(a) return (a)
#define DBUG_VOID_RETURN return
#else
#define DBUG_ENTER(a) DBUG_ENTER_1(a)
#define DBUG_ENTER1(a, b) DBUG_ENTER_2(a, b)
#define DBUG_ENTER2(a, b, c) DBUG_ENTER_3(a, b, c)
#define DBUG_ENTER3(a, b, c, d) DBUG_ENTER_4(a, b, c, d)
#define DBUG_ENTER4(a, b, c, d, e) DBUG_ENTER_5(a, b, c, d, e)
#define DBUG_RETURN(a) DBUG_RETURN_(a)
#define DBUG_VOID_RETURN DBUG_VOID_RETURN_
#endif

#endif // NO_LOGGING

// Unconditional messages:
#define DEBUGLOG(a)      \
  {                      \
    fprintf(logfile, a); \
  }
#define CRASHLOG(a, b)      \
  {                         \
    fprintf(logfile, a, b); \
    FLUSH;                  \
  }
#define CRASH(why)                           \
  {                                          \
    fprintf(logfile, "Crashing: %s\n", why); \
    FLUSH;                                   \
    __builtin_trap();                        \
  }

#endif
