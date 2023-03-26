/******************************************************************************
 * BITBUCKET
 *****************************************************************************/
#define MYSQL_SERVER 1
#include <sys/mman.h>
// "New" in mysql 5.5:
#include "all-mysql-includes.h"

//  END New in 5.5

#define FULL_LOGGING
#include "bucket.h"
#include "logging.h"
#include "bitbucket.h"
#include "ha_handler.h"

// Bitbuckets never have any child buckets, BTW!

// Anonymous bitbuckets:
bitbucket::bitbucket(EngineLocked engine_locked, MEI *mei)
    : Bucket("anon_bb", engine_locked, 0 /* n_bucket_children */, mei),
      buffer(0), words(0), max_value(HA_HANDLER_NOTFOUND)
{
  DBUG_ENTER("bitbucket::bitbucket(meiptr)");
  // No fn_format required - just copy! "name" is generated in Bucket
  strncpy(buffer_name, name, FN_REFLEN);
  DBUG_VOID_RETURN;
}

bitbucket::bitbucket(const char *_name, EngineLocked engine_locked,
                     const char **exts, MEI *mei)
    : Bucket(_name, engine_locked, 0 /* n_bucket_children */, mei),
      buffer(0), words(0), max_value(HA_HANDLER_NOTFOUND)
{
  IFACE_LOG_LOCAL;
  DBUG_ENTER("bitbucket::bitbucket(name,xt,meiptr)");
  fn_format(buffer_name, name, "", exts[0], MY_REPLACE_EXT | MY_UNPACK_FILENAME);
  bucket_lock();
  load();
  bucket_lock_yield();
  DBUG_VOID_RETURN;
}

ulonglong bitbucket::autoincrement_value(void) { return max_value + 1; }
ulonglong bitbucket::deleted(void) { return 0; }         // !!Calculate
ulonglong bitbucket::mean_rec_length(void) { return 1; } // !!Calculate
ulonglong bitbucket::data_file_length() { return words * sizeof(*buffer); }

bitbucket::~bitbucket(void)
{
  DBUG_ENTER("bitbucket::~bitbucket");
  if (!buffer)
    DBUG_VOID_RETURN;
  if (words)
  {
    if (!strncmp("anon_bb", name, 7))
      my_free(buffer);
    else
      munmap(buffer, words * sizeof(*buffer));
  }
  buffer = 0;
  memcount(-words * sizeof(*buffer));
  words = 0;
  DBUG_VOID_RETURN;
}

void bitbucket::load(void) // Call with table mutex locked!
{
  DBUG_ENTER("bitbucket::load");
  LOCALLOG(LOG1("bitbucket::load'ing[%s]\n", name));
  MY_STAT stat;

  dirty = 0;
  max_value = HA_HANDLER_NOTFOUND;
  words = 0;
  LOGG("Crash SET\n");
  hibernated = crashed = 1; // Updated later, we're bucket_lock()ed
  Nrecords = 0;

  stat.st_size = 0;
  buffer = (ulonglong *)resize_file_map(buffer_name, buffer, 0, &stat);
  if (buffer == MAP_FAILED)
    DBUG_VOID_RETURN;

  if (stat.st_size % (BMP_CHUNK * sizeof(*buffer)))
  {
    LOCALLOG(LOG1("%s::load size check failed - crash\n", name));
    my_error(HA_ERR_CRASHED_ON_USAGE, MYF(0), name);
    DBUG_VOID_RETURN;
  }

  words = stat.st_size / sizeof(*buffer); // Words-in-buffer

  ulonglong nzword; // Find last nonzero word
  for (nzword = words - 1; nzword != HA_HANDLER_NOTFOUND && !buffer[nzword]; nzword--)
  {
  };

  if (nzword != HA_HANDLER_NOTFOUND)
  {
    int bit = BMP_MASK;
    while (!(buffer[nzword] & (1ULL << bit)))
      bit--;
    max_value = (nzword << BMP_SHIFT) + bit;
  }
  while (nzword != HA_HANDLER_NOTFOUND)
  {
    ulonglong bword = buffer[nzword];
    while (bword)
    {
      Nrecords += bword & 1;
      bword >>= 1;
    }
    nzword--;
  }

  ctime = stat.st_ctime;
  ltime = time(NULL);
  mtime = stat.st_mtime;

  LOG4("%s load: # %llu, Max: %llu, W: %lld\n", name, Nrecords, max_value, words);
  LOGG("Crash UNset\n");
  hibernated = crashed = 0; // We're bucket_lock()ed
  DBUG_VOID_RETURN;
}

void bitbucket::save(void) // Call with table mutex locked!
{
  DBUG_ENTER("bitbucket::save");
  LOGG("Not using bitbucket::save when mmap'ing!\n");
  dirty = 0;
  DBUG_VOID_RETURN;
}

void bitbucket::truncate()
{
  DBUG_ENTER("bitbucket::truncate()");
  if (Nrecords > 0)
  {
    Nrecords = 0;
    reallocate(max_value = HA_HANDLER_NOTFOUND);
  }
  dirty = 0;
  DBUG_VOID_RETURN;
}

int bitbucket::isset(ulonglong val)
{
  IFACE_NO_LOG_LOCAL;
  DBUG_ENTER4("bitbucket::isset(val=%llu) Nrecords=%llu max_value=%llu [%s]",
              val, Nrecords, max_value, name);

  if (Nrecords == 0 || val > max_value)
    DBUG_RETURN(0);

  ulonglong word = val >> BMP_SHIFT;

  if (!buffer[word])
    DBUG_RETURN(0);

  int bit = val & BMP_MASK;
  // LOG3("  Word %lld, bit %d, wordval %llx\n",word,bit,buffer[word]);

  // Splitting up like this => better debug output
  if ((buffer[word] >> bit) & 1)
    DBUG_RETURN(1);
  DBUG_RETURN(0);
}

int bitbucket::isset(ulonglong word, int bit)
{
  IFACE_NO_LOG_LOCAL;
  DBUG_ENTER4("bitbucket::isset(word %llu,bit %d) wordval=%llx [%s]",
              word, bit, buffer[word], name);

  // Splitting up like this => better debug output
  if ((buffer[word] >> bit) & 1)
    DBUG_RETURN(1);
  DBUG_RETURN(0);
}

ulonglong bitbucket::next(ulonglong current)
{
  DBUG_ENTER3("bitbucket::next(current=%llu) Nrecords=%llu [%s]", current, Nrecords, name);
  ulonglong word;
  int bit;

  if (Nrecords == 0)
    DBUG_RETURN(HA_HANDLER_NOTFOUND); // Doh!

  while (++current <= max_value)
  { // Step through values...
    word = current >> BMP_SHIFT;
    bit = current & BMP_MASK;
    // LOG4("Current=%llu, word=%llu, bit=%d, words=%llu\n",
    //      current,word,bit,words);
    //  If bit=0, it's the first bit of the next word, so we can
    //  jump one all-zero words at a time in our hunt.

    if (!bit)
    {
      // Skip all zero words
      while (word < words && !buffer[word])
        ++word;
      if (word >= words)
        DBUG_RETURN(HA_HANDLER_NOTFOUND);
      // bit is still zero if we'v been skipping to first nonzero word
      current = word << BMP_SHIFT;
    }

    if ((buffer[word] >> bit) & 1)
    {
      LOG1("Returning current=%llu\n", current);
      DBUG_RETURN(current);
    }
  }

  DBUG_RETURN(HA_HANDLER_NOTFOUND);
}

ulonglong bitbucket::prev(ulonglong current)
{
  DBUG_ENTER("bitbucket::prev");
  ulonglong word;
  int bit;

  while (--current != HA_HANDLER_NOTFOUND)
  {
    word = current >> BMP_SHIFT;
    bit = current & BMP_MASK;

    if (bit == BMP_MASK)
    { // All bits set -> Word boundary [highest bit]
      // Skip all zero words
      while (word != HA_HANDLER_NOTFOUND && !buffer[word])
        --word;
      if (word == HA_HANDLER_NOTFOUND)
        DBUG_RETURN(HA_HANDLER_NOTFOUND);
      // bit still equal to BMP_MASK
      current = word << BMP_SHIFT | bit;
    }
    if (isset(word, bit))
      DBUG_RETURN(current);
  };
  DBUG_RETURN(HA_HANDLER_NOTFOUND);
}

void bitbucket::reallocate(ulonglong new_max_val)
{
  DBUG_ENTER("bitbucket::reallocate");

  ulonglong new_words = ((new_max_val >> BMP_SHIFT) / BMP_CHUNK + 1) * BMP_CHUNK;

  if (new_max_val == HA_HANDLER_NOTFOUND)
  {
    if (Nrecords > 0)
    {
      LOGG("Crash SET\n");
      crashed = 1; // For real, ignore bucket_lock()
      LOCALLOG(LOG1("%s::reallocate max_value incorrect, crash\n", name));
      my_error(HA_ERR_CRASHED_ON_USAGE, MYF(0), "new max_value incorrect");
      DBUG_VOID_RETURN;
    }
    new_words = 0;
  }
  LOG2("Trying to reallocate %lld to %lld bytes\n", words * sizeof(*buffer), new_words * sizeof(*buffer));

  buffer = (ulonglong *)resize_file_map_ro(buffer_name, buffer,
                                           words * sizeof(*buffer),
                                           new_words * sizeof(*buffer));
  if (buffer == MAP_FAILED)
    new_words = 0;

  words = new_words;

  DBUG_VOID_RETURN;
}

void bitbucket::set(ulonglong val)
{
  IFACE_NO_LOG_LOCAL;
  DBUG_ENTER2("bitbucket::set(%llu) [%s]", val, name);
  ulonglong word = val >> BMP_SHIFT;
  int bit = val & BMP_MASK;

  if (word >= words)
    reallocate(val);

  if (!isset(val))
  {
    buffer[word] |= ((ulonglong)1) << bit;
    Nrecords++;
    dirty = 1;
    if (max_value == HA_HANDLER_NOTFOUND || val > max_value)
      max_value = val;
  }

  DBUG_VOID_RETURN;
}

void bitbucket::unset(ulonglong val)
{
  DBUG_ENTER("bitbucket::unset");
  LOG1("bitbucket::unset(%lld)\n", val);
  ulonglong word = val >> BMP_SHIFT;
  int bit = val & BMP_MASK;
  if ((!buffer[word]) & (1ULL << bit))
    DBUG_VOID_RETURN;

  buffer[word] &= ~(1ULL << bit);
  Nrecords--;
  dirty = 1;

  LOG3("UNset:word %lld, bit %d, wordval %llx\n", word, bit, buffer[word]);

  if (val != max_value)
    DBUG_VOID_RETURN;

  // Should we free up some memory??
  reallocate(max_value = prev(val));
  DBUG_VOID_RETURN;
}

// If an isset, prev, next, set, unset, etc is called, we need to
// ensure we're not in hibernation right now! Can we trust that we'll
// always get an external_lock at some point before these? I think so.

// Hibernate will only be called for tables that are not elocked, AND the
// table's mutex will be locked by the caller to ensure it's not
// being elocked or otherwise manipulated while we're at work.

void bitbucket::hibernate(void)
{
  DBUG_ENTER("bitbucket::hibernate");
  if (hibernated)
    DBUG_VOID_RETURN; // Just in case
  save();             // Just in case
  LOG2("%s hibernating, freeing %lld?\n", name, words * sizeof(*buffer));
  if (buffer)
  {
    munmap(buffer, words * sizeof(*buffer));
    buffer = 0;
    memcount_locked(-words * sizeof(*buffer));
    words = 0;
  }
  hibernated = 1;
  DBUG_VOID_RETURN;
}

void bitbucket::wakeup(void)
{
  DBUG_ENTER("bitbucket::wakeup");
  if (hibernated)
  {
    load();
    hibernated = 0;
    reg_wakeup(); // update statistics
  }
  DBUG_VOID_RETURN;
}

void bitbucket::Union(bitbucket **s, const int n)
{
  DBUG_ENTER("bitbucket::Union");
  ulonglong word;
  int i;

  // Find max value
  max_value = HA_HANDLER_NOTFOUND;
  for (i = 0; i < n; i++)
    if (s[i]->max_value > max_value)
      max_value = s[i]->max_value;

  if (max_value == HA_HANDLER_NOTFOUND)
  {
    LOGG("EMPTY RESULT - no change\n");
    DBUG_VOID_RETURN;
  }

  reallocate(max_value);
  ulonglong bword;

  for (word = 0; word < words; word++)
  {
    bword = s[0]->buffer[word];
    for (i = 1; i < n; i++)
      bword |= s[i]->buffer[word];
    buffer[word] = bword;
    while (bword)
    {
      Nrecords += bword & 1;
      bword >>= 1;
    }
  }

  dirty = 1;
  save();
  DBUG_VOID_RETURN;
}

void bitbucket::Join(bitbucket **s, const int n)
{
  DBUG_ENTER("bitbucket::Join");
  int i;

  // Find lowest(highest nonzero source word)
  ulonglong nzword = s[0]->words;
  for (i = 1; i < n; i++)
    if (s[i]->words < nzword)
      nzword = s[i]->words;

  // Got a candidate word... start calculating result words working
  // backwards from nzword until we find a nonzero result.

  ulonglong bword = 0;
  while (!bword && --nzword != HA_HANDLER_NOTFOUND)
  {
    bword = s[0]->buffer[nzword];
    for (i = 1; i < n; i++)
      bword &= s[i]->buffer[nzword];
  }

  if (nzword == HA_HANDLER_NOTFOUND)
  {
    LOGG("EMPTY RESULT - no change\n");
    DBUG_VOID_RETURN;
  }

  int bit = BMP_MASK;
  while (!(bword & (1ULL << bit)))
    bit--;
  max_value = (nzword << BMP_SHIFT) + bit;
  reallocate(max_value);

  while (nzword != HA_HANDLER_NOTFOUND)
  {
    bword = s[0]->buffer[nzword];
    for (i = 1; i < n; i++)
      bword &= s[i]->buffer[nzword];
    buffer[nzword] = bword;
    while (bword)
    {
      Nrecords += bword & 1;
      bword >>= 1;
    }
    nzword--;
  }
  dirty = 1;
  save();
  DBUG_VOID_RETURN;
}
