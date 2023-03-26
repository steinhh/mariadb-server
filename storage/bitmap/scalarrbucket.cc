/******************************************************************************
 * SCALARRAY
 ******************************************************************************/

#include "all-mysql-includes.h"

#define FULL_LOGGING
#include "bucket.h"
#include "logging.h"
#include "bitbucket.h"
#include "scalarrbucket.h"
#include "ha_handler.h"

#define MAX(a, b) ((a > b) ? (a) : (b))

#define SCALARRBUCKET(t)        \
  template <class IT, class VT> \
  t scalarrbucket<IT, VT>::

SCALARRBUCKET()
scalarrbucket(const char *_name, EngineLocked engine_locked, int _typecode,
              int _singlecol, const char **exts, MEI *mei)

    : Bucket(_name, engine_locked, 2 /*n_bucket_children*/, mei),

      typecode(_typecode), singlecol(_singlecol), have_deleted(0),
      have_inserted(0), index(0), iwords(0), buffer(0), bwords(0)
{
  IFACE_LOG_LOCAL;
  DBUG_ENTER("scalarrbucket::scalarrbucket");
  LOG1("singlecol=%d\n", singlecol);
  bits = new bitbucket(name, engine_locked, exts, mei);
  inserted = new bitbucket(engine_locked, mei);

  // Report both Bucket children to Bucket, so lock changes propagate
  bucket_children[0] = bits;
  bucket_children[1] = inserted;

  fn_format(buffer_name, name, "", exts[1], MY_REPLACE_EXT | MY_UNPACK_FILENAME);
  LOG1("Formatted buffer_name %s\n", buffer_name);

  fn_format(index_name, name, "", exts[2], MY_REPLACE_EXT | MY_UNPACK_FILENAME);
  LOG1("Formatted index_name %s\n", index_name);

  if (is_crashed())
    LOCALLOG(LOG1("%s::scalarrbucket crash!\n", name));
  // No load needed - done by bitbucket/scalarrbucket
  DBUG_VOID_RETURN;
}

SCALARRBUCKET(ulonglong)
autoincrement_value(void) { return bits->autoincrement_value(); }

SCALARRBUCKET(ulonglong)
deleted(void) { return bits->deleted(); }

SCALARRBUCKET(ulonglong)
mean_rec_length(void) { return bits->mean_rec_length(); }

SCALARRBUCKET(ulonglong)
data_file_length(void) { return bits->data_file_length(); }

SCALARRBUCKET(ulonglong)
records(void) { return bits->records(); }

SCALARRBUCKET()
~scalarrbucket(void)
{
  DBUG_ENTER("scalarrbucket::~scalarrbucket");
  // We don't need/want propagation of engine lock status *now*
  bucket_children[0] = bucket_children[1] = NULL;
  delete bits;
  delete inserted;

  if (!buffer)
    DBUG_VOID_RETURN;
  munmap(buffer, bwords * sizeof(*buffer));
  buffer = 0;
  memcount(-bwords * sizeof(*buffer));
  munmap(index, iwords * sizeof(*index));
  index = 0;
  memcount(-iwords * sizeof(*index));
  iwords = bwords = 0;
  DBUG_VOID_RETURN;
}

SCALARRBUCKET(int)
is_crashed(void)
{
  DBUG_ENTER("scalarrbucket::is_crashed");
  bucket_lock();
  int crash = crashed = crashed | bits->is_crashed();
  bucket_lock_yield();
  DBUG_RETURN(crash);
}

SCALARRBUCKET(void)
bag_load(void) // Used to be in scalarbag.cc
{
  MY_STAT stat;
  DBUG_ENTER("SCALARRBUCKET::bag_load");

  dirty = 0;
  bwords = iwords = 0;
  LOGG("Crash SET\n");
  hibernated = crashed = 1; // Updated later, bucket_lock()ed
  Nrecords = 0;

  stat.st_size = 0;
  buffer = (VT *)resize_file_map(buffer_name, buffer, 0, &stat);
  LOGG("resize_file_map(?) returned\n");
  if (buffer == MAP_FAILED)
    DBUG_VOID_RETURN;

  if (stat.st_size % (SAR_CHUNK * sizeof(*buffer)))
  { // Size check
    LOCALLOG(LOG1("%s size check failed for buffer - crash\n", name));
    my_error(HA_ERR_CRASHED_ON_USAGE, MYF(0), name);
    DBUG_VOID_RETURN;
  }

  bwords = stat.st_size / sizeof(*buffer); // Words-in-buffer
  LOG1("loaded buffer %llu words\n", (ulonglong)bwords);
  // Now for the index:
  if (singlecol)
  {
    LOGG("Skipping index (singlecol)\n");
    goto SKIP_LOAD_INDEX;
  }

  stat.st_size = 0;
  index = (IT *)resize_file_map(index_name, index, 0, &stat);
  if (stat.st_size % (SAR_CHUNK * sizeof(*index)))
  { // Size check
    LOCALLOG(LOG1("size check failed for index - crash(%s)\n", name));
    my_error(HA_ERR_CRASHED_ON_USAGE, MYF(0), name);
    DBUG_VOID_RETURN;
  }

  iwords = stat.st_size / sizeof(*index); // Words-in-index
  LOG1("loaded index %llu words\n", (ulonglong)iwords);

SKIP_LOAD_INDEX:
  // All seems to be ok - record some vital stats
  Nrecords = bits->records();
  if (!singlecol && Nrecords > 0)
  {
    LOG1("Nrecords=%lld\n", Nrecords);
    min_value = buffer[index[0]];
    max_value = buffer[index[Nrecords - 1]];
    // check_and_repair();
  }

  ctime = stat.st_ctime;
  ltime = time(NULL);
  mtime = stat.st_mtime;

  LOG4("%s load: # %llu, Max: %.8g, W: %llu\n", name, Nrecords, (double)max_value, (ulonglong)bwords);
  LOGG("Crash UNset\n");
  hibernated = crashed = 0;
  have_deleted = have_inserted = 0;
  DBUG_VOID_RETURN;
}

SCALARRBUCKET(void)
load(void) // Call with table mutex locked!
{
  DBUG_ENTER("scalarrbucket::load");
  LOGG("Crash SET\n");
  hibernated = crashed = 1; // Updated by bag_load(), we're bucket_lock()ed

  bits->wakeup(); // May be entirely unnecessary
  if (!bits->is_crashed())
    bag_load();

  // bits/bats should have issued their own my_errors:
  if (crashed)
  { // Do NOT use is_crashed, bucket/bucket mutex is locked
    LOCALLOG(LOG1("%s::load crash detected\n", name));
    goto error;
  }

  ctime = MAX(bits->ctime, ctime);
  ltime = time(NULL);
  mtime = MAX(bits->mtime, mtime);

error:
  DBUG_VOID_RETURN;
}

SCALARRBUCKET(void)
save(void) // Call with table mutex locked!
{
  DBUG_ENTER("scalarrbucket::save");
  bits->save();
  if (!hibernated)
  {
    if (!dirty)
    {
      LOGG("Not dirty -> no save action\n");
      DBUG_VOID_RETURN;
    }
    if (singlecol)
    {
      LOGG("No pre_save() b/c singlecol\n");
    } //;
    else
      pre_save();
    dirty = 0;
  }
  mtime = time(NULL);
  DBUG_VOID_RETURN;
}

// If an isset, prev, next, set, unset, etc is called, we need to
// ensure we're not in hibernation right now! Can we trust that we'll
// always get an external_lock at some point before these? I think so.

// Hibernate will only be called for tables that are not elocked, AND the
// table's mutex will be locked by the caller to ensure it's not
// being elocked or otherwise manipulated while we're at work.

SCALARRBUCKET(void)
bag_hibernate(void)
{
  DBUG_ENTER("SCALARRBUCKET::hibernate");
  if (hibernated)
    DBUG_VOID_RETURN; // Just in case
  mtime = 0;          // Saving without this is futile!
  save();             // Just in case
  LOG2("%s hibernating, freeing %lld?\n", name, (ulonglong)bwords);
  LOG2("%s hibernating, freeing %lld?\n", name, (ulonglong)iwords);
  if (buffer)
  {
    // Can't use resize_file_map() b/c we need memcount_locked!
    munmap(buffer, bwords * sizeof(*buffer));
    memcount_locked(-bwords * sizeof(*buffer));
    buffer = 0;
  }
  if (index)
  {
    // Can't use resize_file_map() b/c we need memcount_locked!
    munmap(index, iwords * sizeof(*index));
    memcount_locked(-iwords * sizeof(*index));
    index = 0;
  }
  bwords = iwords = 0;
  hibernated = 1;
  DBUG_VOID_RETURN;
}

SCALARRBUCKET(void)
hibernate(void)
{
  DBUG_ENTER("scalarrbucket::hibernate");
  if (hibernated)
    DBUG_VOID_RETURN; // Just in case
  bits->hibernate();  // Each of these save if necessary
  bag_hibernate();
  hibernated = 1;
  DBUG_VOID_RETURN;
}

SCALARRBUCKET(void)
wakeup(void)
{
  DBUG_ENTER("scalarrbucket::wakeup");
  bits->wakeup(); // LOOK: is this entirely unnecessary?
  if (hibernated)
  {
    load();
    hibernated = crashed = bits->is_crashed(); // For real, ignore bucket_lock()
    if (crashed)
    {
      LOCALLOG(LOG1("%s::wakeup crashed\n", name));
    }
    reg_wakeup(); // update statistics
  }
  else
  {
    LOGG("My status variable say I'm awake\n");
  }
  DBUG_VOID_RETURN;
}

SCALARRBUCKET(void)
bmp_filter(scalarrbucket<IT, VT> *src, bitbucket *filt)
{
  DBUG_ENTER("scalarrbucket::bmp_filter");
  if (hibernated)
  {
    bucket_lock();
    wakeup();
    bucket_lock_yield();
  }
  bag_bmp_filter(src, filt);
  DBUG_VOID_RETURN;
}

SCALARRBUCKET(void)
bmp_sar_in_match(bitbucket *dest, int n, VT vals[])
{
  DBUG_ENTER("scalarrbucket::bmp_sar_in_match");
  if (hibernated)
  {
    bucket_lock();
    wakeup();
    bucket_lock_yield();
  }
  bag_bmp_sar_in_match(dest, n, vals);
  DBUG_VOID_RETURN;
}

SCALARRBUCKET(int)
repair(void)
{
  DBUG_ENTER("SCALARRBUCKET::repair");
  if (singlecol)
  {
    LOGG("No repair of index for single-column table!\n");
    DBUG_RETURN(0);
  }
  ireallocate(bits->Nrecords);
  breallocate(bits->max_value);
  // Strategy: bits defines which records are set, resort entire index
  ulonglong curpos = HA_HANDLER_NOTFOUND;
  ulonglong curinx = 0;
  while ((curpos = bits->next(curpos)) <= bits->max_value)
  {
    index[curinx++] = curpos;
  }
  LOG2("curinx=%lld, Nrecords=%llu\n", curinx, Nrecords);
  have_inserted = curinx;
  Nrecords = 0;
  have_deleted = 0;
  dirty = 1;
  pre_save();
  save();
  DBUG_RETURN(0);
}

SCALARRBUCKET(int)
check(void)
{
  DBUG_ENTER("SCALARRBUCKET::check");
  int need_repair = 0;
  VT lastval = min_value; // VALUE ~= VT
  for (unsigned int i = 0; i < Nrecords && !need_repair; i++)
  {
    if (!bits->isset(index[i]))
      need_repair = 1;
    if (index[i] >= bwords)
      need_repair = 1;
    if (need_repair)
      goto skip;
    if (buffer[index[i]] < lastval)
      need_repair = 1;
    lastval = buffer[index[i]];
  }
skip:

  if (need_repair)
    DBUG_RETURN(HA_ADMIN_CORRUPT);
  DBUG_RETURN(HA_ADMIN_OK);
}

SCALARRBUCKET(int)
index_size(int actinx)
{
  if (actinx == 0)
    return sizeof(*index);
  if (actinx == 1)
    return sizeof(*buffer);
  return 0;
}

// INSERT/UPDATE/DELETE strategies:

// Delete: Unset bits entry, keep track of number in "have_deleted";
// don't update "Nrecords".

// Insert: keep track of number in "have_inserted" while adding index
// entries at end [Nrecords+have_inserted]. Set entry in the
// "inserted" bitbucket - allows any original index entry to be
// purged; don't update Nrecords, and DON'T set bits entry

// Update's: Delete + Insert. I.e. unset primary entry, flag as
// inserted, add new index entry

// At end:

// 1. SORT NEW ENTRIES first - index[Nrecords..Nrecords+have_inserted-1].

// 2. PURGE INDEX by traversing index, copying only those entries that
// have primary set *and* are *not* flagged as newly inserted. Old
// "Nrecords" value is kept, so we can then merge in the new entries.

// 3. MERGE: Merge-sort, using temp buffer:
//    index[0..Nrecords] with index[oldNrecords..oldNrecords+have_inserted-1]

// sort always works on index[Nrecords..Nrecords+have_inserted]
SCALARRBUCKET(void)
sort(void)
{
  DBUG_ENTER("SCALARRBUCKET::sort");
  if (have_inserted > 1)
  {
    starttimer();
    QSORT(index + Nrecords, have_inserted);
    LOG2("sorting %llu took %6.5f\n", have_inserted, interval());
  }
  DBUG_VOID_RETURN;
}

// PURGING index[0 .. records-1] using bits->isset
// NOTE: Do *not* copy index entries for *INSERTED* records!
SCALARRBUCKET(void)
purge(void)
{
  DBUG_ENTER2("SCALARRBUCKET::purge; Nrecords=%llu have_deleted=%llu",
              Nrecords, have_deleted);
  if (!have_deleted)
    DBUG_VOID_RETURN;

  starttimer();

  ulonglong wref = 0, rref = 0; // Write ref & Read ref
  while (rref < Nrecords)
  {
    longlong pos = index[rref];
    if (bits->isset(pos) && !inserted->isset(pos))
      index[wref++] = index[rref];
    rref++;
  }

  Nrecords = wref; // wref was ++'ed after last write, so no +1
  have_deleted = 0;

  LOG2("purge:Nrecords=%llu, t=%6.3f\n", Nrecords, interval());
  DBUG_VOID_RETURN;
}

// Not IT: HA_HANDLER_NOTFOUND
SCALARRBUCKET(void)
merge(ulonglong PrePurgeRecords)
{
  IFACE_LOG_LOCAL;
  DBUG_ENTER("SCALARRBUCKET::merge");

  // Deal with trivial cases first:

  if (!have_inserted)
  { // Shouldn't have been here, but what the heck
    LOGG("Trivial: have_inserted = 0\n");
    DBUG_VOID_RETURN;
  }

  if (!Nrecords)
  {
    LOGG("Trivial: no old records to merge with!\n");
    Nrecords = have_inserted;
    inserted->truncate();
    have_inserted = 0;
    DBUG_VOID_RETURN;
  }

  LOG3("Nontrivial: Nrecords=%llu, PrePurgeRecords=%llu, inserted=%llu\n",
       Nrecords, PrePurgeRecords, have_inserted);

  IT *nuix; // temp memory buffer for result
  //! void *my_realloc(PSI_memory_key key, void *ptr, size_t size, myf MyFlags)
  nuix = (IT *)my_realloc(0, 0, (Nrecords + have_inserted) * sizeof(*index), MYF(MY_ALLOW_ZERO_PTR));
  if (!nuix)
  {
    LOCALLOG(LOGG("out of memory?\n"));
    my_error(HA_ERR_OUT_OF_MEM, MYF(0), "out of memory!");
    LOGG("Crash SET\n");
    crashed = 1; // For real, ignore bucket_lock()
    DBUG_VOID_RETURN;
  }

  ulonglong dest = 0,         // Destination index
      osrc = 0,               // Old records index
      nsrc = PrePurgeRecords; // New records index

  // While we have both old and new left, compare and pick

  while (osrc < Nrecords && nsrc < PrePurgeRecords + have_inserted)
  {
    if (buffer[index[osrc]] <= buffer[index[nsrc]])
    {
      nuix[dest++] = index[osrc++]; // old <= new; copy old
    }
    else
    {
      nuix[dest++] = index[nsrc]; // new < old; copy new
      bits->set(index[nsrc++]);   // And SET! See insert expl. above
    }
  }

  // Copy rest of either list (only one will have elements left);
  while (osrc < Nrecords)
  {
    nuix[dest++] = index[osrc++];
  }

  while (nsrc < PrePurgeRecords + have_inserted)
  {
    bits->set(nuix[dest++] = index[nsrc++]); // Includes bits->set and copy
  }

  Nrecords = Nrecords + have_inserted; // Ok, we can finally update this

  // Copy temp memory buffer into mem-mapped index
  memcpy(index, nuix, Nrecords * sizeof(*index));
  my_free(nuix);

  have_inserted = 0;
  inserted->truncate();

  DBUG_ASSERT(Nrecords == dest);
  DBUG_VOID_RETURN;
}

SCALARRBUCKET(IT)
BSEARCH2(VT key, int sign_when_equal)
{
  ulonglong nmemb = Nrecords;
  const IT *base = index;
  const IT *mid_point = 0; // Init to avoid compiler warning: Nrecords>0
  int cmp;                 // Cannot be VT - must *not* be unsigned!!

  while (nmemb > 0)
  {
    mid_point = base + (nmemb >> 1);
    if (key == buffer[*mid_point])
      cmp = sign_when_equal;
    else if (key > buffer[*mid_point])
      cmp = 1;
    else
      cmp = -1;

    if (cmp > 0)
    {
      // LOG2("Up: %.8g > %.8g\n",(double)key,(double)buffer[*mid_point]);
      base = mid_point + 1;
      nmemb = (nmemb - 1) >> 1;
    }
    else
    {
      // LOG2("Dn: %.8g < %.8g\n",(double)key,(double)buffer[*mid_point]);
      nmemb >>= 1;
    }
  }
  return mid_point - index;
}

// Isset_first/_last behave like overshooting brute-force searches
// starting from the beginning and end of the index, respectively,
// but will not overshoot the index space.

// Return *ixix pointing to first entry equal to val, or to first
// [smallest] entry larger than val. If no such entry exists, return
// HA_HANDLER_NOTFOUND

SCALARRBUCKET(int)
isset_first(VT val_in, ulonglong *ixix)
{
  VT val = (VT)val_in;
  DBUG_ENTER("SCALARRBUCKET::isset_first");
  LOG2("Searching for %.12g among %llu entries\n", (double)val_in, Nrecords);

  if (!Nrecords)
  {
    *ixix = HA_HANDLER_NOTFOUND;
  }
  if (val < (VT)min_value)
  {
    *ixix = 0; // Not found, but point to first that's larger
    DBUG_RETURN(0);
  }

  LOGG("Hey1\n");

  // If val is larger than max_val, there's no entry larger than val!
  if (val > (VT)max_value)
  {
    *ixix = HA_HANDLER_NOTFOUND; // EOF!
    DBUG_RETURN(0);
  }

  LOGG("Hey2 - BSEARCH'ing\n");

  *ixix = BSEARCH2(val, -1); // Regard key as smaller when eq, go down

  LOGG("Hey3 - BSEARCH'ed\n");

  while (*ixix < Nrecords && buffer[index[*ixix]] < val)
    ++*ixix;

  LOGG("Hey4 - Skipped ahead\n");

  if (*ixix == Nrecords)
  {
    LOGG("Ouch - this should never happen!\n");
    CRASH("isset_first() *ixix == Nrecords shouldn't happen");
  }

  LOGG("Almost done now!\n");

  LOG1("index[*ixix]=%llu\n", (ulonglong)index[*ixix]);

  if (buffer[index[*ixix]] == val)
  { // Yess. Found it.
    LOG1("isset *ixix=%lld\n", *ixix);
    DBUG_RETURN(1);
  }

  LOGG("Really, REALLY close\n");

  LOG1("!sset *ixix=%lld\n", *ixix);
  DBUG_RETURN(0);
}

// Return *ixix pointing to last entry equal to val, or to first
// [largest] entry smaller than val. If no such entry exists, return
// HA_HANDLER_NOTFOUND

SCALARRBUCKET(int)
isset_last(VT val_in, ulonglong *ixix)
{
  VT val = (VT)val_in;
  DBUG_ENTER("SCALARRBUCKET::isset_last");

  if (val > (VT)max_value)
  {
    *ixix = Nrecords - 1;
    DBUG_RETURN(0);
  }

  // val smaller than min_value means no entry smaller than val!
  if (val < (VT)min_value)
  {
    *ixix = HA_HANDLER_NOTFOUND;
    DBUG_RETURN(0);
  }

  *ixix = BSEARCH2(val, 1); // Regard key as larger when eq, go up
  while (*ixix != HA_HANDLER_NOTFOUND && buffer[index[*ixix]] > val)
    --*ixix;

  if (*ixix == HA_HANDLER_NOTFOUND)
  {
    LOGG("Ouch, this shouldn't happen!\n");
    CRASH("isset_last() *ixix == HA_HANDLER_NOTFOUND shouldn't happen");
  }

  if (buffer[index[*ixix]] == val)
  {
    LOG1("isset *ixix=%lld\n", *ixix);
    DBUG_RETURN(1);
  }

  LOG1("!sset_last  *ixix=%lld\n", *ixix);
  DBUG_RETURN(0);
}

SCALARRBUCKET(void)
set(IT val1, VT val2_in)
{
  VT val2 = (VT)val2_in;

  DBUG_ENTER("scalarrbucket::set");
  LOG2("SCALARRBUCKET::set(%lld,%.10g)\n", (longlong)val1, (double)val2);

  if (val1 >= bwords)
  {
    LOGG("breallocating\n");
    breallocate(val1);
  }
  LOGG("Past breallocate\n");

  if (!singlecol)
  {
    if (Nrecords + have_inserted + 1 >= iwords)
    {
      LOGG("ireallocating\n");
      ireallocate(Nrecords + have_inserted + 1);
    }
  }
  else
  {
    LOG1("Dropping index reallocation, singlecol=%d!\n", singlecol);
  }
  LOGG("Past ireallocate\n");

  // No isset/dupkey-check b/c handler will have done it through bits

  // Between here-----------------------------------
  bits->isset(val1);
  LOG3("scalarr buffer=%llx; bits bucket=%llx, bits word=%llx\n",
       (ulonglong)buffer, (ulonglong)bits->buffer, *bits->buffer);
  buffer[val1] = val2;
  LOG3("scalarr buffer=%llx; bits bucket=%llx, bits word=%llx\n",
       (ulonglong)buffer, (ulonglong)bits->buffer, *bits->buffer);
  LOGG("Value stored in buffer\n");
  // And here:
  bits->isset(val1);

  // But check for double inserts etc.!
  if (!singlecol)
  {
    if (inserted->isset(val1))
    {
      LOGG("Double insert!\n");
    }
    else
    {
      LOGG("Registering insert\n");
      inserted->set(val1);
      LOGG("inserted->set done\n");
      index[Nrecords + have_inserted++] = val1; // We sort it later!
      LOGG("index entry added\n");
    }
  }
  dirty = 1;
  DBUG_VOID_RETURN;
}

SCALARRBUCKET(void)
unset(IT pos)
{
  DBUG_ENTER("SCALARRBUCKET::unset");
  LOG1("pos: %llu\n", (ulonglong)pos);
  dirty = ++have_deleted;
  DBUG_VOID_RETURN;
}

SCALARRBUCKET(int)
check_and_repair(void)
{
  DBUG_ENTER("SCALARRBUCKET::check_and_repair");
  if (singlecol)
  {
    LOGG("No check_and_repair for single-column table\n");
    DBUG_RETURN(TRUE);
  }
  // Sanity check - do repair if things look bad!
  // But don't clutter logs
  int checkval = check();
  if (checkval != HA_ADMIN_OK)
  {
    LOG3("Need to repair:[%llu] %llu > %lld\n", Nrecords - 1,
         (ulonglong)index[Nrecords - 1], bits->max_value);
    repair();
  }
  max_value = buffer[index[Nrecords - 1]];
  min_value = buffer[index[0]];
  LOG2("min: %.8g, max: %.8g\n", (double)min_value, (double)max_value);
  DBUG_RETURN(TRUE);
}

SCALARRBUCKET(void)
pre_save(void)
{
  IFACE_LOG_LOCAL;
  DBUG_ENTER("SCALARRBUCKET::pre_save");
  if (have_inserted)
  {
    sort();
    LOGG("sorted\n");
  }

  longlong PrePurgeRecords = Nrecords;

  if (have_deleted)
  {
    LOGG("purging\n");
    purge();
    LOGG("purged\n");
  }

  if (have_inserted)
  {
    LOGG("merging\n");
    merge(PrePurgeRecords);
    LOGG("merged\n");
  }

  // Min/max values not defined if no records. BEWARE

  if (Nrecords)
  {
    min_value = buffer[index[0]];
    max_value = buffer[index[Nrecords - 1]];
  }
  DBUG_VOID_RETURN;
}

// SORTING: QSORT

/*
 * (c) copyright 1987 by the Vrije Universiteit, Amsterdam, The Netherlands.
 * See the copyright notice in the ACK home directory, in the file "Copyright".
 */

// Ideas for improvement
// http://eternallyconfuzzled.com/tuts/algorithms/jsw_tut_sorting.aspx
// Also:

// Radix sort first
// Scan through data once: Assume lowest N(20?) bits are the "radix bits"
// Keep counts (and max/min) in array for each bucket
//
// See picture on criterion - picasa-label "sorting algorithm parallel
// quicksort radix find highest *variable* bit" - IMG_1272.JPG
//
// Detection of "static" bits... e.g. data set w/1111000 - 1111111, first 4
// bits are static: If a reasonable re-radixing is possible/likely/definite,
// then do it
//
// If bit N+m is found set - collapse counts down to size given radix
// bits shifted accordingly
//
//       All data belonging in bucket N+1 is greater than data in bucket N
//
//             This also allows preallocation of space for each bucket!
//             And allows independent insertion of sorted results!
//  2nd scan: stuff buckets - when buckets are full, insert into queue
//            for Qsort worker threads
//
//       Buckets that are too large [comp. with average] get split up for
//         new radix sort [ignoring the N upper bits]
//       [This also goes for data sets w/*NO* variation in upper N bits]
//
//
SCALARRBUCKET(void)
QSORT(IT *base, size_t nel)
{
  DBUG_ENTER("SCALARRBUCKET::QSORT");
  /* when nel is 0, the expression '(nel - 1) * width' is wrong */
  if (!nel)
    return;
  qsort1(base, base + (nel - 1));
  DBUG_VOID_RETURN;
}

#define COMPAR(a, b)                         \
  (buffer[a] == buffer[b] ? (a > b ? 1 : -1) \
                          : (buffer[a] > buffer[b] ? 1 : -1))
SCALARRBUCKET(void)
qsort1(IT *a1, IT *a2)
{
  IT *left, *right;
  IT *lefteq, *righteq;
  longlong cmp;
  const int width = 1;
  DBUG_ENTER("qsort1");
  for (;;)
  {
    if (a2 <= a1)
      DBUG_VOID_RETURN;
    left = a1;
    right = a2;
    lefteq = righteq = a1 + (((a2 - a1) + 1) / 2);
    /*
      Pick an element in the middle of the array.
      We will collect the equals around it.
      "lefteq" and "righteq" indicate the left and right
      bounds of the equals respectively.
      Smaller elements end up left of it, larger elements end
      up right of it.
    */
  again:
    while (left < lefteq && (cmp = COMPAR(*left, *lefteq)) <= 0)
    {
      if (cmp < 0)
      {
        /* leave it where it is */
        left += width;
      }
      else
      {
        /* equal, so exchange with the element to
           the left of the "equal"-interval.
        */
        lefteq -= width;
        qexchange(left, lefteq);
      }
    }
    while (right > righteq)
    {
      if ((cmp = COMPAR(*right, *righteq)) < 0)
      {
        /* smaller, should go to left part
         */
        if (left < lefteq)
        {
          /* yes, we had a larger one at the
             left, so we can just exchange
          */
          qexchange(left, right);
          left += width;
          right -= width;
          goto again;
        }
        /* no more room at the left part, so we
           move the "equal-interval" one place to the
           right, and the smaller element to the
           left of it.
           This is best expressed as a three-way
           exchange.
        */
        righteq += width;
        q3exchange(left, righteq, right);
        lefteq += width;
        left = lefteq;
      }
      else if (cmp == 0)
      {
        /* equal, so exchange with the element to
           the right of the "equal-interval"
        */
        righteq += width;
        qexchange(right, righteq);
      }
      else /* just leave it */
        right -= width;
    }
    if (left < lefteq)
    {
      /* larger element to the left, but no more room,
         so move the "equal-interval" one place to the
         left, and the larger element to the right
         of it.
      */
      lefteq -= width;
      q3exchange(right, lefteq, left);
      righteq -= width;
      right = righteq;
      goto again;
    }
    /* now sort the "smaller" part */
    qsort1(a1, lefteq - width);
    /* and now the larger, saving a subroutine call
       because of the for(;;)
    */
    a1 = righteq + width;
  }
  /*NOTREACHED*/
}

SCALARRBUCKET(void)
qexchange(IT *p, IT *q)
{
  IT c;

  c = *p;
  *p = *q;
  *q = c;
}

SCALARRBUCKET(void)
q3exchange(IT *p, IT *q, IT *r)
{
  IT c;
  c = *p;
  *p++ = *r;
  *r++ = *q;
  *q++ = c;
}

SCALARRBUCKET(void)
breallocate(ulonglong new_max_primary)
{
  DBUG_ENTER("SCALARRBUCKET::breallocate[BUFFER]");

  longlong new_bwords = (new_max_primary / SAR_CHUNK + 1) * SAR_CHUNK;

  if (new_max_primary == HA_HANDLER_NOTFOUND)
  {
    if (Nrecords != HA_HANDLER_NOTFOUND)
    {
      LOGG("Crash SET\n");
      crashed = 1; // For real, ignore bucket_lock()
      LOCALLOG(LOG1("%s::breallocate max_value incorrect, crash\n", name));
      my_error(HA_ERR_CRASHED_ON_USAGE, MYF(0), "new max_value incorrect");
      DBUG_VOID_RETURN;
    }
    bwords = 0;
  }

  buffer = (VT *)resize_file_map_ro(buffer_name, buffer, bwords * sizeof(*buffer), new_bwords * sizeof(*buffer));
  if (buffer == MAP_FAILED)
    new_bwords = 0;

  bwords = new_bwords;
  DBUG_VOID_RETURN;
}

SCALARRBUCKET(void)
ireallocate(ulonglong new_Nrecords)
{
  DBUG_ENTER("SCALARRBUCKET::ireallocate");

  if (singlecol)
  {
    LOGG("Not allocating index space - singlecol w/o index!\n");
    DBUG_VOID_RETURN;
  }

  longlong new_iwords = (new_Nrecords / SAR_CHUNK + 1) * SAR_CHUNK;

  if (new_Nrecords == 0)
  {
    iwords = 0;
  }

  index = (IT *)resize_file_map_ro(index_name, index, iwords * sizeof(*index),
                                   new_iwords * sizeof(*index));
  if (index == MAP_FAILED)
    new_iwords = 0;

  iwords = new_iwords;
  DBUG_VOID_RETURN;
}

SCALARRBUCKET(void)
bag_bmp_filter(scalarrbucket<IT, VT> *src, bitbucket *filt)
{
  DBUG_ENTER("SB:bmp_filter");
  if (src->typecode != typecode)
  {
    LOGG("Aiai, type mismatch!\n");
    CRASH("SB:bag_bmp_filter() src->typecode != typecode");
  }

  if (!filt->records() || !src->bits->records())
  {
    LOGG("EMPTY RESULT - no change\n");
    DBUG_VOID_RETURN;
  }
  // Allocate for 100% hit rate: bitbucket max_value
  ulonglong bits_max = filt->max_value;
  if (src->bits->max_value < bits_max)
    bits_max = src->bits->max_value;

  LOG1("Reallocating bits_max=%llu\n", bits_max);

  bits->reallocate(bits_max);
  LOG1("Reallocating buffer for bits_max=%llu\n", bits_max);
  breallocate(bits_max);

  // Allocate index for 100% hit rate
  ulonglong max_records = filt->records();
  if (src->bits->records() < max_records)
    max_records = src->bits->records();

  LOG1("Reallocating index for %llu records\n", max_records);
  if (!singlecol)
    ireallocate(max_records);

  // Now, for the meat. We'll process records in index order, saving
  // the need to do a sort when we're done.
  longlong srci, nsrci;
  longlong dsti = 0;
  nsrci = src->bits->records();

  for (dsti = srci = 0; srci < nsrci; srci++)
  {
    IT pri;
    VT val;
    pri = src->index[srci];
    if (filt->isset(pri))
    {
      val = src->buffer[pri];
      index[dsti++] = pri;
      buffer[pri] = val;
      bits->set(pri);
    }
  }
  Nrecords = dsti;
  max_value = buffer[index[dsti - 1]];
  min_value = buffer[index[0]];
  LOG3("Nrecords=%llu, min_value=%.8g, max_value=%.8g\n", Nrecords, (double)min_value, (double)max_value);
  bits->dirty = 1;
  bits->save();
  dirty = 1;
  save();
  DBUG_VOID_RETURN;
}

// This will be compiled with VT=float/double;
// BUT: it's only intended for int/enum-like mathces
// We therefore do an explicit cast to ulonglong in calling set()

SCALARRBUCKET(void)
bag_bmp_sar_in_match(bitbucket *dest, int n, VT vals[])
{
  ulonglong curixix;
  DBUG_ENTER("SCALARRBUCKET::bmp_sar_in_match");
  for (int i = 0; i < n; i++)
  {
    if (isset_first(vals[i], &curixix))
    {
      do
      {
        dest->set((ulonglong)buffer[index[curixix]]);
      } while (buffer[++curixix] == vals[i]);
    }
  }
  DBUG_VOID_RETURN;
}

// Explicit instantiation is done together with ha_scalarray_toolbox!
