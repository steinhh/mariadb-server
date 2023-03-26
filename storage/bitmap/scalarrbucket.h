#ifndef SCALARRBUCKET_H
#define SCALARRBUCKET_H

#define SAR ".SAR" // ScalARray
#define SAI ".SAI" // ScalArray Index

#define SAR_CHUNK (2 * 8 * 1024)

template <class IT, class VT>
class scalarrbucket : public Bucket
{
  char buffer_name[FN_REFLEN];
  char index_name[FN_REFLEN];

public:
  int typecode;
  int singlecol;
  bitbucket *bits;
  ulonglong have_deleted;
  ulonglong have_inserted;
  bitbucket *inserted;
  VT max_value;
  VT min_value;

  // NOTE: all "IT-like" variables/params (that MAY OR MAY NOT point
  // to an actual table entry) need to be ULONGLONG so it can contain
  // HA_HANDLER_NOTFOUND!

  IT *index;        // Whooops, this had become VT!!
  ulonglong iwords; // Words allocated in index [from # of entries]

  VT *buffer;
  ulonglong bwords; // Words allocated in buffer [from max primary key]

  scalarrbucket(const char *name, EngineLocked engine_locked,
                int typecode, int singlecol,
                const char **exts, MEI *mei);
  virtual ~scalarrbucket(void);
  void save();
  void bag_load(void);
  void load(void);
  ulonglong records(void); // Virtual: read from bits->records()
  ulonglong autoincrement_value(void);
  ulonglong deleted(void);
  ulonglong mean_rec_length(void);
  ulonglong data_file_length();
  void bag_hibernate();
  void hibernate(void);
  void wakeup(void);
  int is_crashed(void);

  int repair(void);
  int check(void);
  int index_size(int actinx);
  void sort(void);
  void purge(void);
  void merge(ulonglong Orecords);
  void set(IT val1, VT val2);
  void unset(IT val);

  // Isset_first/_last behave like overshooting brute-force searches
  // starting from the beginning and end of the index, respectively.

  // Return *xref pointing to first entry equal to val, or to first
  // [smallest] entry larger than val. If no such entry exists, return
  // HA_HANDLER_NOTFOUND
  int isset_first(VT val_in, ulonglong *xref);

  // Return *xref pointing to last entry equal to val, or to first
  // [largest] entry smaller than val. If no such entry exists, return
  // HA_HANDLER_NOTFOUND
  int isset_last(VT val_in, ulonglong *xref);

  int check_and_repair(void);

  void pre_save(void);
  void QSORT(IT *base, size_t nel);
  void qsort1(IT *a1, IT *a2);
  void qexchange(IT *p, IT *q);
  void q3exchange(IT *p, IT *q, IT *r);

  IT BSEARCH2(VT key, int sign_when_equal);
  //  IT SEARCH2(VT key,int sign_when_equal); // Doesn't exist, and not used??

  void breallocate(ulonglong new_max_primary);
  void ireallocate(ulonglong new_Nrecords);

  void bag_bmp_filter(scalarrbucket<IT, VT> *srcif, bitbucket *filt);
  void bmp_filter(scalarrbucket<IT, VT> *srcif, bitbucket *filt);

  void bag_bmp_sar_in_match(bitbucket *dest, int n, VT vals[]);
  void bmp_sar_in_match(bitbucket *dest, int n, VT vals[]);
};

#endif
