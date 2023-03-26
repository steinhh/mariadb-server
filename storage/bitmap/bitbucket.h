#ifndef BITBUCKET_H
#define BITBUCKET_H

#define BMP ".BMP"
#define BMI ".BMI"

#define BMP_CHUNK (2 * 8 * 1024)
#define BMP_MASK 63 /* val &  BMP_MASK  =>  bit_no  */
#define BMP_SHIFT 6 /* val >> BMP_SHIFT =>  word_no */

/* BMP_MASK = (1 << BMP_SHIFT) -1 */
/*          ==   NBITS         -1 */

class bitbucket : public Bucket
{
public:
  ulonglong *buffer;
  ulonglong words;     // Words allocated/mmapped in buffer
  ulonglong max_value; //

  char buffer_name[FN_REFLEN];

  void reallocate(ulonglong new_max_val); // To be accessed from scalarbag!

  void Join(bitbucket **s, const int n);
  void Union(bitbucket **s, const int n);

  void load(void);
  bitbucket(EngineLocked engine_locked, MEI *meiptr);
  bitbucket(const char *name, EngineLocked engine_locked, const char **ext, MEI *mei);
  virtual ~bitbucket(void);
  void save();
  void set(ulonglong val);
  void unset(ulonglong val);
  int isset(ulonglong val);
  int isset(ulonglong word, int bit);
  ulonglong next(ulonglong current);
  ulonglong prev(ulonglong current);
  ulonglong autoincrement_value(void);
  ulonglong deleted(void);
  ulonglong mean_rec_length(void);
  ulonglong data_file_length();
  void hibernate(void);
  void wakeup(void);
  void truncate(void);
};
#endif
