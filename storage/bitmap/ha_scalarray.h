/* Copyright (C) 2005 MySQL AB

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

/* The following defines can be increased if necessary */
#define SCALARRAY_MAX_KEY 3     /* Max allowed keys */
#define SCALARRAY_MAX_KEY_SEG 2 /* Max segments for key */
#define SCALARRAY_MAX_KEY_LENGTH (2 * sizeof(longlong))

/*
  Class definition for the scalarray storage engine HANDLER. The
  handler itself is more or less a "placeholder" or pass-through
  filter for the real requests. The reason being that we want to
  separate out every operation that deals with the actual data types
  (index/value) into a template class w/tools.

  The implementation of this is to have a union containing pointers to
  all the possible realizations of the template. Each realization is
  associated with TYPECODE, and we use macros containing case switches
  in order to branch off to the correct one when invoking methods in
  the toolbox.
*/

template <class IT, class VT>
class ha_scalarray_toolbox; // Forward

// Make sure types are defined globally!
#include "typecode_expansion.h"

class ha_scalarray : public ha_handler
{
  union
  {
#define TYPECODE_EXPANSION(IT, VT, tcode, extra) \
  class ha_scalarray_toolbox<IT, VT> *b##tcode;
#include "typecode_expansion.h"
    ALL_TYPECODE_EXPANSIONS()
#undef TYPECODE_EXPANSION
  } toolbox;

private:
  void add_one_ulp(double &value);
  void add_one_ulp(float &value);
  void sub_one_ulp(double &value);
  void sub_one_ulp(float &value);

public:
  int singlecol;
  int vc; // Value is in (MySQL) column no.

  int typecode;   // TYPECODE
  int val_is_d;   // True if value is internally represented as double
  int val_is_f;   // True if value is internally represented as float
  int val_is_dec; // True if value stems from a decimal field

  ha_scalarray(handlerton *hton, TABLE_SHARE *table_arg);
  ~ha_scalarray();

  /* The name that will be used for display purposes */
  const char *table_type() const { return "SCALARRAY"; }
  /*
    The name of the index type that will be used for display
    don't implement this method unless you really have indexes
  */
  const char **bas_ext() const;

  virtual Table_flags table_flags() const;
  virtual ulong index_flags(uint inx, uint part, bool all_parts) const;

  uint max_supported_keys() const { return SCALARRAY_MAX_KEY; }
  uint max_supported_key_length() const { return SCALARRAY_MAX_KEY_LENGTH; }
  uint max_supported_key_part_length() const { return SCALARRAY_MAX_KEY_LENGTH; }
  int open(const char *name, int mode, uint test_if_locked);
  int close(void);
  int rnd_init(bool scan);
  int rnd_next(uchar *buf);
  int rnd_prev(uchar *buf);
  int rnd_pos(uchar *buf, uchar *pos);

  int index_init(uint idx, bool sorted);

  int index_read(uchar *buf, const uchar *key, uint key_len, enum ha_rkey_function find_flag);
  int index_read_last(uchar *buf, const uchar *key, uint key_len);

  int index_next(uchar *buf);
  int index_prev(uchar *buf);
  int index_first(uchar *buf);
  int index_last(uchar *buf);
  ha_rows records_in_range(uint inx, const key_range *min_key, const key_range *max_key, page_range *pages);

  void position(const uchar *record);
  int create(const char *name, TABLE *table_arg, HA_CREATE_INFO *create_info);

  int repair(THD *thd, HA_CHECK_OPT *check_opt);
  int check(THD *thd, HA_CHECK_OPT *check_opt);

  // INSERT
  void start_bulk_insert(ha_rows rows, uint i_dont_know_what_this_is);
  int end_bulk_insert();
  int write_row(const uchar *buf __attribute__((unused)));

  // UPDATE - note we don't need to worry about duplicate keys, since
  // we have only one unique key!
  bool start_bulk_update(void);
  int bulk_update_row(const uchar *old_data, const uchar *new_data, ha_rows *dup_key_found);
  int exec_bulk_update(ha_rows *dup_key_found);
  int end_bulk_update();
  int update_row(const uchar *old_data, const uchar *new_data);

  // DELETE
  bool start_bulk_delete();
  int end_bulk_delete();
  int delete_row(const uchar *buf);
};
