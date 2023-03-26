/* Copyright (C) 2009 Stein Vidar Hagfors Haugan

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

#define BMI ".BMI" // BitMap Index

/* The following defines can be increased if necessary */
#define BITMAP_MAX_KEY 1     /* Max allowed keys */
#define BITMAP_MAX_KEY_SEG 1 /* Max segments for key */
#define BITMAP_MAX_KEY_LENGTH sizeof(longlong)

/*
  Class definition for the bitmap storage engine
*/
class ha_bitmap : public ha_handler
{
  ulonglong current_position; // Whoops
public:
  ha_bitmap(handlerton *hton, TABLE_SHARE *table_arg);
  ~ha_bitmap();

  /* The name that will be used for display purposes */
  const char *table_type() const { return "BITMAP"; }
  /*
    The name of the index type that will be used for display
    don't implement this method unless you really have indexes
  */
  const char **bas_ext() const;

  ulonglong table_flags() const; // ulonglong_ok

  ulong index_flags(uint inx, uint part, bool all_parts) const
  {
    return ((table_share->key_info[inx].algorithm == HA_KEY_ALG_FULLTEXT) ? 0 : HA_READ_NEXT | HA_READ_PREV | HA_READ_RANGE | HA_READ_ORDER | HA_KEYREAD_ONLY);
  }

  uint max_supported_keys() const { return BITMAP_MAX_KEY; }
  uint max_supported_key_length() const { return BITMAP_MAX_KEY_LENGTH; }
  uint max_supported_key_part_length() const { return BITMAP_MAX_KEY_LENGTH; }
  int open(const char *name, int mode, uint test_if_locked);
  int close(void);
  int rnd_init(bool scan);
  int rnd_next(uchar *buf);
  int rnd_prev(uchar *buf);
  int rnd_pos(uchar *buf, uchar *pos);

  int index_init(uint idx, bool sorted)
  {
    ACTINX = idx;
    current_position = act_fflag = -1;
    return 0;
  }
  int index_read(uchar *buf, const uchar *key,
                 uint key_len, enum ha_rkey_function find_flag);
  int index_read_last(uchar *buf, const uchar *key, uint key_len);

  int index_next(uchar *buf);
  int index_prev(uchar *buf);
  int index_first(uchar *buf);
  int index_last(uchar *buf);
  ha_rows records_in_range(uint inx, const key_range *min_key, const key_range *max_key, page_range *pages);

  void position(const uchar *record);
  int create(const char *name, TABLE *table_arg,
             HA_CREATE_INFO *create_info);
  virtual int write_row(const uchar *buf);
  virtual int update_row(const uchar *old_data, const uchar *new_data);
  virtual int delete_row(const uchar *buf);
};
