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

template <class IT, class VT>
class ha_scalarray_toolbox
{
  MEI *mei;
  friend class ha_scalarray;
  ha_scalarray *handler;
  ulonglong curpos;
  VT curval;

  // Index-into-index :-)
  // Note: the index can't have more entries than the [primary] index type
  // allows. So, an IT variable will do. Right? Nope. Since IT is unsigned,
  // we need an alternate way to sign "not found". I.e. SCALARRAY_NOTFOUND
  ulonglong curixix; // Index into index :-)

  ha_scalarray_toolbox(ha_scalarray *const h_in, MEI *mei_in);
  ~ha_scalarray_toolbox();

  void add_one_ulp(VT &value);
  void sub_one_ulp(VT &value);

  int open(const char *name, int mode, uint test_if_locked);
  int close(void);
  int rnd_init(bool scan);
  void putrow(uchar *buf, longlong cpos, VT cval);
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
  void start_bulk_insert(ha_rows rows);
  int end_bulk_insert();
  int write_row(const uchar *buf);

  // UPDATE - note we don't need to worry about duplicate keys, since
  // we have only one unique key!
  bool start_bulk_update();
  int bulk_update_row(const uchar *old_data, const uchar *new_data, ha_rows *dup_key_found);
  int exec_bulk_update(ha_rows *dup_key_found);
  int end_bulk_update();
  int update_row_internal(const uchar *old_data, const uchar *new_data);
  int update_row(const uchar *old_data, const uchar *new_data);

  // DELETE
  bool start_bulk_delete();
  int end_bulk_delete();
  int delete_row(const uchar *buf);

  // UTILITIES:

#undef EOFF
#define EOFF(f) (f == 1 ? handler->table->field[0]->pack_length() : 0)

  inline void VT_put(uint fieldno, uchar *buf, VT val)
  {
    DBUG_ENTER2("TOOLBOX::VT_put(fieldno=%u; val=%.10g)", fieldno, (double)val);
    buf += EOFF(fieldno);

    Field *field = handler->table->field[fieldno];

    if (field->type() == MYSQL_TYPE_DOUBLE)
    {
      field->pack(buf, (uchar *)&val);
    }
    else if (field->type() == MYSQL_TYPE_FLOAT)
    {
      field->pack(buf, (uchar *)&val);
    }
    else if (field->type() == MYSQL_TYPE_NEWDECIMAL)
    {
      int packlen = field->pack_length();
      union
      {
        ulonglong l;
        uchar b[sizeof(longlong)];
      } oval;
      oval.l = (ulonglong)val; // Will be compiled for e.g. DOUBLE/newdecimal
      for (int i = 0; i < packlen; i++)
      {
        buf[i] = oval.b[packlen - i - 1];
      }
      LOG2("put NEWDECIMAL as longlong %llx (packlen %d)\n", oval.l, packlen);
      DBUG_VOID_RETURN;
    }

    // Vanilla case:
    field->pack(buf, (uchar *)&val);
    DBUG_VOID_RETURN;
  }

  inline void IT_put(uint fieldno, uchar *buf, IT pos)
  {
    DBUG_ENTER2("TOOLBOX::IT_put(fieldno=%u pos=%llu)", fieldno, (ulonglong)pos);
    buf += EOFF(fieldno);

    Field *field = handler->table->field[fieldno];
    field->pack(buf, (uchar *)&pos);
    DBUG_VOID_RETURN;
  }
};

#define SAR ".SAR" // ScalARray
#define SAI ".SAI" // ScalArray Index
