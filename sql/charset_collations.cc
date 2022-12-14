/* Copyright (c) 2023, MariaDB Corporation.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation; version 2 of the License.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1335  USA */


#include "my_global.h"
#include "my_sys.h"
#include "lex_charset.h"
#include "mysqld_error.h"
#include "charset_collations.h"
#include "simple_tokenizer.h"

bool Charset_collation_map_st::insert_or_replace(
                                  const Lex_exact_charset &charset,
                                  const Lex_extended_collation &collation,
                                  bool error_on_conflicting_duplicate)
{
  Lex_exact_charset_opt_extended_collate res(charset);
  Used used;
  if (res.merge_collation_override(&used, *this, collation))
    return true;

  if (error_on_conflicting_duplicate)
  {
    const Elem_st *dup;
    if ((dup= find_elem_by_charset_id(charset.charset_info()->number)) &&
        dup->collation() != res.collation().charset_info())
    {
      my_error(ER_CONFLICTING_DECLARATIONS, MYF(0),
               "", dup->collation()->coll_name.str,
               "", res.collation().charset_info()->coll_name.str);
      return true;
    }
  }
  return insert_or_replace(Elem(charset.charset_info(),
                                res.collation().charset_info()));
}


bool Charset_collation_map_st::from_text(const LEX_CSTRING &str, myf utf8_flag)
{
  init();
  Simple_tokenizer stream(str.str, str.length);

  stream.get_spaces();
  if (stream.eof())
    return 0; /* Empty string */

  for ( ; ; )
  {
    LEX_CSTRING charset_name= stream.get_ident();
    if (!charset_name.length)
      return true;
    stream.get_spaces();
    if (stream.get_char('='))
      return true;
    stream.get_spaces();
    LEX_CSTRING collation_name= stream.get_ident();
    if (!collation_name.length)
      return true;

    char charset_name_c[MY_CS_CHARACTER_SET_NAME_SIZE + 1/*for '\0'*/];
    strmake(charset_name_c, charset_name.str, charset_name.length);
    CHARSET_INFO *cs= get_charset_by_csname(charset_name_c,
                                            MY_CS_PRIMARY, utf8_flag);
    if (!cs)
    {
      my_error(ER_UNKNOWN_CHARACTER_SET, MYF(0), charset_name_c);
      return true;
    }

    char collation_name_c[MY_CS_COLLATION_NAME_SIZE + 1/*for '\0'*/];
    strmake(collation_name_c, collation_name.str, collation_name.length);

    Lex_exact_collation tmpec(&my_charset_bin);
    Lex_extended_collation tmp(tmpec);
    if (tmp.set_by_name(collation_name_c, utf8_flag))
      return true;

    /*
      Don't allow duplicate conflicting declarations within the same string:
        SET @@var='utf8mb3=utf8mb3_general_ci,utf8mb3=utf8mb3_bin';
    */
    if (insert_or_replace(Lex_exact_charset(cs), tmp, true/*err on dup*/))
      return true;

    stream.get_spaces();
    if (stream.eof())
      break;
    if (stream.ptr()[0] != ',')
      return true;
    stream.get_char(',');
    stream.get_spaces();
  }
  return false;
}
