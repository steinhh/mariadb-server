/*****************************************************************************

Copyright (c) 1996, 2013, Oracle and/or its affiliates. All Rights Reserved.
Copyright (c) 2020, MariaDB Corporation.

This program is free software; you can redistribute it and/or modify it under
the terms of the GNU General Public License as published by the Free Software
Foundation; version 2 of the License.

This program is distributed in the hope that it will be useful, but WITHOUT
ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details.

You should have received a copy of the GNU General Public License along with
this program; if not, write to the Free Software Foundation, Inc.,
51 Franklin Street, Fifth Floor, Boston, MA 02110-1335 USA

*****************************************************************************/

/**************************************************//**
@file include/dict0boot.ic
Data dictionary creation and booting

Created 4/18/1996 Heikki Tuuri
*******************************************************/

/**********************************************************************//**
Returns a new row id.
@return the new id */
UNIV_INLINE
row_id_t
dict_sys_get_new_row_id(void)
/*=========================*/
{
	row_id_t	id;

	mutex_enter(&dict_sys.mutex);

	id = dict_sys.row_id;

	if (0 == (id % DICT_HDR_ROW_ID_WRITE_MARGIN)) {

		dict_hdr_flush_row_id();
	}

	dict_sys.row_id++;

	mutex_exit(&dict_sys.mutex);

	return(id);
}

/**********************************************************************//**
Writes a row id to a record or other 6-byte stored form. */
UNIV_INLINE
void
dict_sys_write_row_id(
/*==================*/
	byte*		field,	/*!< in: record field */
	row_id_t	row_id)	/*!< in: row id */
{
	compile_time_assert(DATA_ROW_ID_LEN == 6);
	mach_write_to_6(field, row_id);
}

/*********************************************************************//**
Check if a table id belongs to  system table.
@return true if the table id belongs to a system table. */
UNIV_INLINE
bool
dict_is_sys_table(
/*==============*/
	table_id_t	id)		/*!< in: table id to check */
{
	return(id < DICT_HDR_FIRST_ID);
}


