/*****************************************************************************

Copyright (c) 1994, 2016, Oracle and/or its affiliates. All Rights Reserved.
Copyright (c) 2015, 2019, MariaDB Corporation.

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
@file include/btr0btr.ic
The B-tree

Created 6/2/1994 Heikki Tuuri
*******************************************************/

#include "mach0data.h"
#include "mtr0mtr.h"
#include "mtr0log.h"
#include "page0zip.h"

/** Gets a buffer page and declares its latching order level.
@param[in]	page_id	page id
@param[in]	zip_size	ROW_FORMAT=COMPRESSED page size, or 0
@param[in]	mode	latch mode
@param[in]	file	file name
@param[in]	line	line where called
@param[in]	index	index tree, may be NULL if it is not an insert buffer
tree
@param[in,out]	mtr	mini-transaction
@return block */
UNIV_INLINE
buf_block_t*
btr_block_get_func(
	const page_id_t		page_id,
	ulint			zip_size,
	ulint			mode,
	const char*		file,
	unsigned		line,
	dict_index_t*		index,
	mtr_t*			mtr)
{
	buf_block_t*	block;
	dberr_t		err=DB_SUCCESS;

	block = buf_page_get_gen(
		page_id, zip_size, mode, NULL, BUF_GET, file, line, mtr, &err);

	if (err == DB_DECRYPTION_FAILED) {
		if (index && index->table) {
			index->table->file_unreadable = true;
		}
	}

	if (block) {
		if (mode != RW_NO_LATCH) {

			buf_block_dbg_add_level(
				block, index != NULL && dict_index_is_ibuf(index)
				? SYNC_IBUF_TREE_NODE : SYNC_TREE_NODE);
		}
	}

	return(block);
}

/**************************************************************//**
Sets the index id field of a page. */
UNIV_INLINE
void
btr_page_set_index_id(
/*==================*/
	page_t*		page,	/*!< in: page to be created */
	page_zip_des_t*	page_zip,/*!< in: compressed page whose uncompressed
				part will be updated, or NULL */
	index_id_t	id,	/*!< in: index id */
	mtr_t*		mtr)	/*!< in: mtr */
{
	if (page_zip) {
		mach_write_to_8(page + (PAGE_HEADER + PAGE_INDEX_ID), id);
		page_zip_write_header(page_zip,
				      page + (PAGE_HEADER + PAGE_INDEX_ID),
				      8, mtr);
	} else {
		mlog_write_ull(page + (PAGE_HEADER + PAGE_INDEX_ID), id, mtr);
	}
}

/**************************************************************//**
Gets the index id field of a page.
@return index id */
UNIV_INLINE
index_id_t
btr_page_get_index_id(
/*==================*/
	const page_t*	page)	/*!< in: index page */
{
	return(mach_read_from_8(page + PAGE_HEADER + PAGE_INDEX_ID));
}

/********************************************************//**
Sets the node level field in an index page. */
UNIV_INLINE
void
btr_page_set_level(
/*===============*/
	page_t*		page,	/*!< in: index page */
	page_zip_des_t*	page_zip,/*!< in: compressed page whose uncompressed
				part will be updated, or NULL */
	ulint		level,	/*!< in: level, leaf level == 0 */
	mtr_t*		mtr)	/*!< in: mini-transaction handle */
{
	ut_ad(page != NULL);
	ut_ad(mtr != NULL);
	ut_ad(level <= BTR_MAX_NODE_LEVEL);

	if (page_zip) {
		mach_write_to_2(page + (PAGE_HEADER + PAGE_LEVEL), level);
		page_zip_write_header(page_zip,
				      page + (PAGE_HEADER + PAGE_LEVEL),
				      2, mtr);
	} else {
		mlog_write_ulint(page + (PAGE_HEADER + PAGE_LEVEL), level,
				 MLOG_2BYTES, mtr);
	}
}

/********************************************************//**
Sets the next index page field. */
UNIV_INLINE
void
btr_page_set_next(
/*==============*/
	page_t*		page,	/*!< in: index page */
	page_zip_des_t*	page_zip,/*!< in: compressed page whose uncompressed
				part will be updated, or NULL */
	ulint		next,	/*!< in: next page number */
	mtr_t*		mtr)	/*!< in: mini-transaction handle */
{
	ut_ad(page != NULL);
	ut_ad(mtr != NULL);

	if (page_zip) {
		mach_write_to_4(page + FIL_PAGE_NEXT, next);
		page_zip_write_header(page_zip, page + FIL_PAGE_NEXT, 4, mtr);
	} else {
		mlog_write_ulint(page + FIL_PAGE_NEXT, next, MLOG_4BYTES, mtr);
	}
}

/********************************************************//**
Sets the previous index page field. */
UNIV_INLINE
void
btr_page_set_prev(
/*==============*/
	page_t*		page,	/*!< in: index page */
	page_zip_des_t*	page_zip,/*!< in: compressed page whose uncompressed
				part will be updated, or NULL */
	ulint		prev,	/*!< in: previous page number */
	mtr_t*		mtr)	/*!< in: mini-transaction handle */
{
	ut_ad(page != NULL);
	ut_ad(mtr != NULL);

	if (page_zip) {
		mach_write_to_4(page + FIL_PAGE_PREV, prev);
		page_zip_write_header(page_zip, page + FIL_PAGE_PREV, 4, mtr);
	} else {
		mlog_write_ulint(page + FIL_PAGE_PREV, prev, MLOG_4BYTES, mtr);
	}
}

/**************************************************************//**
Gets the child node file address in a node pointer.
NOTE: the offsets array must contain all offsets for the record since
we read the last field according to offsets and assume that it contains
the child page number. In other words offsets must have been retrieved
with rec_get_offsets(n_fields=ULINT_UNDEFINED).
@return child node address */
UNIV_INLINE
ulint
btr_node_ptr_get_child_page_no(
/*===========================*/
	const rec_t*	rec,	/*!< in: node pointer record */
	const rec_offs*	offsets)/*!< in: array returned by rec_get_offsets() */
{
	const byte*	field;
	ulint		len;
	ulint		page_no;

	ut_ad(!rec_offs_comp(offsets) || rec_get_node_ptr_flag(rec));

	/* The child address is in the last field */
	field = rec_get_nth_field(rec, offsets,
				  rec_offs_n_fields(offsets) - 1, &len);

	ut_ad(len == 4);

	page_no = mach_read_from_4(field);
	ut_ad(page_no > 1);

	return(page_no);
}

/**************************************************************//**
Releases the latches on a leaf page and bufferunfixes it. */
UNIV_INLINE
void
btr_leaf_page_release(
/*==================*/
	buf_block_t*	block,		/*!< in: buffer block */
	ulint		latch_mode,	/*!< in: BTR_SEARCH_LEAF or
					BTR_MODIFY_LEAF */
	mtr_t*		mtr)		/*!< in: mtr */
{
	ut_ad(latch_mode == BTR_SEARCH_LEAF
	      || latch_mode == BTR_MODIFY_LEAF
	      || latch_mode == BTR_NO_LATCHES);

	ut_ad(!mtr_memo_contains(mtr, block, MTR_MEMO_MODIFY));

	ulint mode;
	switch (latch_mode) {
		case BTR_SEARCH_LEAF:
			mode = MTR_MEMO_PAGE_S_FIX;
			break;
		case BTR_MODIFY_LEAF:
			mode = MTR_MEMO_PAGE_X_FIX;
			break;
		case BTR_NO_LATCHES:
			mode = MTR_MEMO_BUF_FIX;
			break;
		default:
			ut_a(0);
	}

	mtr->memo_release(block, mode);
}
