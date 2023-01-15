/*****************************************************************************

Copyright (c) 1995, 2015, Oracle and/or its affiliates. All Rights Reserved.
Copyright (c) 2017, 2020, MariaDB Corporation.

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
@file include/log0log.ic
Database log

Created 12/9/1995 Heikki Tuuri
*******************************************************/

#include "mach0data.h"
#include "srv0mon.h"
#include "ut0crc32.h"

#ifdef UNIV_LOG_LSN_DEBUG
#include "mtr0types.h"
#endif /* UNIV_LOG_LSN_DEBUG */
extern ulong srv_log_buffer_size;

/************************************************************//**
Gets a log block flush bit.
@return TRUE if this block was the first to be written in a log flush */
UNIV_INLINE
ibool
log_block_get_flush_bit(
/*====================*/
	const byte*	log_block)	/*!< in: log block */
{
	if (LOG_BLOCK_FLUSH_BIT_MASK
	    & mach_read_from_4(log_block + LOG_BLOCK_HDR_NO)) {

		return(TRUE);
	}

	return(FALSE);
}

/************************************************************//**
Sets the log block flush bit. */
UNIV_INLINE
void
log_block_set_flush_bit(
/*====================*/
	byte*	log_block,	/*!< in/out: log block */
	ibool	val)		/*!< in: value to set */
{
	ulint	field;

	field = mach_read_from_4(log_block + LOG_BLOCK_HDR_NO);

	if (val) {
		field = field | LOG_BLOCK_FLUSH_BIT_MASK;
	} else {
		field = field & ~LOG_BLOCK_FLUSH_BIT_MASK;
	}

	mach_write_to_4(log_block + LOG_BLOCK_HDR_NO, field);
}

/************************************************************//**
Gets a log block number stored in the header.
@return log block number stored in the block header */
UNIV_INLINE
ulint
log_block_get_hdr_no(
/*=================*/
	const byte*	log_block)	/*!< in: log block */
{
	return(~LOG_BLOCK_FLUSH_BIT_MASK
	       & mach_read_from_4(log_block + LOG_BLOCK_HDR_NO));
}

/************************************************************//**
Sets the log block number stored in the header; NOTE that this must be set
before the flush bit! */
UNIV_INLINE
void
log_block_set_hdr_no(
/*=================*/
	byte*	log_block,	/*!< in/out: log block */
	ulint	n)		/*!< in: log block number: must be > 0 and
				< LOG_BLOCK_FLUSH_BIT_MASK */
{
	ut_ad(n > 0);
	ut_ad(n < LOG_BLOCK_FLUSH_BIT_MASK);

	mach_write_to_4(log_block + LOG_BLOCK_HDR_NO, n);
}

/************************************************************//**
Gets a log block data length.
@return log block data length measured as a byte offset from the block start */
UNIV_INLINE
ulint
log_block_get_data_len(
/*===================*/
	const byte*	log_block)	/*!< in: log block */
{
	return(mach_read_from_2(log_block + LOG_BLOCK_HDR_DATA_LEN));
}

/************************************************************//**
Sets the log block data length. */
UNIV_INLINE
void
log_block_set_data_len(
/*===================*/
	byte*	log_block,	/*!< in/out: log block */
	ulint	len)		/*!< in: data length */
{
	mach_write_to_2(log_block + LOG_BLOCK_HDR_DATA_LEN, len);
}

/************************************************************//**
Gets a log block first mtr log record group offset.
@return first mtr log record group byte offset from the block start, 0
if none */
UNIV_INLINE
ulint
log_block_get_first_rec_group(
/*==========================*/
	const byte*	log_block)	/*!< in: log block */
{
	return(mach_read_from_2(log_block + LOG_BLOCK_FIRST_REC_GROUP));
}

/************************************************************//**
Sets the log block first mtr log record group offset. */
UNIV_INLINE
void
log_block_set_first_rec_group(
/*==========================*/
	byte*	log_block,	/*!< in/out: log block */
	ulint	offset)		/*!< in: offset, 0 if none */
{
	mach_write_to_2(log_block + LOG_BLOCK_FIRST_REC_GROUP, offset);
}

/************************************************************//**
Gets a log block checkpoint number field (4 lowest bytes).
@return checkpoint no (4 lowest bytes) */
UNIV_INLINE
ulint
log_block_get_checkpoint_no(
/*========================*/
	const byte*	log_block)	/*!< in: log block */
{
	return(mach_read_from_4(log_block + LOG_BLOCK_CHECKPOINT_NO));
}

/************************************************************//**
Sets a log block checkpoint number field (4 lowest bytes). */
UNIV_INLINE
void
log_block_set_checkpoint_no(
/*========================*/
	byte*		log_block,	/*!< in/out: log block */
	ib_uint64_t	no)		/*!< in: checkpoint no */
{
	mach_write_to_4(log_block + LOG_BLOCK_CHECKPOINT_NO, (ulint) no);
}

/************************************************************//**
Converts a lsn to a log block number.
@return log block number, it is > 0 and <= 1G */
UNIV_INLINE
ulint
log_block_convert_lsn_to_no(
/*========================*/
	lsn_t	lsn)	/*!< in: lsn of a byte within the block */
{
	return(((ulint) (lsn / OS_FILE_LOG_BLOCK_SIZE) &
		DBUG_EVALUATE_IF("innodb_small_log_block_no_limit",
			0xFUL, 0x3FFFFFFFUL)) + 1);
}

/** Calculate the checksum for a log block using the pre-5.7.9 algorithm.
@param[in]	block	log block
@return		checksum */
UNIV_INLINE
ulint
log_block_calc_checksum_format_0(
	const byte*	block)
{
	ulint	sum;
	ulint	sh;
	ulint	i;

	sum = 1;
	sh = 0;

	for (i = 0; i < OS_FILE_LOG_BLOCK_SIZE - LOG_BLOCK_CHECKSUM; i++) {
		ulint	b = (ulint) block[i];
		sum &= 0x7FFFFFFFUL;
		sum += b;
		sum += b << sh;
		sh++;
		if (sh > 24) {
			sh = 0;
		}
	}

	return(sum);
}

/** Calculate the checksum for a log block using the MySQL 5.7 algorithm.
@param[in]	block	log block
@return checksum */
UNIV_INLINE
ulint
log_block_calc_checksum_crc32(
	const byte*	block)
{
	return ut_crc32(block, OS_FILE_LOG_BLOCK_SIZE - LOG_BLOCK_CHECKSUM);
}

/************************************************************//**
Gets a log block checksum field value.
@return checksum */
UNIV_INLINE
ulint
log_block_get_checksum(
/*===================*/
	const byte*	log_block)	/*!< in: log block */
{
	return(mach_read_from_4(log_block + OS_FILE_LOG_BLOCK_SIZE
				- LOG_BLOCK_CHECKSUM));
}

/************************************************************//**
Sets a log block checksum field value. */
UNIV_INLINE
void
log_block_set_checksum(
/*===================*/
	byte*	log_block,	/*!< in/out: log block */
	ulint	checksum)	/*!< in: checksum */
{
	mach_write_to_4(log_block + OS_FILE_LOG_BLOCK_SIZE
			- LOG_BLOCK_CHECKSUM,
			checksum);
}

/************************************************************//**
Initializes a log block in the log buffer. */
UNIV_INLINE
void
log_block_init(
/*===========*/
	byte*	log_block,	/*!< in: pointer to the log buffer */
	lsn_t	lsn)		/*!< in: lsn within the log block */
{
	ulint	no;

	no = log_block_convert_lsn_to_no(lsn);

	log_block_set_hdr_no(log_block, no);

	log_block_set_data_len(log_block, LOG_BLOCK_HDR_SIZE);
	log_block_set_first_rec_group(log_block, 0);
}

/** Append a string to the log.
@param[in]	str		string
@param[in]	len		string length
@param[out]	start_lsn	start LSN of the log record
@return end lsn of the log record, zero if did not succeed */
UNIV_INLINE
lsn_t
log_reserve_and_write_fast(
	const void*	str,
	ulint		len,
	lsn_t*		start_lsn)
{
	ut_ad(log_mutex_own());
	ut_ad(len > 0);

#ifdef UNIV_LOG_LSN_DEBUG
	/* Append a MLOG_LSN record after mtr_commit(), except when
	the last bytes could be a MLOG_CHECKPOINT marker. We have special
	handling when the log consists of only a single MLOG_CHECKPOINT
	record since the latest checkpoint, and appending the
	MLOG_LSN would ruin that.

	Note that a longer redo log record could happen to end in what
	looks like MLOG_CHECKPOINT, and we could be omitting MLOG_LSN
	without reason. This is OK, because writing the MLOG_LSN is
	just a 'best effort', aimed at finding log corruption due to
	bugs in the redo log writing logic. */
	const ulint	lsn_len
		= len >= SIZE_OF_MLOG_CHECKPOINT
		&& MLOG_CHECKPOINT == static_cast<const char*>(str)[
			len - SIZE_OF_MLOG_CHECKPOINT]
		? 0
		: 1
		+ mach_get_compressed_size(log_sys.lsn >> 32)
		+ mach_get_compressed_size(log_sys.lsn & 0xFFFFFFFFUL);
#endif /* UNIV_LOG_LSN_DEBUG */

	const ulint	data_len = len
#ifdef UNIV_LOG_LSN_DEBUG
		+ lsn_len
#endif /* UNIV_LOG_LSN_DEBUG */
		+ log_sys.buf_free % OS_FILE_LOG_BLOCK_SIZE;

	if (data_len >= log_sys.trailer_offset()) {

		/* The string does not fit within the current log block
		or the log block would become full */

		return(0);
	}

	*start_lsn = log_sys.lsn;

#ifdef UNIV_LOG_LSN_DEBUG
	if (lsn_len) {
		/* Write the LSN pseudo-record. */
		byte* b = &log_sys.buf[log_sys.buf_free];

		*b++ = MLOG_LSN | (MLOG_SINGLE_REC_FLAG & *(const byte*) str);

		/* Write the LSN in two parts,
		as a pseudo page number and space id. */
		b += mach_write_compressed(b, log_sys.lsn >> 32);
		b += mach_write_compressed(b, log_sys.lsn & 0xFFFFFFFFUL);
		ut_a(b - lsn_len == &log_sys.buf[log_sys.buf_free]);

		::memcpy(b, str, len);

		len += lsn_len;
	} else
#endif /* UNIV_LOG_LSN_DEBUG */
	memcpy(log_sys.buf + log_sys.buf_free, str, len);

	log_block_set_data_len(
                reinterpret_cast<byte*>(ut_align_down(
                        log_sys.buf + log_sys.buf_free,
                        OS_FILE_LOG_BLOCK_SIZE)),
                data_len);

	log_sys.buf_free += ulong(len);

	ut_ad(log_sys.buf_free <= srv_log_buffer_size);

	log_sys.lsn += len;

	MONITOR_SET(MONITOR_LSN_CHECKPOINT_AGE,
		    log_sys.lsn - log_sys.last_checkpoint_lsn);

	return(log_sys.lsn);
}

/************************************************************//**
Gets the current lsn.
@return current lsn */
UNIV_INLINE
lsn_t
log_get_lsn(void)
/*=============*/
{
	lsn_t	lsn;

	log_mutex_enter();

	lsn = log_sys.lsn;

	log_mutex_exit();

	return(lsn);
}

/************************************************************//**
Gets the last lsn that is fully flushed to disk.
@return	last flushed lsn */
UNIV_INLINE
ib_uint64_t
log_get_flush_lsn(void)
{
	ib_uint64_t	lsn;

	log_mutex_enter();

	lsn = log_sys.flushed_to_disk_lsn;

	log_mutex_exit();

	return(lsn);
}

/************************************************************//**
Gets the current lsn with a trylock
@return	current lsn or 0 if false*/
UNIV_INLINE
lsn_t
log_get_lsn_nowait(void)
/*====================*/
{
	lsn_t	lsn=0;

	if (!mutex_enter_nowait(&(log_sys.mutex))) {

		lsn = log_sys.lsn;

		mutex_exit(&(log_sys.mutex));
	}

	return(lsn);
}

/****************************************************************
Gets the log group capacity. It is OK to read the value without
holding log_sys.mutex because it is constant.
@return log group capacity */
UNIV_INLINE
lsn_t
log_get_capacity(void)
/*==================*/
{
	return(log_sys.log_group_capacity);
}

/****************************************************************
Get log_sys::max_modified_age_async. It is OK to read the value without
holding log_sys::mutex because it is constant.
@return max_modified_age_async */
UNIV_INLINE
lsn_t
log_get_max_modified_age_async(void)
/*================================*/
{
	return(log_sys.max_modified_age_async);
}

/***********************************************************************//**
Checks if there is need for a log buffer flush or a new checkpoint, and does
this if yes. Any database operation should call this when it has modified
more than about 4 pages. NOTE that this function may only be called when the
OS thread owns no synchronization objects except the dictionary mutex. */
UNIV_INLINE
void
log_free_check(void)
/*================*/
{
	/* During row_log_table_apply(), this function will be called while we
	are holding some latches. This is OK, as long as we are not holding
	any latches on buffer blocks. */

#ifdef UNIV_DEBUG
	static const latch_level_t latches[] = {
		SYNC_DICT,		/* dict_sys.mutex during
					commit_try_rebuild() */
		SYNC_DICT_OPERATION,	/* dict_sys.latch X-latch during
					commit_try_rebuild() */
		SYNC_FTS_CACHE,		/* fts_cache_t::lock */
		SYNC_INDEX_TREE		/* index->lock */
	};
#endif /* UNIV_DEBUG */

	ut_ad(!sync_check_iterate(
		      sync_allowed_latches(latches,
					   latches + UT_ARR_SIZE(latches))));

	if (log_sys.check_flush_or_checkpoint) {

		log_check_margins();
	}
}
