/*
 * Copyright (c) 2017, MammothDB
 * All rights reserved.
 * 
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *  - Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 *  - Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 *  - Neither the name of the <organization> nor the
 *    names of its contributors may be used to endorse or promote products
 *    derived from this software without specific prior written permission.
 * 
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL MAMMOTHDB BE LIABLE FOR ANY
 * DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

#include "ieee754.h"
#include "decimal.h"
#include "timestamp.h"
#include "murmur3.h"
#include "lib.h"

void extract_arg(struct function_s *function);
int set_error(struct job_s *job, int error, char *error_msg, ...);
void build_meta(struct data_storage_s *storage, int flags);
void *chunk_new(struct data_storage_s *storage, uint32_t row_len, int32_t slot);
void chunk_load(struct data_storage_s *storage, uint32_t chunk_id);
void chunk_save(struct data_storage_s *storage);
void *chunk_check(struct data_storage_s *storage, uint32_t row_len);
int row_insert (struct data_storage_s *storage, struct cell_s *cells);
int row_copy(struct data_storage_s *storage_to, struct data_storage_s *storage_from, uint32_t row_id);
int row_update(struct data_storage_s *storage, void **row, uint32_t *row_len, struct accumulator_s *acc, uint32_t acc_len, struct function_s *functions);
int row_null(struct data_storage_s *storage, void **row, uint32_t *row_len);
int row_merge(struct data_storage_s *storage_to, struct data_storage_s *storage_from, struct view_s *view, void **row, uint32_t *row_len);
int row_bucket(struct job_s *job, int storage_id, struct cell_s *cells);
void ms_sort(struct row_meta_s *a, struct row_meta_s *b, int low, int high);

//// INLINE FUNCTIONS

// Helper functions to check for addition/subtraction overflow/underflow
static inline int check_overflow_uint32(uint32_t a, uint32_t b) {
	int overflow = 0;

	if ((a > 0) && (b > UINT_MAX - a))
		overflow = 1;

	if (overflow)
		system_log(LOG_ERR, "Overflow in uint32 for values %u, %u", a, b);

	return overflow;
}

static inline int check_overflow_int32(int32_t a, int32_t b) {
	int overflow = 0;

	if ((a > 0) && (b > INT_MAX - a))
		overflow = 1;
	else if ((a < 0) && (b < INT_MAX - a))
		overflow = 1;

	if (overflow)
		system_log(LOG_ERR, "Overflow in int32 for values %i, %i", a, b);

	return overflow;
}

static inline int check_overflow_uint64(uint64_t a, uint64_t b) {
	int overflow = 0;

	if ((a > 0) && (b > ULONG_MAX - a))
		overflow = 1;

	if (overflow)
		system_log(LOG_ERR, "Overflow in uint64 for values %lu, %lu", a, b);

	return overflow;
}

static inline int check_overflow_int64(int64_t a, int64_t b) {
	int overflow = 0;

	if ((a > 0) && (b > LONG_MAX - a))
		overflow = 1;
	else if ((a < 0) && (b < LONG_MIN - a))
		overflow = 1;

	if (overflow)
		system_log(LOG_ERR, "Overflow in int64 for values %li, %li", a, b);

	return overflow;
}

static inline int check_overflow_double(double a, double b) {
	int overflow = 0;

	if ((a < 0.0) == (b < 0.0) && abs(b) > DBL_MAX - abs(a))
		overflow = 1;

	if (overflow)
		system_log(LOG_ERR, "Overflow in double for values %lf, %lf", a, b);

	return overflow;
}

// Function to get length by data type
// Returns -1 for variable width data types (e.g., char)
static inline int32_t get_datatype_len(int col, int32_t orig) {
	switch(col) {
		case DEXEC_DATA_LONG:
		case DEXEC_DATA_DOUBLE:
		case DEXEC_DATA_TIMESTAMP:
			return EIGHT_BYTES;
		case DEXEC_DATA_DECIMAL:
			return DECIMAL_SIGN + DECIMAL_LEN * DECIMAL_CHUNK;
		case DEXEC_DATA_VARCHAR:
		case DEXEC_DATA_VARBINARY:
			return orig;
		default:
			system_log(LOG_ERR, "Unsupported data type %u", col);
			return -1;
	}
}

// Function to get string (printed) length by data type
// Returns -1 for variable width data types (e.g., char)
static inline int get_datatype_strlen(int col) {
	switch(col) {
		case DEXEC_DATA_LONG:
		case DEXEC_DATA_DOUBLE:
			return 24;				// 20 digits, sign, decimal point
		case DEXEC_DATA_DECIMAL:
			return 100;				// 96 digits, sign, decimal point
		case DEXEC_DATA_TIMESTAMP:
			//return 12;			// 10 digits, sign, decimal point
			return 24;				// MySQL format YYYY-DD-MM HH:MM:SS.mmm
		case DEXEC_DATA_VARCHAR:
		case DEXEC_DATA_VARBINARY:
			return -1;			
		default:
			system_log(LOG_ERR, "Unsupported data type %u", col);
			return -1;
	}
}

// Function to check if an element "column" is present in the array "supported"
static inline int is_supported(int column, int *supported, int size) {
	int i;
	for (i=0; i<size; i++) {
		if (column == supported[i])
			return 1;
	}
	return 0;
}

// Compare two schemas: returns 1 if schemas are equal, 0 otherwise
static inline int schema_cmp(struct data_storage_s *a, struct data_storage_s *b) {
	uint32_t i;

	if (a->schema_len != b->schema_len)
		return 0;

	for (i=0; i < a->schema_len; i++) {
		if (a->schema[i] != b->schema[i])
			return 0;
	}

	return 1;
}

// Function to build view and row meta for a single row
// Needs the storage and a pointer to the beginning of the row
static inline void build_meta_row(struct data_storage_s *storage, void *row, struct view_s *view, struct row_meta_s *row_meta) {
	uint32_t offset = 0, i, distinct_len;
	int len, key_len;

	// We need to always create row_meta even if passed NULL, because the offset for the view depends on it
	if (row_meta) {
		// Get a pointer to the beginning of row
		row_meta->data = row;

		// Get row length
		row_meta->len = extract_int32(row);
		offset += FOUR_BYTES;

		// Extract node ID
		row_meta->node_id = extract_int32(row + offset);
		offset += FOUR_BYTES;
	}
	else
		offset = 2 * FOUR_BYTES;

	// Extract row key
	key_len = extract_int32(row + offset);
	offset += FOUR_BYTES;

	if (key_len) {
		if (row_meta) {
			row_meta->key_len = key_len;
			row_meta->key = row + offset;
		}
		offset += key_len;
	}

	// Set a pointer to the beginning of cell data and its length (used when creating hash and full row key)
	if (row_meta) {
		row_meta->cells = row + offset;
		row_meta->cells_len = row_meta->len - offset;
	}

	if (! view)
		return;

	// Walk along the row, picking up pointers to column values
	for (i=0; i < storage->schema_len; i++) {
		// Get NULL flag
		view[i].null = row + offset;
		offset += ONE_BYTE;

		len = get_datatype_len(storage->schema[i], -1);

		if (len > 0) {
			// Fixed-width columns (no size in binary format)
			view[i].data = row + offset;
			view[i].len = len;
			offset += len;
		}
		else {
			// Variable-width columns: first, read length
			view[i].len = extract_int32(row + offset);
			offset += FOUR_BYTES;

			if (view[i].len) {
				// If length > 0
				view[i].data = row + offset;
				offset += view[i].len;
			}
			else 
				// If length == 0 (for VARCHAR/VARBINARY NULL values)
				view[i].data = NULL;
		}

		// Check if we have a DISTINCT key
		distinct_len = extract_int32(row + offset);
		offset += FOUR_BYTES;
		if (distinct_len) {
			view[i].distinct = row + offset;
			offset += distinct_len;
		}
		else
			view[i].distinct = NULL;
	}
}

// Obtain a NULL cell for a given data type
static inline void cell_null (uint32_t datatype, struct cell_s *cell) {
	cell->data = NULL;
	cell->len = get_datatype_len(datatype, 0);
	cell->null = 1;
}

// Link a cell to another using its view
static inline void cell_link (struct cell_s *cell, struct view_s *view) {
	cell->data = view->data;
	cell->len = view->len;
	cell->null = *(view->null);
}

