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

#include "common.h"
#include "functions-common.h"
#include "fs.h"

// Function to extact argument for aggregation functions.
// Works for arguments consusting of 4 INTs in format <STORAGE_IN><COL_IN><STORAGE_OUT><COLUMN_OUT>
void extract_arg(struct function_s *function) {
	int offset = 0;

	function->storage_in = extract_int32(function->arg);
	offset += FOUR_BYTES;

	function->col_in = extract_int32(function->arg + offset);
	offset += FOUR_BYTES;

	function->storage_out = extract_int32(function->arg + offset);
	offset += FOUR_BYTES;

	function->col_out = extract_int32(function->arg + offset);
}

// Handle an error in a function
int set_error(struct job_s *job, int error, char *error_msg, ...) {
	int msg_len;
	va_list argptr;

	job->error = error;

	if (job->error_msg)
		free(job->error_msg);
	job->error_msg = mem_alloc(ERROR_MESSAGE_LEN);

	va_start(argptr, error_msg);
	vsnprintf(job->error_msg, ERROR_MESSAGE_LEN, error_msg, argptr);
	va_end(argptr);

	return error;
}

// Function to build view and row meta from storage
void build_meta(struct data_storage_s *storage, int flags) {
	int len;
	uint32_t offset = MESSAGE_HEADER_SIZE, i;
	struct chunk_s *chunk;
	struct view_s *view;
	struct row_meta_s *row_meta;

	if (! storage->chunk)
		return;

	// Get & check the number of rows
	if (! storage->row_count)
		storage->row_count = extract_int32(storage->chunk + offset);
	if (! storage->row_count)
		return;

	// Go to the beginning of the first row
	offset += FOUR_BYTES;

	// Define input view and input row meta arrays
	system_log(LOG_INFO, "Initialising storage view and meta arrays for %u rows, %u columns", storage->row_count, storage->schema_len);

	// Allocate memory for view and row_meta
	if ((flags & DEXEC_META_VIEW) == DEXEC_META_VIEW) {
		if (storage->view)
			free(storage->view);
			storage->view = mem_alloc(storage->row_count * storage->schema_len * sizeof(struct view_s));
	}

	if ((flags & DEXEC_META_ROW) == DEXEC_META_ROW) {
		if (storage->row_meta)
			free(storage->row_meta);
			storage->row_meta = mem_alloc(storage->row_count * sizeof(struct row_meta_s));
	}

	// Fill in the input view and input row meta array - row by row (skip chunk header 
	for (i=0; i < storage->row_count; i++) {
		view = ((flags & DEXEC_META_VIEW) == DEXEC_META_VIEW) ? &storage->view[i * storage->schema_len] : NULL;
		row_meta = ((flags & DEXEC_META_ROW) == DEXEC_META_ROW) ? &storage->row_meta[i] : NULL;
		build_meta_row(storage, storage->chunk + offset, view, row_meta);
		offset += storage->row_meta[i].len;
	}
}

// Calculate the key for a row
static inline void key_calc(struct data_storage_s *storage, int32_t col, struct view_s *view, struct row_meta_s *row_meta, void *res) {
	uint32_t i, buffer_len=0, buffer_free = 0, full_key = 0;
	int offset = 0;
	char *buffer = NULL;

	if (! storage->key && (col < 0))
		return;

	if (col < 0) {
		// GROUP key - check if we may enable full key
		if (storage->schema_len == storage->key_len)
			full_key = 1;
	}

	// If having full key, read data directly from row
	if (full_key) {
		buffer = row_meta->cells;
		buffer_len = row_meta->cells_len;
	}
	else {
		buffer_free = 1;

		// Calculate buffer size: all columns from GROUP key + DISTINCT key, if such is requested
		for (i=0; i < storage->key_len; i++)
			buffer_len += view[storage->key[i]].len;
		if (col >= 0)
			buffer_len += view[col].len;
		buffer = mem_alloc(buffer_len);

		// Copy data to buffer: all columns from GROUP key + DISTINCT key, if such is requested
		for (i=0; i < storage->key_len; i++) {
			memcpy(buffer + offset, view[storage->key[i]].data, view[storage->key[i]].len);
			offset += view[storage->key[i]].len;
		}
		if ((col >= 0) && view[col].len)
			memcpy(buffer + offset, view[col].data, view[col].len);
	}

	// Hash
	if (buffer)
		murmur3((void *)buffer, buffer_len, TRULY_RANDOM_NUMBER, res);

	if (buffer_free)
		free(buffer);
}

// Function to create a new empty chunk (but its future length may already be set in the header)
// Returns pointer to the beginning of the first row
void *chunk_new(struct data_storage_s *storage, uint32_t row_len, int32_t slot) {
	void *offset;
	uint32_t chunk_len, ui32;

	storage->chunk_len = MESSAGE_HEADER_SIZE + FOUR_BYTES + row_len;
	storage->chunk = mem_alloc(storage->chunk_len);
	offset = storage->chunk;

	// If row_len is given, set the number of rows in the chunk to 1
	storage->row_count = (row_len) ? 1 : 0;

	// Expand chunks handlers if no slot to overwrite is given
	if (storage->chunks) {
		if (slot < 0) {
			storage->chunks = mem_realloc(storage->chunks, (storage->chunks_len + 1) * sizeof(struct chunk_handler_s));
			slot = storage->chunks_len;
			storage->chunks_len ++;
		}
	}
	else {
		storage->chunks = mem_alloc(sizeof (struct chunk_handler_s));
		storage->chunks_len = 1;
		slot = 0;
	}

	// Register chunk with handlers
	storage->chunks[slot].data = storage->chunk;
	storage->chunks[slot].type = DEXEC_TYPE_RAM;
	storage->chunk_id = slot;

	// Copy a chunk header to the chunk: magic string, chunk len and message type (2)
	memcpy(offset, MAGIC_STRING, MAGIC_STRING_LEN);
	offset += MAGIC_STRING_LEN;

	ui32 = htonl(storage->chunk_len);
	memcpy(offset, &ui32, FOUR_BYTES);
	offset += FOUR_BYTES;

	ui32 = htonl(2);
	memcpy(offset, &ui32, FOUR_BYTES);
	offset += FOUR_BYTES;

	// Set the row counter
	ui32 = (row_len) ? htonl(1) : 0;
	memcpy(offset, &ui32, FOUR_BYTES);
	offset += FOUR_BYTES;

	// Account memory usage
	mem_usage.used += storage->chunk_len;

	return offset;
}

// Function to create or update a chunk header
static inline void *chunk_expand(struct data_storage_s *storage, uint32_t row_len, uint32_t rows) {
	void *offset = NULL;
	uint32_t ui32;

	// Update chunk length and reallocate
	storage->row_count += rows;
	storage->chunk_len += row_len;
	storage->chunk = mem_realloc(storage->chunk, storage->chunk_len);
	storage->chunks[storage->chunk_id].data = storage->chunk;

	// Jump to chunk size in header, update it
	offset = storage->chunk + MAGIC_STRING_LEN;
	ui32 = htonl(storage->chunk_len);
	memcpy(offset, &ui32, FOUR_BYTES);

	// Jump to row count, increment it
	offset += 2 * FOUR_BYTES;
	ui32 = htonl(storage->row_count);
	memcpy(offset, &ui32, FOUR_BYTES);

	// Zero the new segment
	bzero(storage->chunk + storage->chunk_len - row_len, row_len);

	// Jump to the beginning of the new row
	offset = storage->chunk + storage->chunk_len - row_len;

	// Account memory usage
	mem_usage.used += row_len;

	return offset;
}

// Function to load a chunk from the handlers to the storage
void chunk_load(struct data_storage_s *storage, uint32_t chunk_id) {
	if (storage->chunks[chunk_id].type == DEXEC_TYPE_RAM) {
		storage->chunk = storage->chunks[chunk_id].data;
		storage->chunk_len = storage->chunks[chunk_id].size;
	}
	else {
		// Load from FS Queue, set type to RAM
		fs_read(storage->queue, storage->chunks[chunk_id].id, (void **) &storage->chunk, &storage->chunk_len);
		storage->chunks[chunk_id].type = DEXEC_TYPE_RAM;
	}

	storage->chunk_id = chunk_id;
	storage->row_count = storage->chunks[storage->chunk_id].row_count;
}


// Function to save back a chunk from the storage to the handlers
void chunk_save(struct data_storage_s *storage) {
	storage->chunks[storage->chunk_id].size = storage->chunk_len;
	storage->chunks[storage->chunk_id].row_count = storage->row_count;

	if (mem_usage.used < mem_usage.threshold) {
		storage->chunks[storage->chunk_id].data = storage->chunk;

		// Partial clean of the current chunk - do not free() the chunk as it is linked in the handler
		clear_data_storage(storage, DEXEC_CLEAN_CUSTOM2);
	}
	else {
		// RAM usage is high, save the chunk to disk
		if (! storage->queue)
			storage->queue = fs_init(DEXEC_FS_QUEUE, storage->slot, storage->index);

		queue_write(storage->queue, storage->chunk, storage->chunk_len, &storage->chunks[storage->chunk_id].id);
		storage->chunks[storage->chunk_id].type = DEXEC_TYPE_QUEUE;

		// Partial clean of the current chunk 
		clear_data_storage(storage, DEXEC_CLEAN_CUSTOM);
	}
}

// Function to check if we need to create or expand chunk
void *chunk_check(struct data_storage_s *storage, uint32_t row_len) {
	// Check if we need a new chunk, get a starting point where to write the row
	if (storage->row_count >= max_rows_per_chunk) {
		chunk_save(storage);
		return chunk_new(storage, row_len, -1);
	}
	else {
		// Create first chunk if needed
		if (! storage->chunk)
			return chunk_new(storage, row_len, -1);

		// Update chunk header, get a starting point where to write the row
		return chunk_expand(storage, row_len, 1);
	}
}

// Function to create a new row from an array of cells
int row_new(struct data_storage_s *storage, struct cell_s *cells, void **row, uint32_t *row_len, void **key) {
	uint32_t i, j, ui32;
	uint64_t ui64;
	char *offset;
	union value_u value;

	// Row length consists of the row length in bytes, node ID (4 bytes) and GROUP key (4 bytes key length and N bytes key)
	*row_len = FOUR_BYTES + FOUR_BYTES + FOUR_BYTES;
	if (storage->key)
		*row_len += SIXTEEN_BYTES;

	// Calc the size of the data that the row will hold (for variable width columns, use the actual cell length)
	for (i=0; i< storage->schema_len; i++) {
		*row_len += ONE_BYTE;						// NULL flag

		// Add the real cell length and additional FOUR_BYTES for length of VARCHAR/VARBINARY
		*row_len += get_datatype_len(storage->schema[i], (FOUR_BYTES + cells[i].len));

		// Add a per-column DISTINCT key if specified
		*row_len += FOUR_BYTES;
		if (is_supported(i, storage->distinct, storage->distinct_len))
			*row_len += SIXTEEN_BYTES;
	}

	// Create an empty row
	*row = mem_alloc(*row_len);
	offset = *row;

	// Set row length
	ui32 = htonl(*row_len);
	memcpy(offset, &ui32, FOUR_BYTES);
	offset += FOUR_BYTES;

	// Set node ID
	ui32 = htonl(node_id);
	memcpy(offset, &ui32, FOUR_BYTES);
	offset += FOUR_BYTES;

	// Set the row key in the chunk: row key length and placeholder for row key
	ui32 = (storage->key) ? htonl(SIXTEEN_BYTES) : 0;
	memcpy(offset, &ui32, FOUR_BYTES);
	offset += FOUR_BYTES;
	if (storage->key) {
		bzero(offset, SIXTEEN_BYTES);
		offset += SIXTEEN_BYTES;
	}

	// Loop over the provided array and fill in the chunk
	for (i=0; i < storage->schema_len; i++) {
		// NULL flag
		memcpy(offset, &cells[i].null, ONE_BYTE);
		offset += ONE_BYTE;

		// Cell value
		switch(storage->schema[i]) {
			case DEXEC_DATA_VARCHAR:
			case DEXEC_DATA_VARBINARY:
				// Cell length (even for NULL cells!)
				ui32 = htonl(cells[i].len);
				memcpy(offset, &ui32, FOUR_BYTES);
				offset += FOUR_BYTES;

				// NB: No break here!

			case DEXEC_DATA_LONG:
			case DEXEC_DATA_DOUBLE:
			case DEXEC_DATA_DECIMAL:
			case DEXEC_DATA_TIMESTAMP:
				if (! cells[i].null)
					memcpy(offset, cells[i].data, cells[i].len);
				break;

			default:
				system_log(LOG_ERR, "Unsupported data type %u", storage->schema[i]);
				free(*row);
				*row = NULL;
				*row_len = 0;
				return DEXEC_ERROR_UNSUPPORTED;
		}

		offset += cells[i].len;

		// Cell DISTINCT key, if any
		if (is_supported(i, storage->distinct, storage->distinct_len)) {
			ui32 = htonl(SIXTEEN_BYTES);
			memcpy(offset, &ui32, FOUR_BYTES);
			offset += FOUR_BYTES;
			bzero(offset, SIXTEEN_BYTES);
			offset += SIXTEEN_BYTES;
		}
		else {
			ui32 = 0;
			memcpy(offset, &ui32, FOUR_BYTES);
			offset += FOUR_BYTES;
		}							
	}

	// Build meta for this row
	struct view_s view[storage->schema_len];
	struct row_meta_s row_meta;
	build_meta_row(storage, *row, &view[0], &row_meta);

	// Calc and set the row key
	if (storage->key)
		key_calc(storage, -1, &view[0], &row_meta, row_meta.key);

	// Calc and set DISTINCT keys
	for (i=0; i < storage->distinct_len; i++)
		key_calc(storage, storage->distinct[i], &view[0], &row_meta, view[storage->distinct[i]].distinct);

	if (key)
		*key = row_meta.key;

	return DEXEC_OK;
}

// Function to insert new row in a storage.
// Accepts an array of pointers (double pointer) matching storage schema.
// Each pointer points to a cell value in the row (strings must be NULL terminated!)
int row_insert (struct data_storage_s *storage, struct cell_s *cells) {
	uint32_t i, j, row_len, ui32;
	uint64_t ui64;
	int error;
	void *row, *offset;
	union value_u value;

	// Obtain a row from cells
	if (error = row_new(storage, cells, &row, &row_len, NULL))
		return error;

	// Check if we need a new chunk, get a starting point where to write the row
	offset = chunk_check(storage, row_len);

	// Write the row to chunk
	memcpy(offset, row, row_len);

	free(row);

	return DEXEC_OK;
}

// Function to copy a whole row to another storage
//NB: Only use this function if input and output schemas are equal!
int row_copy(struct data_storage_s *storage_to, struct data_storage_s *storage_from, uint32_t row_id) {
	char *offset; 

	offset = chunk_check(storage_to, storage_from->row_meta[row_id].len);

	memcpy(offset, storage_from->row_meta[row_id].data, storage_from->row_meta[row_id].len);

	return DEXEC_OK;
}

// Function to update a row
int row_update(struct data_storage_s *storage, void **row, uint32_t *row_len, struct accumulator_s *acc, uint32_t acc_len, struct function_s *functions) {
	int i, c, error=DEXEC_OK;
	uint32_t bytes, diff, offset, ui32;
	uint64_t ui64;
	struct view_s *view;
	struct row_meta_s *row_meta;

	// Create view for new row
	view = mem_alloc(storage->schema_len * sizeof(struct view_s));
	row_meta = mem_alloc(storage->schema_len * sizeof(struct row_meta_s));
	build_meta_row(storage, *row, view, row_meta);

	// Update cells in the new row as per the function list - loop around accumulators
	for (i=0; i < acc_len; i++) {
		// Get cell position 
		c = functions[i].col_out;

		// Set NULL byte properly
		if (acc[i].null) {
			*(view[c].null) = 1;
			continue;
		}

		*(view[c].null) = 0;

		// Loop around functions
		switch(storage->schema[functions[i].col_out]) {
			case DEXEC_DATA_LONG:
				//system_log(LOG_DEBUG, "Updating cell value (long): %li", acc[i].value.l);
				ui64 = htobe64(acc[i].value.l);
				memcpy(view[c].data, &ui64, EIGHT_BYTES);
				break;
			case DEXEC_DATA_DOUBLE:
				//system_log(LOG_DEBUG, "Updating cell value (double): %lf", acc[i].value.d);
				ui64 = htobe64(pack754_64(acc[i].value.d));
				memcpy(view[c].data, &ui64, EIGHT_BYTES);
				break;
			case DEXEC_DATA_TIMESTAMP:
				ui64 = htobe64(acc[i].value.l);
				memcpy(view[c].data, &ui64, EIGHT_BYTES);
				//char *tstr = timestamp_to_str(acc[i].value.t, TIMESTAMP_CONVERT_DATETIME_S);
				//system_log(LOG_INFO, "Updating cell value (timestamp): %s", tstr);
				//free(tstr);
				break;
			case DEXEC_DATA_DECIMAL:
				//decimal_to_str(acc[i].value.dec, &value.s[0]);
				//system_log(LOG_DEBUG, "Updating cell value (decimal): %s", &value.s[0]);
				decimal_to_blob(acc[i].value.dec, view[c].data);
				free(acc[i].value.dec);
				break;
			case DEXEC_DATA_VARCHAR:
				// If result length is longer than current lenght, expand the row
				diff = acc[i].s_len - view[c].len;
				if (diff > 0) {
					// Calculate how many bytes we need to move (from end of data to end of row)
					// NB: Use the NULL byte pointer as reference because data pointer might be NULL
					offset = (void *)view[c].null + ONE_BYTE + FOUR_BYTES - *row;
					bytes = *row_len - offset;

					// Set new cell length in row
					ui32 = htonl(view[c].len + diff);
					memcpy(view[c].null + ONE_BYTE, &ui32, FOUR_BYTES);

					// Set new row length in row
					ui32 = htonl(row_meta->len + diff);
					memcpy(row_meta->data, &ui32, FOUR_BYTES);

					// Reallocate current row
					*row_len += diff;
					*row = mem_realloc(*row, *row_len);

					// Move data
					memmove(*row + offset + diff, *row + offset, bytes);

					// Rebuild meta
					build_meta_row(storage, *row, view, row_meta);
				}

				// Copy value
				memcpy(view[c].data, acc[i].value.s, acc[i].s_len);
				free(acc[i].value.s);

				break;

			default:
				system_log(LOG_ERR, "row_update: Unsupported data type: %u", storage->schema[functions[i].col_out]);
				error = DEXEC_ERROR_UNSUPPORTED;
		}
	}

	free(view);
	free(row_meta);

	return error;
}

// Add a row to bucket
int row_bucket(struct job_s *job, int storage_id, struct cell_s *cells) {
	int error;
	void *key = NULL, *row;
	unsigned char bucket_id;
	uint32_t row_len;

	// Build row
	if (error = row_new(&job->storage[storage_id], cells, &row, &row_len, &key))
		return error;

	if (! key) {
		free(row);
		system_log(LOG_ERR, "Unable to add new row to bucket - no key found");
		return DEXEC_ERROR_SYSTEM;
	}

	// Get bucket and copy row
	bucket_id = *((unsigned char *)key);
	error = bucket_append(&job->buckets->buckets[bucket_id], row, row_len);

	// Update bucket info: row counter
	job->buckets->buckets[bucket_id].row_count ++;

	free(row);

	return error;
}

// Obtain a NULL row for a given storage
int row_null(struct data_storage_s *storage, void **row, uint32_t *row_len) {
	int res, i;
	struct cell_s *cells = NULL;

	// Create an empty row of NULL cells
	cells = mem_alloc(storage->schema_len * sizeof(struct cell_s));
	for (i=0; i < storage->schema_len; i++)
		cell_null(storage->schema[i], &cells[i]);

	res = row_new(storage, cells, row, row_len, NULL);

	free(cells);

	return res;
}

// Obtain a row by merging a row from one storage to the schema of another
int row_merge(struct data_storage_s *storage_to, struct data_storage_s *storage_from, struct view_s *view, void **row, uint32_t *row_len) {
	struct cell_s *cells;
	int res, i;

	cells = mem_alloc(storage_to->schema_len * sizeof(struct cell_s));
	for (i=0; i < storage_to->schema_len; i++) {
		if (storage_to->schema[i] == storage_from->schema[i])
			// Column types match
			cell_link(&cells[i], &view[i]);
		else 
			cell_null(storage_to->schema[i], &cells[i]);
	}

	res = row_new(storage_to, cells, row, row_len, NULL);

	free(cells);

	return res;
}

// Merge-sort functions: merge (only called from sort, hence inline it)
static inline void ms_merge(struct row_meta_s *a, struct row_meta_s *b, int low, int mid, int high) {
	int l1, l2, i, key_len;
	void *key1, *key2;

	for(l1 = low, l2 = mid + 1, i = low; l1 <= mid && l2 <= high; i++) {
		// Compare actual row keys
		if (memcmp(a[l1].key, a[l2].key, a[l1].key_len) > 0)
			b[i] = a[l2++];
		else
			b[i] = a[l1++];			
	}
   
	while(l1 <= mid)
		b[i++] = a[l1++];

	while(l2 <= high)
		b[i++] = a[l2++];

	for(i = low; i <= high; i++)
		a[i] = b[i];
}

// Merge-sort functions: sort
void ms_sort(struct row_meta_s *a, struct row_meta_s *b, int low, int high) {
	int mid;
   
	if(low >= high)
		return;

	mid = (low + high) / 2;
	ms_sort(a, b, low, mid);
	ms_sort(a, b, mid + 1, high);
	ms_merge(a, b, low, mid, high);
}

