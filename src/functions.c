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

// MySQL error handler
int error_mysql(MYSQL *connection, int error) {
	mysql_close(connection);
	mysql_thread_end();
	return error;
}

// Function to retrieve data from MySQL
// The query is passed as the argument after the storage ID
// By convention: if we need further processing, we write to INPUT storage; if we stream directly, write to OUTPUT storage
int function_mysql(struct job_s *job, void *arg, int arg_len) {
	MYSQL_RES *result;
	MYSQL_ROW mysql_row;
	int i, error = 0, num_fields = 0, storage_id, offset = 0, len, mode;
	unsigned long *lengths;
	union value_u value;
	void *row;
	uint32_t row_len;
	struct row_meta_s row_meta;
	char bucket_id;

	mode = extract_int32(arg);
	offset += FOUR_BYTES;
	storage_id = (mode == DEXEC_MYSQL_STREAM) ? DEXEC_STORAGE_OUTPUT : DEXEC_STORAGE_INPUT;
	struct cell_s cells[job->storage[storage_id].schema_len];

	char newarg[arg_len + 1 - offset];
	memcpy(&newarg[0], arg + offset, arg_len - offset);
	newarg[arg_len - offset] = 0;

	system_log(LOG_DEBUG, "DEXEC_FUNCTION_MYSQL: Function starting in mode %u for query: [%s]", mode, &newarg[0]);

	if (job->status = DEXEC_JOB_STATUS_RUNNING)
		job->status = DEXEC_JOB_STATUS_QUERY;

	// Check for supported data type
	int supported[] = {DEXEC_DATA_LONG, DEXEC_DATA_DOUBLE, DEXEC_DATA_DECIMAL, DEXEC_DATA_TIMESTAMP, DEXEC_DATA_VARCHAR, DEXEC_DATA_VARBINARY};
	for (i=0; i < job->storage[storage_id].schema_len; i++) {
		if (! is_supported(job->storage[storage_id].schema[i], &supported[0], sizeof(supported)/sizeof(int)))
			return set_error(job, DEXEC_ERROR_UNSUPPORTED, "DEXEC_FUNCTION_MYSQL: Unsupported data type %u", job->storage[storage_id].schema[i]);
	}

	// Init MySQL 
	if ((job->mysql_connection = mysql_init(NULL)) == NULL)
		return set_error(job, DEXEC_ERROR_MYSQL, "mysql_init() failed");

	// Connect MySQL
	if (mysql_real_connect(job->mysql_connection,
			"localhost",
			getenv("MAMMOTHDB_NODE_DB_USER"), 
			getenv("MAMMOTHDB_NODE_DB_PASS"), 
			"", 
			0, 
			getenv("MAMMOTHDB_NODE_DB_SOCKET"), 
			0) == NULL) {
		set_error(job, DEXEC_ERROR_MYSQL, "Unable to connect to MySQL: %s", mysql_error(job->mysql_connection));
		return error_mysql(job->mysql_connection, DEXEC_ERROR_MYSQL);
	}

	// Get our connection ID (in case the main thread decides to kill us)
	if (mysql_query(job->mysql_connection, "SELECT CONNECTION_ID()")) {
		set_error(job, DEXEC_ERROR_MYSQL, "MySQL query [SELECT CONNECTION_ID()] failed: %s", mysql_error(job->mysql_connection));
		return error_mysql(job->mysql_connection, DEXEC_ERROR_MYSQL);
	}

	if ((result = mysql_store_result(job->mysql_connection)) == NULL) {
		set_error(job, DEXEC_ERROR_MYSQL, "MySQL result is NULL: %s", mysql_error(job->mysql_connection));
		return error_mysql(job->mysql_connection, DEXEC_ERROR_MYSQL);
	}

	mysql_row = mysql_fetch_row(result);
	system_log(LOG_INFO, "MySQL connection ID is %s", mysql_row[0]);

	// Save our the connection ID to our slot in the main thread
	job->mysql_connection_id = atoi(mysql_row[0]);
	system_log(LOG_DEBUG, "Saving MySQL connection ID %u to slot %u", job->mysql_connection_id, job->index);
	mysql_free_result(result);

	// Force UTF8 charset
	if (mysql_set_character_set(job->mysql_connection, "utf8")) {
		set_error(job, DEXEC_ERROR_MYSQL, "Unknown charset specified: utf8");
		return error_mysql(job->mysql_connection, DEXEC_ERROR_MYSQL);
	}

	// Run the main query
	if (mysql_query(job->mysql_connection, &newarg[0])) {
		set_error(job, DEXEC_ERROR_MYSQL, "MySQL query [%s] failed: %s", &newarg[0], mysql_error(job->mysql_connection));
		return error_mysql(job->mysql_connection, DEXEC_ERROR_MYSQL);
	}

	if (job->status = DEXEC_JOB_STATUS_QUERY)
		job->status = DEXEC_JOB_STATUS_EXTRACT;

	// Check result; empty result is probably OK
	if ((result = mysql_use_result(job->mysql_connection)) == NULL) {
		system_log(LOG_ERR, "MySQL result is NULL: %s", mysql_error(job->mysql_connection));
		mysql_free_result(result);
		return error_mysql(job->mysql_connection, DEXEC_OK);
	}

	// If streaming, create first chunk in our designated storage
	if (mode == DEXEC_MYSQL_STREAM)
		chunk_new(&job->storage[storage_id], 0, -1);

	num_fields = mysql_num_fields(result);

	while ((job->status != DEXEC_JOB_STATUS_KILLED) && result && (mysql_row = mysql_fetch_row(result))) {
		lengths = mysql_fetch_lengths(result);

		for(i = 0; i < num_fields; i++) {
			// Get length from schema or actual (add space for NULL byte in VARCHAR)
			len = get_datatype_len(job->storage[storage_id].schema[i], lengths[i]);
			if (job->storage[storage_id].schema[i] == DEXEC_DATA_VARCHAR)
				len ++;
			cells[i].len = len;

			// Handle NULL values
			if (! mysql_row[i]) {
				cells[i].null = 1;
				cells[i].data = NULL;
				if ((job->storage[storage_id].schema[i] == DEXEC_DATA_VARCHAR) || (job->storage[storage_id].schema[i] == DEXEC_DATA_VARBINARY))
					cells[i].len = 0;		// 0 for VARCHAR and VARBINARY
				continue;
			}

			// Clear NULL flag
			cells[i].null = 0;

			// Convert data to chunk format
			cells[i].data = mem_alloc(cells[i].len);

			switch(job->storage[storage_id].schema[i]) {
				case DEXEC_DATA_LONG:				
					*(int64_t *)(cells[i].data) = htobe64(atol(mysql_row[i]));
					break;
				case DEXEC_DATA_DOUBLE:
					*(int64_t *)(cells[i].data) = htobe64(pack754_64(strtod(mysql_row[i], NULL)));
					break;
				case DEXEC_DATA_DECIMAL:
					value.dec = decimal_from_str(mysql_row[i]);
					decimal_to_blob(value.dec, cells[i].data);
					free(value.dec);
					break;
				case DEXEC_DATA_TIMESTAMP:
					// Comply with a misbehavior in MySQL which return NULL for FUNCTION("0000-00-00 00:00:00")
					// ICE is fine and returns "0000-00-00 00:00:00", but we need to stick to MySQL to pass QA tests.
					// See #3997
					if (! memcmp(mysql_row[i], "0000", 4)) {
						cells[i].null = 1;
						cells[i].len = len;
						free(cells[i].data);
						cells[i].data = NULL;
						break;
					}

					*(uint64_t *)(cells[i].data) = htobe64(timestamp_from_str(mysql_row[i]));
					break;
				case DEXEC_DATA_VARCHAR:
				case DEXEC_DATA_VARBINARY:
					memcpy(cells[i].data, mysql_row[i], lengths[i]);
					break;
			}
		}

		// If streaming, create a new row in the output data; else, write to buckets
		if (mode == DEXEC_MYSQL_STREAM)
			error = row_insert(&job->storage[storage_id], &cells[0]);
		else
			error = row_bucket(job, storage_id, &cells[0]);

		// If streaming, send DATA message each MAX_ROWS_PER_CHUNK rows & re-init storage (sender will init self and connect if not connected)
		if (! error && (mode == DEXEC_MYSQL_STREAM) && (job->storage[storage_id].row_count == max_rows_per_chunk)) {
			error = sender_next_hop(job, storage_id, DEXEC_SENDER_DIRECT);
			clear_data_storage(&job->storage[storage_id], DEXEC_CLEAN_CUSTOM);
			chunk_new(&job->storage[storage_id], 0, job->storage[storage_id].chunk_id);
		}

		// Free resources
		for (i = 0; i < num_fields; i++) {
			if (cells[i].data)
				free(cells[i].data);
		}

		if (error)
			break;
	}

	// Free result
	job->mysql_connection_id = 0;
	if (result)
		mysql_free_result(result);
	if (job->mysql_connection) {
		mysql_close(job->mysql_connection);
		job->mysql_connection = NULL;
	}
	mysql_thread_end();	

	// Check if the job has been killed; then check for general error
	if (job->status == DEXEC_JOB_STATUS_KILLED)
		return set_error(job, DEXEC_ERROR_KILLED, "DEXEC_FUNCTION_MYSQL: killed", DEXEC_ERROR_KILLED);
	else if (error)
		return set_error(job, error, "DEXEC_FUNCTION_MYSQL: error %u", error);

	// If streaming, send any remaining rows in the chunk; else, send everything from buckets
	if (mode == DEXEC_MYSQL_STREAM) {
		if (error = sender_next_hop(job, storage_id, DEXEC_SENDER_DIRECT))
			return set_error(job, error, "DEXEC_FUNCTION_MYSQL: error sending direct %u", error);
		clear_data_storage(&job->storage[storage_id], DEXEC_CLEAN_CUSTOM);
	}
	else {
		// Send all buckets (no need to sort them!)
		if (error = sender_next_hop(job, storage_id, DEXEC_SENDER_BUCKETS))
			return set_error(job, error, "DEXEC_FUNCTION_MYSQL: error sending buckets %u", error);

		// Clean up buckets
		fs_destroy(job->buckets, DEXEC_CLEAN_RESET);
	}

	system_log(LOG_DEBUG, "DEXEC_FUNCTION_MYSQL: Function ending.");
	return DEXEC_OK;
}

// Init accumulator
int function_init(struct job_s *job, void *value, uint32_t value_len, struct accumulator_s *acc, int col_type) {
	acc->count = 1;
	uint64_t ui64;
	double d;

	acc->null = 0;

	switch(col_type) {
		case DEXEC_DATA_LONG:
		case DEXEC_DATA_TIMESTAMP:
			acc->value.l = be64toh(*((uint64_t *)value));
			break;

		case DEXEC_DATA_DOUBLE:
			ui64 = be64toh(*(uint64_t *)value);
			d = unpack754_64(ui64);
			acc->value.d = d;
system_log(LOG_DEBUG, "SUM: ADDING TO ACCUMULATOR: %.10f ", d);
			break;

		case DEXEC_DATA_DECIMAL:
			acc->value.dec = decimal_from_blob(value);
			break;

		case DEXEC_DATA_VARCHAR:
			// Init value: copy acc (string is already NULL-terminated and the NULL byte is counted in cell's length)
			acc->value.s = mem_alloc(value_len);
			memcpy(acc->value.s, value, value_len);
			acc->s_len = value_len;
			break;

		default:
			return set_error(job, DEXEC_ERROR_UNSUPPORTED, "FUNCTION_MIN: Unsupported data type: %u", col_type);
	}

	return DEXEC_OK;
}

// Find a minimum value
int function_min(struct job_s *job, void *value, uint32_t value_len, struct accumulator_s *acc, int col_type) {
	struct decimal_s *dec;
	uint32_t len;
	uint64_t ui64;
	double d;

	system_log(LOG_DEBUG, "FUNCTION_MIN: Function starting.");

	// If accumulator is NULL, simply set the current value
	if (acc->null) 
		return function_init(job, value, value_len, acc, col_type);

	switch(col_type) {
		case DEXEC_DATA_LONG:
		case DEXEC_DATA_TIMESTAMP:
			if (be64toh(*((uint64_t *)value)) < acc->value.l)
				acc->value.l = be64toh(*((uint64_t *)value));
			break;

		case DEXEC_DATA_DOUBLE:
			ui64 = be64toh(*(uint64_t *)value);
			d = unpack754_64(ui64);
			if (d < acc->value.d)
				acc->value.d = d;
			break;

		case DEXEC_DATA_DECIMAL:
			dec = decimal_from_blob(value);
			if (decimal_cmp(dec, acc->value.dec) < 0) {
				free(acc->value.dec);
				acc->value.dec = dec;
			}
			else
				free(dec);
			break;

		case DEXEC_DATA_VARCHAR:
			// Find shorter string
			len = (acc->s_len > value_len) ? value_len : acc->s_len;
			//TODO: This should probably become collation-aware via DMem config (with forced UTF-8)
			if (memcmp(value, acc->value.s, len) < 0) {
				free(acc->value.s);
				acc->value.s = mem_alloc(value_len);
				memcpy(acc->value.s, value, value_len);
				acc->s_len = value_len;
			}
			break;

		default:
			return set_error(job, DEXEC_ERROR_UNSUPPORTED, "FUNCTION_MIN: Unsupported data type: %u", col_type);
	}

	system_log(LOG_DEBUG, "FUNCTION_MIN: Function ending.");

	return DEXEC_OK;
}

// Find a maximum value
int function_max(struct job_s *job, void *value, uint32_t value_len, struct accumulator_s *acc, int col_type) {
	struct decimal_s *dec;
	uint32_t len;
	uint64_t ui64;
	double d;

	system_log(LOG_DEBUG, "FUNCTION_MAX: Function starting.");

	// If accumulator is NULL, simply set the current value
	if (acc->null) 
		return function_init(job, value, value_len, acc, col_type);

	switch(col_type) {
		case DEXEC_DATA_LONG:
		case DEXEC_DATA_TIMESTAMP:
			if (be64toh(*((uint64_t *)value)) > acc->value.l)
				acc->value.l = be64toh(*((uint64_t *)value));
			break;

		case DEXEC_DATA_DOUBLE:
			ui64 = be64toh(*(uint64_t *)value);
			d = unpack754_64(ui64);
			if (d > acc->value.d)
				acc->value.d = d;
			break;

		case DEXEC_DATA_DECIMAL:
			dec = decimal_from_blob(value);
			if (decimal_cmp(dec, acc->value.dec) > 0) {
				free(acc->value.dec);
				acc->value.dec = dec;
			}
			else
				free(dec);
			break;

		case DEXEC_DATA_VARCHAR:
			// Find shorter string
			len = (acc->s_len > value_len) ? value_len : acc->s_len;

			//TODO: This should probably become collation-aware via DMem config (with forced UTF-8)
			if (memcmp(value, acc->value.s, len) > 0) {
				free(acc->value.s);
				acc->value.s = mem_alloc(value_len);
				memcpy(acc->value.s, value, value_len);
				acc->s_len = value_len;
			}
			break;

		default:
			return set_error(job, DEXEC_ERROR_UNSUPPORTED, "FUNCTION_MAX: Unsupported data type: %u", col_type);
	}

	system_log(LOG_DEBUG, "FUNCTION_MAX: Function ending.");

	return DEXEC_OK;
}

// Sum elements
int function_sum(struct job_s *job, void *value, uint32_t value_len, struct accumulator_s *acc, int col_type) {
	uint64_t ui64;
	double d;
	struct decimal_s *dec, *dec_tmp;

	system_log(LOG_DEBUG, "FUNCTION_SUM: Function starting.");

	// If accumulator is NULL, simply set the current value
	if (acc->null) 
		return function_init(job, value, value_len, acc, col_type);

	switch(col_type) {
		case DEXEC_DATA_LONG:
			ui64 = be64toh(*(uint64_t *)value);
			if (check_overflow_int64(acc->value.l, ui64))
				return set_error(job, DEXEC_ERROR_OVERFLOW, "FUNCTION_SUM: Overflow for LONG values %li, %li", acc->value.l, ui64);
			acc->value.l += ui64;
			break;

		case DEXEC_DATA_DOUBLE:
			ui64 = be64toh(*(uint64_t *)value);
			d = unpack754_64(ui64);
			if (check_overflow_double(acc->value.d, d))
				return set_error(job, DEXEC_ERROR_OVERFLOW, "FUNCTION_SUM: Overflow for DOUBLE values %f, %f", acc->value.d, d);
			acc->value.d += d;
			break;

		case DEXEC_DATA_DECIMAL:
			dec = decimal_from_blob((char *)value);
			dec_tmp = decimal_new();
			if (decimal_sum(acc->value.dec, dec, dec_tmp)) {
				free(dec);
				free(dec_tmp);
				return set_error(job, DEXEC_ERROR_OVERFLOW, "FUNCTION_SUM: Overflow for DECIMAL");
			}
			decimal_cpy(acc->value.dec, dec_tmp);
			free(dec_tmp);
			free(dec);
			break;

		case DEXEC_DATA_VARCHAR:
		case DEXEC_DATA_VARBINARY:
			// Sum of VARCHARs is always NULL
			acc->value.s = NULL;
			break;

		default:
			return set_error(job, DEXEC_ERROR_UNSUPPORTED, "FUNCTION_SUM: Unsupported data type: %u", col_type);
	}

	system_log(LOG_DEBUG, "FUNCTION_SUM: Function ending.");

	return DEXEC_OK;
}

// Process DISTINCT buckets to only leave the unique entries
int function_distinct(struct job_s *job) {
	int error=DEXEC_OK, offset=MAGIC_STRING_LEN;
	uint32_t i, ui32;
	struct data_storage_s *storage_input, *storage_output;
	struct row_meta_s *row_meta_tmp;

	system_log(LOG_DEBUG, "DEXEC_FUNCTION_DISTINCT: Function starting.");

	storage_input = &job->storage[DEXEC_STORAGE_INPUT];
	storage_output = &job->storage[DEXEC_STORAGE_OUTPUT];

	// Copy rows with unique keys after sorting
	row_copy(storage_output, storage_input, 0);
	for (i=1; i < storage_input->row_count; i++) {
		// Check if current row's key differs from previous
		if (memcmp(storage_input->row_meta[i].key, storage_input->row_meta[i-1].key, storage_input->row_meta[i].key_len))
			row_copy(storage_output, storage_input, i);
	}

	// Send current chunk(s)
	chunk_save(storage_output);
	error = sender_next_hop(job, DEXEC_STORAGE_OUTPUT, DEXEC_SENDER_CHUNKS);

	// Make space for new chunk(s) from the next bucket; 
	// reset the chunks handler, which will be re-created on first row_copy for the next bucket
	clear_data_storage(storage_input, DEXEC_CLEAN_CUSTOM);
	clear_data_storage(storage_output, DEXEC_CLEAN_CUSTOM);
	free(storage_output->chunks);
	storage_output->chunks = NULL;
	storage_output->chunks_len = 0;

	system_log(LOG_DEBUG, "DEXEC_FUNCTION_DISTINCT: Function ending.");

	return error;
}
