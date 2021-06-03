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

// Error handler
void error_handler(struct job_s *job) {
	// Set job status
	if (job->status != DEXEC_JOB_STATUS_KILLED) {
		job->status = DEXEC_JOB_STATUS_FAILURE;

		// Send an error message (a KILL message will be broadcast in response)
		sender_error(job);
	}

	system_log(LOG_ERR, "%s", job->error_msg);

	// Clean up
	cleanup_thread(job->status, job->error_msg);
}

// Thread entry point
void *thread_main(void *arg) {
	char *chunk, type;
	unsigned char bucket_id=0;
	uint32_t value_len, i, j, k, row_len;
	uint64_t chunk_len, c, queue_id;
	int res_aggr, res_distinct, error = 0, view_len, new_key = 0, last_row, next_key_same, global_distinct=0, schema_eq;
	void *key=NULL, *row, *offset;
	struct accumulator_s *acc=NULL;
	struct data_storage_s *storage_input, *storage_output;
	struct row_meta_s *row_meta;
	struct view_s *view;
	struct job_s *job;
	union value_u value_tmp;

	// Save a pointer to our slot in thread set to thread local storage (needed for cleanup et al)
	pthread_setspecific(tls_thread_slot, arg);

	// Get argument (our slot index in thread set)
	struct thread_set_s *self = (struct thread_set_s *) arg;

	// Set our TID used in logging
	self->tid = syscall(SYS_gettid);

	system_log(LOG_INFO, "Starting new thread ID %u (%lu) from slot %i for job ID %016lx%016lx tier %i", self->tid, self->thread_id, self->index, self->job_id[0], self->job_id[1], self->tier);

	// Set shortcuts to INPUT and OUTPUT storages as well as to job
	job = self->job;
	storage_input = &job->storage[DEXEC_STORAGE_INPUT];
	storage_output = &job->storage[DEXEC_STORAGE_OUTPUT];

#ifdef HAVE_MDB_STATS
	// If stats is enabled, init stats and send first message
	if (stats_send && job->stats_task_type) {
		// Clear flags
		job->stats_push = 0;

		// Record current time for stats
		gettimeofday(&job->stats_tv, NULL);

		system_log(LOG_DEBUG, "DEXEC_FUNCTION_STATS: Enabling stats of type %i", job->stats_task_type);

		prepare_task_id(&job->job_id[0], node_id, job->stats_task_type, (void *) &job->task_id[0]);

		if ((*(stats_send))(node_id, STATS_EVENT_WN_EXEC_START, job->stats_task_type, &job->task_id[0], job->stats_query_type, &job->job_id[0], NULL, &job->stats_tv, job->stats_ip, job->stats_port) < 0)
			system_log(LOG_ERR, "Error sending stats message.");
		job->stats_tv = (struct timeval){0};

		// If we're on last tier, send a message that DISt part of the task begins
		if (job->tiers_len == 1) {
			if ((*(stats_send))(node_id, STATS_EVENT_NN_DIST_START, job->stats_task_type, &job->task_id[0], job->stats_query_type, &job->job_id[0], NULL, NULL, job->stats_ip, job->stats_port) < 0)
				system_log(LOG_ERR, "Error sending stats message.");
		}
	}
#endif

	job->status = DEXEC_JOB_STATUS_RUNNING;

	// Run a data retrieval function if we have such (it will optionally also send the data)
	for (i=0; i < job->functions_len; i++) {
		switch (job->functions[i].function) {
			 	case DEXEC_FUNCTION_MYSQL:
					error = function_mysql(job, job->functions[i].arg, job->functions[i].arg_len);
				break;
		}
	}

	// If there has been an error (incl. killed query), clean up and exit the thread
	if (error)
		error_handler(job);

	// Extract function arguments; enable global DISTINCT if specified; init BTree instances for DISTINCT functions
	for (i=0; i < job->functions_len; i++) {
		switch (job->functions[i].function) {
			case DEXEC_FUNCTION_GLOBAL_DISTINCT:
				global_distinct = 1;
				// NB: No break here!

			case DEXEC_FUNCTION_MYSQL:
				break;

			case DEXEC_FUNCTION_SUM_DISTINCT:
			case DEXEC_FUNCTION_AVG_DISTINCT:
			case DEXEC_FUNCTION_COUNT_DISTINCT:
				// Clear BTree handlers
				job->functions[i].btree = NULL;
				// NB: No break here!

			default:
				extract_arg(&job->functions[i]);

				if (! storage_output->chunk)
					chunk_new(storage_output, 0, -1);
		}
	}

	// Determine view length
	view_len = storage_input->schema_len * sizeof(struct view_s);

	// Check if input and output schemas are equal
	schema_eq = schema_cmp(storage_output, storage_input);

	// If we have no GROUP key, prepare a new accumulator set
	if (! storage_input->key_len) {
		acc = mem_alloc(job->functions_len * sizeof(struct accumulator_s));
		for (i=0; i < job->functions_len; i++) 
			acc[i].null = 1;
	}

	// Receive loop
	while (1) {
		// Check for KILLED status
		if (job->status == DEXEC_JOB_STATUS_KILLED)
			error_handler(job);

		// Block until there is data in the FIFO
		while(! fifo_read(self->fifo, (void **)&chunk, &queue_id, &chunk_len, &type)) {
			// Check for KILLED status
			if (job->status == DEXEC_JOB_STATUS_KILLED)
				error_handler(job);

			usleep(10000);		// 10 ms
		}

		if (type == DEXEC_TYPE_RAM)
			// Nothing to do, chunk is already in RAM
			;
		else if (type == DEXEC_TYPE_QUEUE)
			// Chunk from FS Queue, fetch it and increment RAM usage until we process it
			fs_read(storage_input->queue, queue_id, (void **)&chunk, &chunk_len);
		else
			// DEXEC_TYPE_END - end of the queue, quit
			break;

		storage_input->chunk = chunk;
		storage_input->chunk_len = chunk_len;
		storage_input->chunk_id = 0;

		if (! storage_input->chunks) {
			storage_input->chunks = mem_alloc(sizeof (struct chunk_handler_s));
			storage_input->chunks_len = 1;
		}

		storage_input->chunks[storage_input->chunk_id].type = DEXEC_TYPE_RAM;
		storage_input->chunks[storage_input->chunk_id].data = chunk;
		storage_input->chunks[storage_input->chunk_id].size = chunk_len;

		// Build view and row meta for the chunk
		build_meta(storage_input, DEXEC_META_ROW);

		// If we have a GROUP key, the whole chunk goes to one bucket, so copy it all there
		if (storage_input->key_len) {
			bucket_id = *((unsigned char *)storage_input->row_meta[0].key);
			bucket_append(&job->buckets->buckets[bucket_id], storage_input->chunk + 16, storage_input->chunk_len - 16);
			job->buckets->buckets[bucket_id].row_count += storage_input->row_count;
		}
		else {
			// Loop around the rows, feed each to buckets
			for (i=0; i < storage_input->row_count; i++) {
				// Get key, find a bucket and append the row
				if (storage_input->row_meta[i].key)
					bucket_id = *((unsigned char *)storage_input->row_meta[i].key);
				error = bucket_append(&job->buckets->buckets[bucket_id], storage_input->row_meta[i].data, storage_input->row_meta[i].len);
				if (error) {
					set_error(job, error, "Error appending to bucket %i", bucket_id);
					break;
				}

				job->buckets->buckets[bucket_id].row_count++;
			}
		}

		// Remove previous chunk
		clear_data_storage(storage_input, DEXEC_CLEAN_CUSTOM);

		if (error)
			error_handler(job);
	}	// end of receive loop

	// Load and process buckets one by one
	for (i=0; i < BUCKET_COUNT; i++) {
		// Check for KILLED status
		if (job->status == DEXEC_JOB_STATUS_KILLED)
			error_handler(job);

		// If we have 0 rows in this bucket, skip it
		if (! job->buckets->buckets[i].row_count)
			continue;

		// Load bucket as storage chunk
		fs_read(job->buckets, i, &storage_input->chunk, &storage_input->chunk_len);

		// Set the row count in the chunk header (NB only in the storage, not in the chunk; same for chunk len above!)
		storage_input->row_count = job->buckets->buckets[i].row_count;

		// Build meta for the newly loaded chunk; we only need the row meta as we'll sort it first
		build_meta(storage_input, DEXEC_META_ROW);

		// Sort meta by row key
		if (storage_input->row_count > 1) {
			row_meta = mem_alloc(sizeof(struct row_meta_s) * storage_input->row_count);
			ms_sort(storage_input->row_meta, row_meta, 0, storage_input->row_count - 1);
			free(row_meta);
		}

		// If we have global DISTINCT, run it
		if (global_distinct) {
			if (error = function_distinct(job)) {
				set_error(job, error, "Error in global DISTINCT for bucket %i", i);
				error_handler(job);
			}

			continue;
		}

		// Clear BTrees for DISTINCTS
		for (j=0; j < job->functions_len; j++) {
			switch (job->functions[j].function) {
				case DEXEC_FUNCTION_SUM_DISTINCT:
				case DEXEC_FUNCTION_AVG_DISTINCT:
				case DEXEC_FUNCTION_COUNT_DISTINCT:
					btree_destroy(job->functions[j].btree);
					job->functions[j].btree = btree_init(BTREE_DEPTH_128);
			}
		}

		// Loop rows from bucket for aggregation
		for (j=0; j < storage_input->row_count; j++) {
			// Set few flags to use below
			last_row = ((j + 1) == storage_input->row_count) ? 1 : 0;
			next_key_same = (! last_row && ! memcmp(storage_input->row_meta[j].key, storage_input->row_meta[j+1].key, storage_input->row_meta[j].key_len)) ? 1 : 0;
			if (storage_input->key_len)
				new_key = (key) ? memcmp(key, storage_input->row_meta[j].key, storage_input->row_meta[j].key_len) : 1;

			// Get a view for this row only
			view = mem_alloc(view_len);
			build_meta_row(storage_input, storage_input->row_meta[j].data, view, NULL);

			// Register DISTINCT key (if any) in BTree
			for (k=0; k < job->functions_len; k++) {
				c = job->functions[k].col_in;
				switch (job->functions[k].function) {
					case DEXEC_FUNCTION_SUM_DISTINCT:
					case DEXEC_FUNCTION_AVG_DISTINCT:
					case DEXEC_FUNCTION_COUNT_DISTINCT:
						// We only need the BTree result and do not store anything
						job->functions[k].btree_res = btree_put(job->functions[k].btree, view[c].distinct);
				}
			}

			if (new_key) {
				// Save the key
				key = storage_input->row_meta[j].key;

				// Prepare a new accumulator set
				acc = mem_alloc(job->functions_len * sizeof(struct accumulator_s));

				// Loop around functions
				for (k=0; k < job->functions_len; k++) {
					c = job->functions[k].col_in;
					if ( *((char *)(view[c].null)) )
						// Set NULL flag
						acc[k].null = 1;
					else
						// Init accumulator with current cell's value
						function_init(job, view[c].data, view[c].len, &acc[k], storage_input->schema[job->functions[k].col_in]);
				}

				// If aggregation will continue on next row, jump to it (else below we'll flush accumulators to output)
				if (next_key_same) {
					free(view);
					continue;
				}
			}
			// We have seen this key on the previous row, so run the functions
			else {
				// Loop around functions
				for (k=0; k < job->functions_len; k++) {
					c = job->functions[k].col_in;

					// Ignore NULL cells
					if ( *((char *)(view[c].null)) )
						continue;

					// Run the function
					switch(job->functions[k].function) {
						case DEXEC_FUNCTION_MIN:
							error = function_min(job, view[c].data, view[c].len, &acc[k], storage_input->schema[job->functions[k].col_in]);
							break;

						case DEXEC_FUNCTION_MAX:
							error = function_max(job, view[c].data, view[c].len, &acc[k], storage_input->schema[job->functions[k].col_in]);
							break;

						case DEXEC_FUNCTION_AVG:
							acc[k].count ++;
							// NB: No break here! Continue with SUM()

						case DEXEC_FUNCTION_SUM:
							error = function_sum(job, view[c].data, view[c].len, &acc[k], storage_input->schema[job->functions[k].col_in]);
							break;

						case DEXEC_FUNCTION_COUNT:
							acc[k].count ++;
							break;

						case DEXEC_FUNCTION_AVG_DISTINCT:
							if (job->functions[k].btree_res == BTREE_RESULT_ADDED)
								acc[k].count ++;
							// NB: No break here! Continue with SUM_DISTINCT()

						case DEXEC_FUNCTION_SUM_DISTINCT:
							if (job->functions[k].btree_res == BTREE_RESULT_ADDED)
								error = function_sum(job, view[c].data, view[c].len, &acc[k], storage_input->schema[job->functions[k].col_in]);
							break;

						case DEXEC_FUNCTION_COUNT_DISTINCT:
							if (job->functions[k].btree_res == BTREE_RESULT_ADDED)
								acc[k].count ++;
							break;
					}

					if (error)
						error_handler(job);

				} // end of functions loop

				// If this is not last row and if we either don't have GROUP key or next row has same key, continue to it
				if (! last_row) {
					if (! key || next_key_same) {
						free(view);
						continue;
					}
				}
			}

			// Additional processing of the aggregated result before flushing to output
			for (k=0; k < job->functions_len; k++) {
				switch (job->functions[k].function) {
					case DEXEC_FUNCTION_COUNT:
					case DEXEC_FUNCTION_COUNT_DISTINCT:
						// If we counted certain data types, free accumulator values
						if (! acc[k].null) {
							switch(storage_input->schema[job->functions[k].col_in]) {
								case DEXEC_DATA_DECIMAL:
									free(acc[k].value.dec);
									break;
								case DEXEC_DATA_VARCHAR:
									free(acc[k].value.s);
							}
						}

						// Copy counter to INT value, force result to be not NULL
						acc[k].null = 0;
						acc[k].value.l = acc[k].count;
						break;
					case DEXEC_FUNCTION_AVG:
					case DEXEC_FUNCTION_AVG_DISTINCT:
						// Calculate the average
						// NB: We always set column type to DECIMAL for AVG (like MySQL does)
						// Handle the case when no data has been aggregated (e.g. all values were NULL)
						if (acc[k].count) {
							acc[k].null = 0;
							error = decimal_avg(acc[k].value.dec, acc[k].count, &value_tmp.dec);
							free(acc[k].value.dec);
							acc[k].value.dec = value_tmp.dec;
						}
						break;
				}
			}

			if (error) {
				set_error(job, error, "Error post-processing aggregated values");
				error_handler(job);
			}

			// Obtain a new row
			if (schema_eq) {
				row = mem_alloc(storage_input->row_meta[j].len);
				memcpy(row, storage_input->row_meta[j].data, storage_input->row_meta[j].len);
				row_len = storage_input->row_meta[j].len;
			}
			else if (storage_input->schema_len == storage_output->schema_len) {
				if (error = row_merge(storage_output, storage_input, view, &row, &row_len)) {
					set_error(job, error, "Error merging row");
					error_handler(job);
				}
			}
			else {
				if (error = row_null(storage_output, &row, &row_len)) {
					set_error(job, error, "Error creating NULL row");
					error_handler(job);
				}	
			}
system_log(LOG_DEBUG, "FINAL SUM: %.10f ", acc->value.d);
			error = row_update(storage_output, &row, &row_len, acc, job->functions_len, job->functions);

			// Create or update chunk and obtain pointer to write
			offset = chunk_check(storage_output, row_len);
			memcpy(offset, row, row_len);

			free(view);
			free(row);
			free(acc);
			acc = NULL;

			if (error)
				error_handler(job);
		}	// end of row loop

		// Clear input
		clear_data_storage(storage_input, DEXEC_CLEAN_CUSTOM);

		// If we don't have a GROUP key, continue with next bucket
		if (!key)
			continue;

		// If we have data from aggregation in OUTPUT, send it
		if (storage_output->chunk) {
			chunk_save(storage_output);
			if ((error = sender_next_hop(job, DEXEC_STORAGE_OUTPUT, DEXEC_SENDER_CHUNKS)) > 0)
				error_handler(job);
		}

		// Clear output to make it ready for next bucket
		clear_data_storage(storage_output, DEXEC_CLEAN_CUSTOM);
		free(storage_output->chunks);
		storage_output->chunks = NULL;
		storage_output->chunks_len = 0;
		key = NULL;
	}	// end of bucket loop

	// If we have data from aggregation in OUTPUT (e.g., no GROUP KEY), send it
	if (! key) {
		clear_data_storage(storage_input, DEXEC_CLEAN_CUSTOM);
		if (storage_output->chunk) {
			chunk_save(storage_output);
			if ((error = sender_next_hop(job, DEXEC_STORAGE_OUTPUT, DEXEC_SENDER_CHUNKS)) > 0)
				error_handler(job);
		}
	}

	// Clean up accumulator if we did not have aggregation
	if (acc)
		free(acc);

	// Init connections to routes that we did not send a bucket to (to ensure full mesh of connections)
	for (i=0; i < job->routes_len; i++) {
		if (job->routes[i].socket < 0) {
			if (error = sender_connect(job, &job->routes[i]))
				error_handler(job);
		}
	}

	// Send END message to next tiers and clean up socket
	sender_close_all(job, DEXEC_SENDER_END);

	// Set job status
	job->status = DEXEC_JOB_STATUS_COMPLETE;

#ifdef HAVE_MDB_STATS
	// Send stats if enabled - that we've finished running functions
	if (stats_send && job->stats_task_type) {
		if ((*(stats_send))(node_id, STATS_EVENT_WN_EXEC_END, job->stats_task_type, &job->task_id[0], job->stats_query_type, &job->job_id[0], NULL, NULL, job->stats_ip, job->stats_port) < 0)
			system_log(LOG_ERR, "Error sending stats message.");
	}
#endif

	// Clean up
	cleanup_thread(job->status, job->error_msg);

	return NULL;
}

