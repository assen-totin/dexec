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

const char *ident = "DExec";

// Kill MySQL job by its connection ID
int kill_mysql(int connection_id) {
	MYSQL_RES *result;
	MYSQL_ROW row;
	MYSQL *con;
	int i;
	char q[18];

	system_log(LOG_DEBUG, "Killing MySQL query for connection ID %u", connection_id);

	if ((con = mysql_init(NULL)) == NULL) {
		system_log(LOG_ERR, "mysql_init() failed");
		return DEXEC_ERROR_MYSQL;
	}

	if (mysql_real_connect(con,
			getenv("MAMMOTHDB_NODE_DB_HOST"),
			getenv("MAMMOTHDB_NODE_DB_USER"), 
			getenv("MAMMOTHDB_NODE_DB_PASS"), 
			"", 
			0, 
			getenv("MAMMOTHDB_NODE_DB_SOCKET"), 
			0) == NULL) {
		system_log(LOG_ERR, "Unable to connect to MySQL: %s", mysql_error(con));
		mysql_close(con);
		mysql_thread_end();
		return DEXEC_ERROR_MYSQL;
	}

	if (connection_id) {
		snprintf(&q[0], 17, "KILL %u", connection_id);

		if (mysql_query(con, &q[0])) {
			system_log(LOG_ERR, "MySQL query [%s] failed: %s", &q[0], mysql_error(con));
			mysql_close(con);
			mysql_thread_end();
			return DEXEC_ERROR_MYSQL;
		}
	}

	// Free our and system resources
	mysql_close(con);
	mysql_thread_end();

	system_log(LOG_INFO, "Killed MySQL query for connection ID %u", connection_id);

	return DEXEC_OK;
}

// Init DMem masks
void init_btree_mask() {
	int i;

	// 32-bit
	btree_mask32 = mem_alloc(BTREE_DEPTH_32 * BTREE_DEPTH_32);
	for (i=0; i<BTREE_DEPTH_32; i++)
		*(btree_mask32 + i * FOUR_BYTES) = 1 << i;

	// 128-bit - as 2x64 bit
	btree_mask128 = mem_alloc(BTREE_DEPTH_128 * BTREE_DEPTH_128);
	for (i=0; i < BTREE_DEPTH_128 / 2; i++) 
		*(btree_mask128 + (2 * i) * EIGHT_BYTES) = 1L << i;
	for (i=BTREE_DEPTH_128 / 2; i < BTREE_DEPTH_128; i++) 
		*(btree_mask128 + (2 * i + 1) * EIGHT_BYTES) = 1L << i;
}

// Init a thread set slot
void init_slot(struct thread_set_s *thread_set, int index) {
	thread_set->thread_id = 0;
	thread_set->busy = 0;
	thread_set->index = index;
	thread_set->job_id[0] = 0;
	thread_set->job_id[1] = 0;
	thread_set->tier = -1;
}

// Search for a free slot to store a new thread
int get_free_slot(struct thread_set_s *thread_set, int thread_set_len) {
	int i, slot;

	// Check if there is a slot which can be reused
	for (i=0; i<thread_set_len; i++) {
		if (! thread_set[i].busy) {
			thread_set[i].busy = 1;

			system_log(LOG_DEBUG, "Found free slot %u", i);

			thread_set[i].thread_id = 0;
			thread_set[i].tid = 0;

			return i;
		}
	}

	// If no slot can be vacated, expand the set
	system_log(LOG_DEBUG, "Unable to find a free slot, expanding the thread set with %u new slots", THREAD_SET_STEP);

	//FIXME: Start with enough (100? 1000?) slots so that we don't realloc - because threads keep a pointer to their slots!
	thread_set = mem_realloc((void *)thread_set, (thread_set_len + THREAD_SET_STEP) * sizeof(struct thread_set_s));
	for (i=thread_set_len; i<thread_set_len + THREAD_SET_STEP; i++)
		init_slot(&thread_set[i], i);

	slot = thread_set_len;
	thread_set_len += THREAD_SET_STEP;

	system_log(LOG_DEBUG, "Thread set now has %u slots", thread_set_len);

	return slot;
}

// Check if we have a thread for a given job ID (and optionally tier)
int get_slot(uint64_t job0, uint64_t job1, int32_t tier) {
	int i, match = 0;
	for (i=0; i<thread_set_len; i++) {
		if ((job0 == thread_set[i].job_id[0]) && (job1 == thread_set[i].job_id[1])) {
			// If tier is defined, check for it also
			if (tier > -1) {
				if (tier == thread_set[i].tier)
					match = 1;
			}
			else
					match = 1;

			if (match) {
				system_log(LOG_DEBUG, "Thread %u (%lu) found in slot %i for job ID %016lx%016lx tier %i", thread_set[i].tid, thread_set[i].thread_id, i, job0, job1, tier);
				return i;
			}
		}
	}

	return -1;
}

void init_storage(struct data_storage_s *storage, int index, struct message1_s *message1, uint32_t slot) {
	int i, use_hash = 0;
	uint32_t *schema;

	clear_data_storage(storage, DEXEC_CLEAN_RESET);

	// Schema
	switch (index) {
		case DEXEC_STORAGE_INPUT:
			schema = message1->tiers[0].input_schema;
			storage->schema_len = message1->tiers[0].input_schema_len;
			break;
		case DEXEC_STORAGE_OUTPUT:
			schema = message1->tiers[0].output_schema;
			storage->schema_len = message1->tiers[0].output_schema_len;
			break;
		default:
			system_log(LOG_ERR, "Unsupported storage index in init_storage: %i", index);
	}

	storage->schema = mem_alloc(storage->schema_len * FOUR_BYTES);
	for (i=0; i < storage->schema_len; i++)
		storage->schema[i] = schema[i];

	// GROUP Key
	storage->key_len = message1->tiers[0].key_len;
	if (message1->tiers[0].key_len)
		storage->key = mem_alloc(message1->tiers[0].key_len * FOUR_BYTES);
	for (i=0; i < message1->tiers[0].key_len; i++)
		storage->key[i] = message1->tiers[0].key[i];

	// Columns with DISTINCT key
	storage->distinct_len = message1->tiers[0].distinct_len;
	if (message1->tiers[0].distinct_len)
		storage->distinct = mem_alloc(message1->tiers[0].distinct_len * FOUR_BYTES);
	for (i=0; i < message1->tiers[0].distinct_len; i++)
		storage->distinct[i] = message1->tiers[0].distinct[i];

	// Key policy: we always calculate a 128-bit hash. This might change in the future, though.

	// Slot and index (needed to init FS layer on-demand later)
	storage->slot = slot;
	storage->index = index;
}

// Main thread
int main(int argc, char **argv) {
	// System variables
	struct sigaction new_action, old_action;

	// Socket variables
	struct sockaddr_in local_in, remote_in;
	socklen_t addr_len_in; 
	int tcp_port, new_conn;
	int pipefd[2];
	fd_set sock_set_read, sock_set_active;

	// Helper valiables
	int slot, i, j, have_thread = 0, res, mode, status;
	char *res_p;

	// Message variables
	struct message0_s *message0;
	struct message1_s *message1;
	struct message3_s *message3;

	// Job variables
	struct job_s *job;

	// FS queue
	uint64_t queue_id;

	//// INITIALISE

	// Clear global variables
	tcp_socket = 0;
	log_level = 0;
	thread_set_len = 0;

	// Init daemon
	init_daemon(ident, 1);

	// Get our node ID and host IP address
	get_node_id();

	// Init the TCP socket
	tcp_port = get_tcp_port();

	tcp_socket = init_tcp_socket2(&local_in, NULL, tcp_port, 1);

	if (tcp_socket < 0) {
		system_log(LOG_CRIT, "Unable to bind to TCP port %u. Exiting.", tcp_port);
		exit(EXIT_FAILURE);
	}

	if (listen (tcp_socket, 1) < 0) {
		system_log(LOG_CRIT, "Unable to listen to TCP port %u. Exiting.", tcp_port);
		exit(EXIT_FAILURE);
	}

	system_log(LOG_NOTICE, "Listening on TCP socket %u", tcp_port);

	// Calculate useful sizes
	addr_len_in = sizeof(remote_in);

	// Init signal handler
	init_signal_handler(&old_action, &new_action);

	// Init DMem masks
	init_btree_mask();

	// Init memory usage tracker
	mem_usage.used = 0;
	mem_usage.threshold = MAX_RAM;
	if (getenv("MAMMOTHDB_DEXEC_RAM"))
		mem_usage.threshold = atol(getenv("MAMMOTHDB_DEXEC_RAM"));

	max_rows_per_chunk = MAX_ROWS_PER_CHUNK;
	if (getenv("MAMMOTHDB_DEXEC_ROWS"))
		max_rows_per_chunk = atol(getenv("MAMMOTHDB_DEXEC_ROWS"));

	// Initialize the set of active sockets
	FD_ZERO (&sock_set_active);
	FD_SET (tcp_socket, &sock_set_active);

	// Init thread set with slots for 10 concurrent threads
	thread_set_len = THREAD_SET_STEP;
	thread_set = mem_alloc(THREAD_SET_STEP * sizeof(struct thread_set_s));
	for (i=0; i<thread_set_len; i++)
		init_slot(&thread_set[i], i);

	// Init socket storage
	ss = mem_alloc(FD_SETSIZE * sizeof(struct socket_storage_s));
	for (i = 0; i < FD_SETSIZE; ++i) {
		clear_ss(&ss[i], DEXEC_CLEAN_RESET);
		ss[i].thread_slot = -1;
	}

	// Init key for thread-local storage
	if ((res = pthread_key_create(&tls_thread_slot, NULL)) > 0) {
		system_log(LOG_CRIT, "Failed to create keys in thread local storage: %u", res);
		exit(EXIT_FAILURE);
	}

	// Init MySQL client library
	if (mysql_library_init(0, NULL, NULL)) {
		system_log(LOG_CRIT, "Failed to initilaise MySQL client library.");
		exit(EXIT_FAILURE);
	}

	// Check if we can dlopen() the stats client library
	if (! (stats_dl = dlopen ("libstatsclient.so", RTLD_LAZY)))
		system_log(LOG_WARNING, "Unable to find libstatsclient.so, stats will not be sent.");
	else {
		stats_send = dlsym(stats_dl, "stats_send_message_bin");
		if ((res_p = dlerror()) != NULL) {
			system_log(LOG_WARNING, "Unable to find stats_send_message_bin() in libstatsclient, stats will not be sent: %s", res_p);
			stats_send = 0;
		}
	}

// If using SystemD, notify it we are ready
#ifdef HAVE_SYSTEMD
	sd_notify(0, "READY=1");
#endif

	//// MAIN LOOP
	while(1) {
		// Activate the full set of sockets and block until we have something to read
		sock_set_read = sock_set_active;

		for (i = 0; i < FD_SETSIZE; ++i) {
			if (FD_ISSET (i, &sock_set_read))
				system_log(LOG_DEBUG, "Socket %u found in our set", i);
		}

		if ((res = select (FD_SETSIZE, &sock_set_read, NULL, NULL, NULL)) < 0) {
			system_log(LOG_CRIT, "Unable to read from sockets: %s. Exiting.", strerror(errno));
			exit (EXIT_FAILURE);
		}

		// Service all the sockets with input pending
		for (i = 0; i < FD_SETSIZE; ++i) {
			if (FD_ISSET (i, &sock_set_read)) {
				if (i == tcp_socket) {
					// New connection on the original socket
					new_conn = accept (tcp_socket, (struct sockaddr *) &remote_in, &addr_len_in);
					if (new_conn < 0) {
						system_log(LOG_ERR, "Unable to read from new connection.");
						continue;
					}

					system_log(LOG_INFO, "New connection from host %s:%u (socket %u)", inet_ntoa (remote_in.sin_addr), ntohs (remote_in.sin_port), new_conn);

					// Add to our FD set so that we may read the first packet (message0)
					FD_SET (new_conn, &sock_set_active);
					ss[new_conn].connected = 1;

					continue;
				}

				// Data on any other socket
				if (read_data(i, &ss[i]) < 0) {
					FD_CLR (i, &sock_set_active);
					system_log(LOG_INFO, "Closing socket %i", i);
					if ((close (i)) < 0)
						system_log(LOG_ERR, "Error closing socket %i: %s", i, strerror(errno));
					clear_ss(&(ss[i]), DEXEC_CLEAN_FREE);
					ss[i].connected = 0;
					ss[i].thread_slot = -1;
					system_log(LOG_DEBUG, "Cleaned slot %i with socket %i", i, i);
					continue;
				}

				// See if we have to read more data
				if (! ss[i].message_complete)
					continue;

				// Process message by type
				switch(ss[i].type) {
					case DEXEC_MESSAGE_CONNECT:
						system_log(LOG_DEBUG, "Processing message type DEXEC_MESSAGE_CONNECT");

						message0 = process_message0(ss[i].buffer);

						// If we have a slot for this job & tier, simply go on
						if ((slot = get_slot(message0->job_id[0], message0->job_id[1], message0->tier)) >= 0) {
							system_log(LOG_DEBUG, "Slot %i found for job ID %016lx%016lx tier %i, skipping CONNECT message", slot, message0->job_id[0], message0->job_id[1], message0->tier);
							ss[i].thread_slot = slot;
							clear_ss(&ss[i], DEXEC_CLEAN_FREE);
							free(message0);
							break;
						}

						// Seek an empty slot
						slot = get_free_slot(thread_set, thread_set_len);

						// Save the job ID to the thread set slot
						thread_set[slot].job_id[0] = message0->job_id[0];
						thread_set[slot].job_id[1] = message0->job_id[1];
						thread_set[slot].tier = message0->tier;

						// Prepare message queue to this thread
						thread_set[slot].fifo = fifo_new();

						// Link socket storage to thread slot
						ss[i].thread_slot = slot;

						system_log(LOG_INFO, "New job ID %016lx%016lx tier %i from socket %i passed to slot %i", thread_set[slot].job_id[0], thread_set[slot].job_id[1], thread_set[slot].tier, i, thread_set[slot].index);

						clear_ss(&ss[i], DEXEC_CLEAN_FREE);
						system_log(LOG_DEBUG, "Cleaned socket storage %i", i);

						free(message0);

						break;

					case DEXEC_MESSAGE_JOB:
						system_log(LOG_DEBUG, "Processing message type DEXEC_MESSAGE_JOB");

						slot = ss[i].thread_slot;

						// We only process this message type if we haven't done so far from another sender
						if (thread_set[slot].job) {
							system_log(LOG_DEBUG, "We already have the job spec.");
							clear_ss(&ss[i], DEXEC_CLEAN_FREE);
							system_log(LOG_DEBUG, "Cleaned socket storage %i", i);
							break;
						}

						message1 = process_message1(ss[i].buffer);

						// Populate job (functions are taken for tier 0)
						job = mem_alloc(sizeof(struct job_s));
						thread_set[slot].senders_pending = message1->tiers[0].senders_len;
						job->functions_len = message1->tiers[0].functions_len;
						job->functions = message1->tiers[0].functions;
						job->buckets = fs_init(DEXEC_FS_BUCKET, slot, 0);
						job->routes_len = message1->tiers[0].routes_len;
						job->routes = message1->tiers[0].routes;
						job->tiers_len = message1->tiers_len;
						job->tiers = message1->tiers;
						job->job_id[0] = thread_set[slot].job_id[0];
						job->job_id[1] = thread_set[slot].job_id[1];
						job->stats_task_type = message1->tiers[0].stats_task_type;
						job->stats_query_type  = message1->tiers[0].stats_query_type;
						job->stats_ip = message1->tiers[0].stats_ip;
						job->stats_port = message1->tiers[0].stats_port;
						job->error_msg = NULL;
						job->mysql_connection_id = 0;
						job->mysql_connection = NULL;

						// Init storages
						job->storage_len = 2;
						job->storage = mem_alloc(job->storage_len * sizeof(struct data_storage_s));
						init_storage(&job->storage[DEXEC_STORAGE_INPUT], DEXEC_STORAGE_INPUT, message1, slot);
						init_storage(&job->storage[DEXEC_STORAGE_OUTPUT], DEXEC_STORAGE_OUTPUT, message1, slot);

						thread_set[slot].job = job;

						free(message1);		// NB: Functions and routes are linked in the job

						system_log(LOG_INFO, "Init job with %u senders, %u functions, %u routes", thread_set[slot].senders_pending, job->functions_len, job->routes_len);

						// Create new thread
						if (pthread_create(&thread_set[slot].thread_id, NULL,  thread_main, (void *) &thread_set[slot]) < 0) {
							system_log(LOG_CRIT, "Unable to create new thread");
							exit (EXIT_FAILURE);
						}

						// Get ready for next message
						clear_ss(&ss[i], DEXEC_CLEAN_FREE);
						system_log(LOG_DEBUG, "Cleaned socket storage %i", i);

						break;

					case DEXEC_MESSAGE_DATA:
						system_log(LOG_DEBUG, "Processing message type DEXEC_MESSAGE_DATA");

						slot = ss[i].thread_slot;
						job = thread_set[slot].job;

						// Push chunk to the queue
						if (mem_usage.used < mem_usage.threshold) {
							// Push as RAM chunk, account the usage
							fifo_write_ram(thread_set[slot].fifo, ss[i].buffer, ss[i].size);
							mem_usage.used += ss[i].size;
							mode = DEXEC_CLEAN_RESET;
						}
						else {
							// Init queue for INPUT storage if needed
							if (! job->storage[DEXEC_STORAGE_INPUT].queue)
								job->storage[DEXEC_STORAGE_INPUT].queue = fs_init(DEXEC_FS_QUEUE, slot, DEXEC_STORAGE_INPUT);

							if (queue_write(job->storage[DEXEC_STORAGE_INPUT].queue, ss[i].buffer, ss[i].size, &queue_id)) {
								// Error writing to queue; continue with the memory chunk, account it
								system_log(LOG_WARNING, "Unable to use FS queue; continuing from RAM");
								fifo_write_ram(thread_set[slot].fifo, ss[i].buffer, ss[i].size);
								mem_usage.used += ss[i].size;
								mode = DEXEC_CLEAN_RESET;
							}
							else {
								// Enqueue the ID from the FS Queue
								fifo_write_queue(thread_set[slot].fifo, queue_id, ss[i].size);
								mode = DEXEC_CLEAN_FREE;
							}
						}

						// NB: Clear socket storage with reset
						clear_ss(&ss[i], mode);
						system_log(LOG_DEBUG, "Cleaned socket storage %i", i);

						break;

					case DEXEC_MESSAGE_END:
						system_log(LOG_DEBUG, "Processing message type DEXEC_MESSAGE_END");

						slot = ss[i].thread_slot;

						// Remove socket as we don't expect anything more over it
						FD_CLR (i, &sock_set_active);
						system_log(LOG_INFO, "Closing socket %i", i);
						if ((close (i)) < 0)
							system_log(LOG_ERR, "Error closing socket %i: %s", i, strerror(errno));
						clear_ss(&ss[i], DEXEC_CLEAN_FREE);
						ss[i].thread_slot = -1;
						ss[i].connected = 0;
						system_log(LOG_DEBUG, "Cleaned socket storage %i", i);

						thread_set[slot].senders_pending --;

						// Push END to message queue for the thread
						if (thread_set[slot].senders_pending <= 0)
							fifo_write_msg(thread_set[slot].fifo, DEXEC_TYPE_END);

						// TODO: Check if we have a thread for this socket at all and clean up slot here if we don't (unlikely)

						break;

					case DEXEC_MESSAGE_KILL:
						system_log(LOG_DEBUG, "Processing message type DEXEC_MESSAGE_KILL");

						// Read message
						message3 = process_message3(ss[i].buffer);

						// Find all threads for this job ID (on any tier)
						while (1) {
							slot = get_slot(message3->job_id[0], message3->job_id[1], -1);
							if (slot < 0) {
								if (! have_thread)
									system_log(LOG_WARNING, "No thread found for job ID %016lx%016lx, unable to kill it.", message3->job_id[0], message3->job_id[1]);
								break;
							}

							have_thread = 1;

							// Set status and error
							status = thread_set[slot].job->status;
							thread_set[slot].job->status = DEXEC_JOB_STATUS_KILLED;
							thread_set[slot].job->error = DEXEC_ERROR_KILLED;

							// If thread is waiting for MySQL to start fetching, kill the query in MySQL;
							// if past it, no need to do anything - thread will notice the status and exit cleanly
							if (status == DEXEC_JOB_STATUS_QUERY) {
								if (kill_mysql(thread_set[slot].job->mysql_connection_id))
									system_log(LOG_ERR, "Unable to kill MySQL job with connection ID %u", thread_set[slot].job->mysql_connection_id);
							}

							// Free any outstanding network data for this thread
							for (j = 0; j < FD_SETSIZE; ++j) {
								if (ss[j].thread_slot == slot)
									ss[j].connected = 0;
							}

							// Mark slot free so that we don't find it again on next run
							thread_set[slot].job_id[0] = 0;
							thread_set[slot].job_id[1] = 0;
						}

						free(message3);

						// Remove socket as we don't expect anything more over it
						FD_CLR (i, &sock_set_active);
						system_log(LOG_INFO, "Closing socket %i", i);
						if ((close (i)) < 0)
							system_log(LOG_ERR, "Error closing socket %i: %s", i, strerror(errno));
						ss[i].connected = 0;
						clear_ss(&ss[i], DEXEC_CLEAN_FREE);

						system_log(LOG_DEBUG, "Cleaned socket storage %i", i);
							
						break;

					default:
						// Ignore message
						system_log(LOG_WARNING, "Unsupported message type received %u", ss[i].type);
						clear_ss(&ss[i], DEXEC_CLEAN_FREE);
						system_log(LOG_DEBUG, "Cleaned socket storage %i", i);

				}	// switch
			}
		}
	}

	//// NOTREACH
	return 0;
}

