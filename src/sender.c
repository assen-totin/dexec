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

static inline void sender_close(struct job_s *job, struct route_s *route, int mode);

static inline struct route_s *get_route(struct job_s *job, unsigned char bucket_id) {
	int i;

	for (i=0; i < job->routes_len; i++) {
		if ((bucket_id >= job->routes[i].from) && (bucket_id <= job->routes[i].to))
			return &job->routes[i];
	}

	return &job->routes[0];
}

static inline int send_message0(struct job_s *job, struct route_s *route) {
	int res, size;
	char *m0_buf;

	if (route->socket < 0)
		return DEXEC_ERROR_NETWORK;

	m0_buf = prepare_message0(&size, &job->job_id[0], job->tiers_len -1);
	system_log(LOG_DEBUG, "Wrote %i bytes to message type 0", size);

	res = write_to_socket (route->socket, m0_buf, size);
	free(m0_buf);
	if (res < 0) {
		system_log(LOG_ERR, "Unable to sent message type DEXEC_MESSAGE_CONNECT to socket %i", route->socket);
		sender_close(job, route, DEXEC_SENDER_NONE);
		return DEXEC_ERROR_NETWORK;
	}

	system_log(LOG_INFO, "Sent message type DEXEC_MESSAGE_CONNECT: %i bytes prepared, %i bytes sent", size, res);
	return DEXEC_OK;
}

static inline int send_message1(struct job_s *job, struct route_s *route) {
	int i, j, size, res, bytes1=MESSAGE_HEADER_SIZE;
	char *m1_buf;

	if (route->socket < 0)
		return DEXEC_ERROR_NETWORK;

	// If we're the last tier, skip this message
	if (job->tiers_len < 2) {
		system_log(LOG_INFO, "Skipping message type DEXEC_MESSAGE_JOB as we're the last tier");
		return DEXEC_OK;
	}		

	// Move to tiers[1] here!
	m1_buf = prepare_message1(&size, &job->tiers[1], job->tiers_len - 1);
	system_log(LOG_DEBUG, "Wrote %i bytes to message type 1", size);

	// Write message to server
	res = write_to_socket (route->socket, m1_buf, size);
	free(m1_buf);
	if (res < 0) {
		system_log(LOG_ERR, "Unable to sent message type DEXEC_MESSAGE_JOB to socket %i", route->socket);
		sender_close(job, route, DEXEC_SENDER_NONE);
		return DEXEC_ERROR_NETWORK;
	}

	system_log(LOG_INFO, "Sent message type DEXEC_MESSAGE_JOB: %i bytes prepared, %i bytes sent", size, res);
	return DEXEC_OK;
}

static inline int send_message2(struct job_s *job, int storage_id, struct route_s *route, uint32_t *rows_sent, uint32_t *bytes_sent) {
	uint32_t res;
	char stats_payload[48];
	
	if (route->socket < 0)
		return DEXEC_ERROR_NETWORK;

	// If chunk is empty, don't send it
	if (! job->storage[storage_id].row_count)
		return DEXEC_OK;

	// If this is the very first time se send (push) data, send a stats message to record the time
#ifdef HAVE_MDB_STATS
	if (job->stats_task_type && stats_send && job->stats_push) {
		job->stats_push = 1;
		if ((*(stats_send))(node_id, STATS_EVENT_WN_PUSH_START, job->stats_task_type, &job->task_id[0], job->stats_query_type, &job->job_id[0], NULL, NULL, job->stats_ip, job->stats_port) < 0)
			system_log(LOG_ERR, "Error sending stats message.");
	}
#endif

	if ((res = write_to_socket (route->socket, job->storage[storage_id].chunk, job->storage[storage_id].chunk_len)) < 0) {
		system_log(LOG_ERR, "Unable to sent message type DEXEC_MESSAGE_DATA to socket %i", route->socket);
		sender_close(job, route, DEXEC_SENDER_NONE);
		return DEXEC_ERROR_NETWORK;
	}

	system_log(LOG_INFO, "Sent message type DEXEC_MESSAGE_DATA with %u rows, %i bytes prepared, %i bytes sent", job->storage[storage_id].row_count, job->storage[storage_id].chunk_len, res);

	*rows_sent += job->storage[storage_id].row_count;
	*bytes_sent += res;

	// Send stats
#ifdef HAVE_MDB_STATS
	if (job->stats_task_type && stats_send) {
		// Set payload: 48 bytes are enough for two longs and a separator
		sprintf(&stats_payload[0], "%u|%u", *rows_sent, *bytes_sent);

		if ((*(stats_send))(node_id, STATS_EVENT_WN_PUSH, job->stats_task_type, &job->task_id[0], job->stats_query_type, &job->job_id[0], &stats_payload[0], NULL, job->stats_ip, job->stats_port) < 0)
			system_log(LOG_ERR, "Error sending stats message.");
	}
#endif

	return DEXEC_OK;
}

static inline int send_message4(struct job_s *job, struct route_s *route) {
	char *m4_buf;
	int bytes4, res;

	if (route->socket < 0)
		return DEXEC_ERROR_NETWORK;

	// Prepare message (header only - it has no payload)
	m4_buf = mem_alloc(MESSAGE_HEADER_SIZE);
	prepare_header(m4_buf, &bytes4, DEXEC_MESSAGE_END, MESSAGE_HEADER_SIZE);

	// Write messages to server
	res = write_to_socket (route->socket, m4_buf, bytes4);
	free(m4_buf);
	if (res < 0) {
		system_log(LOG_ERR, "Unable to sent message type DEXEC_MESSAGE_END to socket %i", route->socket);
		sender_close(job, route, DEXEC_SENDER_NONE);
		return DEXEC_ERROR_NETWORK;
	}

	system_log(LOG_INFO, "Sent message type DEXEC_MESSAGE_END: %i bytes prepared, %i bytes sent", bytes4, res);
	return DEXEC_OK;
}

static inline int send_message5(struct job_s *job, struct route_s *route) {
	int i, j, size, res, bytes5=MESSAGE_HEADER_SIZE;
	char *m5_buf;

	if (route->socket < 0)
		return DEXEC_ERROR_NETWORK;

	// Prepare message type 5
	m5_buf = prepare_message5(&size, &job->job_id[0], node_id, job->error, job->error_msg);
	system_log(LOG_DEBUG, "Wrote %i bytes to message type 5", size);

	// Write message to server
	res = write_to_socket (route->socket, m5_buf, size);
	free(m5_buf);
	if (res < 0) {
		system_log(LOG_ERR, "Unable to sent message type DEXEC_MESSAGE_ERROR to socket %i", route->socket);
		sender_close(job, route, DEXEC_SENDER_NONE);
		return DEXEC_ERROR_NETWORK;
	}

	system_log(LOG_INFO, "Sent message type DEXEC_MESSAGE_ERROR: %i bytes prepared, %i bytes sent", size, res);
	return DEXEC_OK;
}

static inline int send_bucket(struct job_s *job, int bucket_id, int storage_id, uint32_t *rows_sent, uint32_t *bytes_sent) {
	int res;
	uint32_t ui32;
	struct route_s *route;
	struct data_storage_s *storage = &job->storage[storage_id];
	struct bucket_s *bucket = &job->buckets->buckets[bucket_id];

	// Check for KILLED status
	if (job->status == DEXEC_JOB_STATUS_KILLED)
		return DEXEC_JOB_STATUS_KILLED;

	// If we have 0 rows in this bucket, skip it
	if (! bucket->row_count)
		return DEXEC_OK;

	// Load bucket as storage chunk
	fs_read(job->buckets, bucket_id, &storage->chunk, &storage->chunk_len);
	storage->row_count = bucket->row_count;
	storage->chunks[0].data = storage->chunk;
	storage->chunks[0].size = storage->chunk_len;

	// Update header for bucket which has size of 0
	ui32 = htonl(storage->chunk_len);
	memcpy(storage->chunk + 4, &ui32, FOUR_BYTES);

	// Update row count for bucket which has value of 0
	ui32 = htonl(storage->row_count);
	memcpy(storage->chunk + 12, &ui32, FOUR_BYTES);

	// If we don't have a connected socket on current route, connect one
	route = get_route(job, (unsigned char)bucket_id);
	if (route->socket < 0) {
		if (res = sender_connect(job, route))
			return res;
	}

	// Send the bucket
	res = send_message2(job, storage_id, route, rows_sent, bytes_sent);
	clear_data_storage(storage, DEXEC_CLEAN_CUSTOM);

	// Return result
	return res;
}

static inline int sender_init(struct job_s *job, struct route_s *route) {
	struct sockaddr_in remote_in;

	// Init socket
	if ((route->socket = init_tcp_socket(&remote_in, route->ip, route->port, 0)) < 0) {
		system_log(LOG_ERR, "Unable to create client socket.");
		return DEXEC_ERROR_NETWORK;
	}
	system_log(LOG_INFO, "Sender socket to %08x:%i is %i", route->ip, route->port, route->socket);

	if (0 > connect (route->socket, (struct sockaddr *) &remote_in, sizeof (remote_in))) {
		route->socket = -1;
		system_log(LOG_ERR, "Unable to connect to %08x:%i: %s", route->ip, route->port, strerror(errno));
		return DEXEC_ERROR_NETWORK;
	}

	system_log(LOG_DEBUG, "Sender socket %i connected to %08x:%i", route->socket, route->ip, route->port);
	return DEXEC_OK;
}

int sender_connect(struct job_s *job, struct route_s *route) {
	int error;

	if ((error = sender_init(job, route)) > 0)
		return error;

	if ((error = send_message0(job, route)) > 0)
		return error;

	if ((error = send_message1(job, route)) > 0)
		return error;

	return DEXEC_OK;
}

int sender_next_hop(struct job_s *job, int storage_id, int mode) {
	int bucket_id=0;
	uint32_t i, error, rows_sent=0, bytes_sent=0;

	// Send data
	if (mode == DEXEC_SENDER_DIRECT) {
		// If we don't have a connected socket on route 0, connect one
		if (job->routes[0].socket < 0) {
			if (error = sender_connect(job, &job->routes[0]))
				return error;
		}

		if (job->storage[storage_id].chunk)
			return send_message2(job, storage_id, &job->routes[0], &rows_sent, &bytes_sent);
		else
			system_log(LOG_DEBUG, "Storage %i has no chunk to send, skipping DEXEC_MESSAGE_DATA", storage_id);
	}
	else if (mode == DEXEC_SENDER_CHUNKS) {
		// If we don't have a connected socket on route 0, connect one
		if (job->routes[0].socket < 0) {
			if (error = sender_connect(job, &job->routes[0]))
				return error;
		}

		if (job->storage[storage_id].chunks) {
			// Send from multiple chunks
			for (i=0; i < job->storage[storage_id].chunks_len; i++) {
				// Check for KILLED status
				if (job->status == DEXEC_JOB_STATUS_KILLED)
					return DEXEC_JOB_STATUS_KILLED;

				chunk_load(&job->storage[storage_id], i);
				error = send_message2(job, storage_id, &job->routes[0], &rows_sent, &bytes_sent);
				clear_data_storage(&job->storage[storage_id], DEXEC_CLEAN_CUSTOM);

				// Return on error
				if (error)
					return error;
			}
		}
		else
			system_log(LOG_DEBUG, "Storage %i has no chunks to send, skipping DEXEC_MESSAGE_DATA", storage_id);
	}
	else {
		// Check if we need to create a chunks handler in storage
		if (! job->storage[storage_id].chunks) {
			job->storage[storage_id].chunks = mem_alloc(sizeof (struct chunk_handler_s));
			job->storage[storage_id].chunks_len = 1;
			job->storage[storage_id].chunks[0].type = DEXEC_TYPE_RAM;
			job->storage[storage_id].chunk_id = 0;
		}

		// Send all buckets that we have in the job
		// NB: if all nodes start sending from the first bucket, first node in the routing table will be flooded with data
		// and remaining will stay idle. To avoid this, we first send data to ourselves, then send buckets with larger IDs,
		// then start from zero.
		if (job->routes_len > 1) {
			for (i=0; i < job->routes_len; i++) {
				if (job->routes[i].ip == host_ip) {
					bucket_id = job->routes[i].from;
					break;
				}
			}
		}
		
		for (i=bucket_id; i < BUCKET_COUNT; i++)
			if (error = send_bucket(job, i, storage_id, &rows_sent, &bytes_sent))
				return error;

		for (i=0; i < bucket_id; i++)
			if (error = send_bucket(job, i, storage_id, &rows_sent, &bytes_sent))
				return error;
	}

	return DEXEC_OK;
}

static inline void sender_close(struct job_s *job, struct route_s *route, int mode) {
	int i;

	if (route->socket < 0)
		return;

	// Send END message if asked
	if (mode == DEXEC_SENDER_END)
		send_message4(job, route);

	system_log(LOG_INFO, "Closing socket %i", route->socket);
	if((close(route->socket)) < 0)
		system_log(LOG_ERR, "Unable to close socket %i: %s", route->socket, strerror(errno));
	route->socket = -1;
}

void sender_close_all(struct job_s *job, int mode) {
	int i;

	for (i=0; i<job->routes_len; i++)
		sender_close(job, &job->routes[i], mode);
}

int sender_error(struct job_s *job) {
	int error, close=0;
	struct route_s *route;

	system_log(LOG_INFO, "Sending error message for job ID: %016lx%016lx\n", job->job_id[0], job->job_id[1]);

	// If we are on the last tier and have a connected socket, use it
	if ((job->tiers_len == 1) && (job->routes[0].socket > -1))
		route = &job->routes[0];
	else {
		route = &job->tiers[job->tiers_len - 1].routes[0];
		close = 1;

		if ((error = sender_init(job, route)) > 0)
			return error;

		// MESSAGE TYPE 0
		if ((error = send_message0(job, route)) > 0)
			return error;
	}

	// MESSAGE TYPE 5
	if ((error = send_message5(job, route)) > 0)
		return error;

	// Clean-up
	if (close)
		sender_close(job, route, DEXEC_SENDER_NONE);

	system_log(LOG_INFO, "Sent error message for job ID: %016lx%016lx\n", job->job_id[0], job->job_id[1]);

	return DEXEC_OK;
}

