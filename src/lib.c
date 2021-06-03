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

// libdexeclient.so
#include "common.h"
#include "ieee754.h"
#include "functions-common.h"
#include "fs.h"

// PRIVATE METHODS

// Process a chunk into rows
void process_chunk(struct dexec_listener_s *listener) {
	uint32_t i, rows, row_len, offset=MESSAGE_HEADER_SIZE;
	uint64_t chunk_len, queue_id;
	char type, *buffer, *chunk;

	// Block until there is data in the chunks FIFO
	while(! fifo_read(listener->fifo_chunks, (void **)&chunk, &queue_id, &chunk_len, &type))
		usleep(1000);

	// Load the chunk
	if (type == DEXEC_TYPE_RAM)
		// Nothing to do, chunk is already in RAM
		;
	else if (type == DEXEC_TYPE_QUEUE)
		// Chunk from FS Queue, fetch it and increment RAM usage until we process it
		fs_read(listener->queue, queue_id, (void **)&chunk, &chunk_len);
	else {
		// DEXEC_TYPE_END - end of the queue, push EOF
		fifo_write_ram(listener->fifo_rows, NULL, DEXEC_MESSAGE_END);
		return;
	}

	// Get the number of rows in the message
	rows = extract_int32(chunk + offset);
	offset += FOUR_BYTES;

	// Push the rows to the queue
	for (i=0; i<rows; i++) {
		// Get the row length
		row_len = extract_int32(chunk + offset);

		// Copy full row, incl. length
		buffer = mem_alloc(row_len);
		memcpy(buffer, chunk + offset, row_len);

		// Push to FIFO
		if (fifo_write_ram(listener->fifo_rows, (void *) buffer, DEXEC_MESSAGE_DATA) > 0) {
			listener->error = 1;
			free(buffer);
			break;
		}

		// Move to next row
		offset += row_len;
	}

	free(chunk);
}


// Listener main function
void *listener_main(void *arg) {
	int i, mode, res, new_conn, remote_in_len=sizeof(struct sockaddr_in);
	uint64_t queue_id;
	struct sockaddr_in remote_in;
	struct dexec_listener_s *listener = (struct dexec_listener_s *) arg;
	struct message5_s *message5;
	fd_set sock_set_read, sock_set_active;

	// Detach ourselves so that our resources may be freed on exit
	pthread_detach(pthread_self());

	// Initialize the set of active sockets
	FD_ZERO (&sock_set_active);
	FD_SET (listener->socket, &sock_set_active);

	// Clean socket storage
	listener->ss = mem_alloc(FD_SETSIZE * sizeof(struct socket_storage_s));
	for (i = 0; i < FD_SETSIZE; ++i)
		clear_ss(listener->ss, DEXEC_CLEAN_RESET);

	// Main receiver loop
	while(1) {
		// Activate the full set of sockets and block until we have something to read
		sock_set_read = sock_set_active;

/*
		for (i = 0; i < FD_SETSIZE; ++i) {
			if (FD_ISSET (i, &sock_set_read))
				printf("Socket %u found in our set\n", i);
		}
*/
		if ((res = select (FD_SETSIZE, &sock_set_read, NULL, NULL, NULL)) < 0) {
			//printf("Unable to read from sockets: %s. Exiting.\n", strerror(errno));
			exit (EXIT_FAILURE);
		}

		// Service all the sockets with input pending
		for (i = 0; i < FD_SETSIZE; ++i) {
			if (FD_ISSET (i, &sock_set_read)) {
				if (i == listener->socket) {
					// New connection on the original socket
					new_conn = accept (listener->socket, (struct sockaddr *) &remote_in, &remote_in_len);
					if (new_conn < 0) {
						//printf("Unable to read from new connection.\n");
						continue;
					}

					//printf ("New connection from host %s:%u (socket %u)\n", inet_ntoa (remote_in.sin_addr), ntohs (remote_in.sin_port), new_conn);

					// Add to our FD set so that we may read the first packet (message0)
					FD_SET (new_conn, &sock_set_active);
					listener->ss[new_conn].connected = 1;

					continue;
				}

				// Data on any other socket
				if (read_data(i, &listener->ss[i]) < 0) {
					FD_CLR (i, &sock_set_active);
					close (i);
					clear_ss(&listener->ss[i], DEXEC_CLEAN_FREE);
					listener->ss[i].connected = 0;
					continue;
				}

				// See if we have to read more data
				if (! listener->ss[i].message_complete)
					continue;

				// Process message by type
				switch(listener->ss[i].type) {
					case DEXEC_MESSAGE_CONNECT:
						clear_ss(&listener->ss[i], DEXEC_CLEAN_FREE);
						break;

					case DEXEC_MESSAGE_DATA:
						// Push chunk to the queue
						if (mem_usage.used < mem_usage.threshold) {
							// Push as RAM chunk, account the usage
							fifo_write_ram(listener->fifo_chunks, listener->ss[i].buffer, listener->ss[i].size);
							mem_usage.used += listener->ss[i].size;
							mode = DEXEC_CLEAN_RESET;
						}
						else {
							// Init queue for INPUT storage if needed
							if (! listener->queue)
								listener->queue = fs_init(DEXEC_FS_QUEUE, syscall(SYS_gettid), 0);

							if (queue_write(listener->queue, listener->ss[i].buffer, listener->ss[i].size, &queue_id)) {
								// Error writing to queue; continue with the memory chunk, account it
								//printf("Unable to use FS queue; continuing from RAM\n");
								fifo_write_ram(listener->fifo_chunks, listener->ss[i].buffer, listener->ss[i].size);
								mem_usage.used += listener->ss[i].size;
								mode = DEXEC_CLEAN_RESET;
							}
							else {
								// Enqueue the ID from the FS Queue
								fifo_write_queue(listener->fifo_chunks, queue_id, listener->ss[i].size);
								mode = DEXEC_CLEAN_FREE;
							}
						}

						// NB: Clear socket storage with reset
						clear_ss(&listener->ss[i], mode);
						break;

					case DEXEC_MESSAGE_ERROR:
						listener->error = 1;

						// Push to rows FIFO: pointer to the full packet (incl. header) as the processing function will expect it
						fifo_write_ram(listener->fifo_rows, (void *) listener->ss[i].buffer, DEXEC_MESSAGE_ERROR);

						// Process as ERROR message
						message5 = process_message5(listener->ss[i].buffer);

						// Send a KILL message
						dexec_kill_job(listener->nodes, listener->nodes_len, &message5->job_id[0]);

						free(message5);
						break;

					case DEXEC_MESSAGE_END:
						// Remove socket as we don't expect anything more over it
						FD_CLR (i, &sock_set_active);
						close (i);
						clear_ss(&listener->ss[i], DEXEC_CLEAN_FREE);
						listener->ss[i].connected = 0;

						listener->senders --;

						// If this was the last sender, push END to chunks queue
						if (! listener->senders)
							fifo_write_msg(listener->fifo_chunks, DEXEC_TYPE_END);
						
						break;

					default:
						// Unknown message received, exit
						listener->error = 1;
						break;
				}

				if ((! listener->senders) || (listener->error))
					break;
			}
		}

		if ((! listener->senders) || (listener->error))
			break;
	} // while (1)

	// Clean-up
	dexec_listener_close(listener);

	return NULL;
}

// PUBLIC METHODS

// Create a listener thread that will collect the result of a job
int dexec_listener_init(struct dexec_listener_s *listener) {
	struct sockaddr_in local_in, tmp_addr;
	int size;

	listener->error = 0;
	listener->ss = NULL;
	listener->fifo_chunks = NULL;
	listener->fifo_rows = NULL;
	listener->queue = NULL;
	listener->nodes = NULL;
	listener->nodes_len = 0;
	listener->senders = 0;

	// Create a socket for a random port (set port to 0) and bind to it
	if ((listener->socket = init_tcp_socket2(&local_in, NULL, 0, 1)) < 0)
		return DEXEC_ERROR_NETWORK;

	// Listen to the socket
	if (listen (listener->socket, 1) < 0)
		return DEXEC_ERROR_NETWORK;

	// Get the port number
	size = sizeof(struct sockaddr_in);
	if (getsockname(listener->socket, (struct sockaddr *)&tmp_addr, &size) < 0)
		return DEXEC_ERROR_NETWORK;
	listener->port = ntohs(tmp_addr.sin_port);

	// Create FIFOs
	listener->fifo_chunks = fifo_new();
	listener->fifo_rows = fifo_new();

	return DEXEC_OK;
}

// Init a random TCP socket that will be used for the listener
int dexec_listener_start(struct dexec_listener_s *listener) {
	// Create a listening thread
	if (pthread_create(&listener->thread_id, NULL, listener_main, (void *) listener) < 0)
		return DEXEC_ERROR_SYSTEM;

	return DEXEC_OK;
}

// Clean up a listener thread
// NB: We keep this a separate call to make it easier to clean up a thread on external signal
int dexec_listener_close(struct dexec_listener_s *listener) {
	int i;

	for (i = 0; i < FD_SETSIZE; ++i) {
		clear_ss(&listener->ss[i], DEXEC_CLEAN_FREE);
		close(listener->ss[i].socket);
	}
	free(listener->ss);

	close(listener->socket);
}

// Clean up a listener thread
int dexec_listener_end(struct dexec_listener_s *listener) {
	int i;

	if (listener->nodes)
		free(listener->nodes);

	fifo_free(listener->fifo_rows, NULL);
	fifo_free(listener->fifo_chunks, NULL);
	fs_destroy(listener->queue, DEXEC_CLEAN_FREE);
}

// Submit a job to one or more worker nodes
int dexec_submit_job(char *ep, int ep_len, uint32_t *hosts, int hosts_len, uint64_t *job_id) {
	struct sockaddr_in remote_in[hosts_len];
	char *message0, *message1, *message4;
	int i, tcp_port, sockets[hosts_len], bytes0, bytes1, bytes4, ret=DEXEC_OK;
	uint32_t tiers;

	// Read the number of tiers from execution plan
	tiers = extract_int32(ep);

	// Prepare message type 0
	message0 = prepare_message0(&bytes0, job_id, tiers);

	// Prepare message type 1
	message1 = mem_alloc(MESSAGE_HEADER_SIZE + ep_len);
	prepare_header(message1, &bytes1, DEXEC_MESSAGE_JOB, MESSAGE_HEADER_SIZE + ep_len);
	memcpy(message1 + MESSAGE_HEADER_SIZE, ep, ep_len);
	bytes1 += ep_len;

	// Prepare message type 4 (header only - it has no payload)
	message4 = mem_alloc(MESSAGE_HEADER_SIZE);
	prepare_header(message4, &bytes4, DEXEC_MESSAGE_END, MESSAGE_HEADER_SIZE);

	// Decide which TCP port to use
	tcp_port = get_tcp_port();

	// Loop around all workers and send them the messages
	for (i=0; i < hosts_len; i++) {
		// Init worker socket
		if ((sockets[i] = init_tcp_socket(&remote_in[i], hosts[i], tcp_port, 0)) < 0) {
			ret = DEXEC_ERROR_NETWORK;
			break;
		}

		// Connect to worker
		if (0 > connect (sockets[i], (struct sockaddr *) &remote_in[i], sizeof (struct sockaddr))) {
			close(sockets[i]);
			ret = DEXEC_ERROR_NETWORK;
			break;
		}

		// Write to worker
		if (write_to_socket (sockets[i], message0, bytes0) < 0) {
			close(sockets[i]);
			ret = DEXEC_ERROR_NETWORK;
			break;
		}
		if (write_to_socket (sockets[i], message1, bytes1) < 0) {
			close(sockets[i]);
			ret = DEXEC_ERROR_NETWORK;
			break;
		}
		if (write_to_socket (sockets[i], message4, bytes4) < 0) {
			close(sockets[i]);
			ret = DEXEC_ERROR_NETWORK;
			break;
		}

		close(sockets[i]);
	}

	// Clean up
	free(message0);
	free(message1);
	free(message4);

	return ret;
}

// Kill a job
int dexec_kill_job(uint32_t *hosts, int hosts_len, uint64_t *job_id) {
	struct sockaddr_in remote_in[hosts_len];
	char *message3;
	int i, tcp_port, sockets[hosts_len], bytes3, ret=DEXEC_OK;
	uint32_t tiers;

	// Prepare message type 3
	message3 = prepare_message3(&bytes3, job_id);

	// Decide which TCP port to use
	tcp_port = get_tcp_port();

	// Loop around all workers and send them the messages
	for (i=0; i < hosts_len; i++) {
		// Init worker socket
		if ((sockets[i] = init_tcp_socket(&remote_in[i], hosts[i], tcp_port, 0)) < 0) {
			ret = DEXEC_ERROR_NETWORK;
			break;
		}

		// Connect to worker
		if (0 > connect (sockets[i], (struct sockaddr *) &remote_in[i], sizeof (struct sockaddr))) {
			close(sockets[i]);
			ret = DEXEC_ERROR_NETWORK;
			break;
		}

		// Write to worker
		if (write_to_socket (sockets[i], message3, bytes3) < 0) {
			close(sockets[i]);
			ret = DEXEC_ERROR_NETWORK;
			break;
		}

		close(sockets[i]);
	}

	// Clean up
	free(message3);

	return ret;
}

// Parse the data of a row into MySQL format
int dexec_rnd_next(struct dexec_listener_s *listener, int *schema, int schema_len, char **mysql_row, int *mysql_row_len, char **error_msg) {
	unsigned char null;
	int i, j, len;
	double d;
	uint32_t cell_len, mysql_cell_len, value_len, offset = 0, mysql_offset = 0, ui_one = 1;
	uint64_t ui64, i64, queue_id, row_len = 0;
	char type, *row = NULL, value[128], value_tmp[128], *value_p, one=1, zero=0;
	struct decimal_s *decimal;
	struct message5_s *message5;

	*mysql_row = NULL;
	*mysql_row_len = 0;

	// If we don't have a row in the row FIFO, wait for a chunk and process it;
	// If chunk queue is empty, return EOF
	if (fifo_is_empty(listener->fifo_rows))
		process_chunk(listener);

	// Block until there is data in the FIFO
	while(! fifo_read(listener->fifo_rows, (void **)&row, &queue_id, &row_len, &type))
		usleep(1000);

	// Return HA_ERR_END_OF_FILE if we reached the end of the queue
	if (row_len == DEXEC_MESSAGE_END)
		return DEXEC_HA_ERR_END_OF_FILE;

	// Check length (NB: lengths below 8 bytes are message type flags!)
	if (row_len == DEXEC_MESSAGE_ERROR) {
		// Process as ERROR message
		message5 = process_message5(row);

		// Return our error code + 1000 to put it outside HA_ERR codes (120-167)
		// and perpare an error message which the handler can store and return to MySQL when asked.
		if (error_msg && message5->error_msg) {
			*error_msg = mem_alloc(strlen(message5->error_msg) + 64);
			sprintf(*error_msg, "[Node ID: %i] %s", message5->node_id, message5->error_msg);
		}
		if (message5->error_msg)
			free (message5->error_msg);

		free(message5);
		free(row);

		return 1000 + message5->error;
	}

	// Process as DATA message
	// Skip ROW_LEN (4 bytes), NODE_ID (4 bytes), ROW_KEY (16 bytes)
	offset += FOUR_BYTES + FOUR_BYTES + 2 * EIGHT_BYTES;

	for (i=0; i < schema_len; i++) {
		// Get NULL flag for cell
		null = extract_char(row + offset);
		offset += ONE_BYTE;

		// Get datatype length for cell
		len = get_datatype_len(schema[i], -1);
		if (len > 0)
			cell_len = len;
		else {
			cell_len = extract_int32(row + offset);
			offset += FOUR_BYTES;
		}

		// Convert to ASCII - as MySQL likes it
		// Attempt to use only DExec datatypes: auto conversion for float - might raise a MySQL warning
		// and auto guess for DATE/TIME/DATETIME based on length
		// MySQL cell format when not NULL: <INT(column index from zero)><BYTE(0)><INT(value length)><VALUE>
		// MySQL cell format when NULL: <INT(column index from zero)><BYTE(1)><INT(0)>

		// Handle NULL values
		if (null) {
			mysql_cell_len = FOUR_BYTES + 1 + FOUR_BYTES;

			// Allocate space
			if (! i) 
				*mysql_row = mem_alloc(mysql_cell_len);
			else 
				*mysql_row = realloc(*mysql_row, *mysql_row_len + mysql_cell_len);

			// Fill in
			memcpy(*mysql_row + mysql_offset, &one, 1);
			mysql_offset += 1;

			memcpy(*mysql_row + mysql_offset, &ui_one, FOUR_BYTES);
			mysql_offset += FOUR_BYTES;

			*mysql_row_len += mysql_cell_len;

			// Jump over value
			offset += cell_len;

			continue;
		}

		switch(schema[i]) {
			case DEXEC_DATA_LONG:
				i64 = extract_int64(row + offset);
				sprintf(&value[0], "%li", i64);
				value_len = strlen(&value[0]);
				value_p = &value[0];
				break;

			case DEXEC_DATA_DOUBLE:
				i64 = extract_int64(row + offset);
				d = unpack754_64(i64);
				sprintf(&value[0], "%0.16f", d);		// Use %0.8f for FLOAT
				value_len = strlen(&value[0]);
				value_p = &value[0];
			break;

			case DEXEC_DATA_DECIMAL:
				decimal = decimal_from_blob(row + offset);
				decimal_to_str(decimal, &value[0]);
				value_len = strlen(&value[0]);
				value_p = &value[0];
				free(decimal);
			break;

			case DEXEC_DATA_TIMESTAMP:
				// Extract int and detect MySQL datatype based on length
				ui64 = extract_int64(row + offset);
				sprintf(&value_tmp[0], "%lu", ui64);

				switch(strlen(&value_tmp[0])) {
					// NB: We ignore milliseconds for MySQL 5.1 and always print them as ".0"
					case 9:
						sprintf(&value[0], "HH:MM:SS.0");
						memcpy(&value[0], &value_tmp[0], 2);
						memcpy(&value[3], &value_tmp[2], 2);
						memcpy(&value[6], &value_tmp[4], 2);
						break;
					case 17:
						sprintf(&value[0], "YYYY-MM-DD HH:MM:SS.0");
						memcpy(&value[0], &value_tmp[0], 4);
						memcpy(&value[5], &value_tmp[4], 2);
						memcpy(&value[8], &value_tmp[6], 2);
						// Detect if this a date-only
						if ( ! memcmp(&value_tmp[8], "00", 2) && ! memcmp(&value_tmp[10], "00", 2) && ! memcmp(&value_tmp[12], "00", 2)) {
							memset(&value[10], '\0', 1);
							break;
						}
						memcpy(&value[11], &value_tmp[8], 2);
						memcpy(&value[14], &value_tmp[10], 2);
						memcpy(&value[17], &value_tmp[12], 2);
						break;
					default:
						if (*mysql_row)
							free(*mysql_row);
						*mysql_row = NULL;
						*mysql_row_len = 0;
						return DEXEC_HA_ERR_CRASHED_ON_USAGE;
				}

				value_len = strlen(&value[0]);
				value_p = &value[0];
				break;

			case DEXEC_DATA_VARCHAR:
				value_p = row + offset;
				value_len = strlen(value_p);		// NULL-terminated, skip trailing NULLs as MySQL does not like them
				break;

			case DEXEC_DATA_VARBINARY:
				value_p = row + offset;
				value_len = cell_len;
				break;

			default:
				if (*mysql_row)
					free(*mysql_row);
				*mysql_row = NULL;
				*mysql_row_len = 0;
				free(row);
				return DEXEC_HA_ERR_CRASHED_ON_USAGE;
		}

		// Calc MySQL cell length
		mysql_cell_len = FOUR_BYTES + 1 + FOUR_BYTES + value_len;
		
		// Allocate space
		if (! i) 
			*mysql_row = mem_alloc(mysql_cell_len);
		else 
			*mysql_row = realloc(*mysql_row, *mysql_row_len + mysql_cell_len);

		// Write to MySQL row
		memcpy(*mysql_row, &i, FOUR_BYTES);
		mysql_offset += FOUR_BYTES;

		memcpy(*mysql_row + mysql_offset, &zero, 1);
		mysql_offset += 1;

		memcpy(*mysql_row + mysql_offset, &value_len, FOUR_BYTES);
		mysql_offset += FOUR_BYTES;

		memcpy(*mysql_row + mysql_offset, value_p, value_len);
		mysql_offset += value_len;

		*mysql_row_len += mysql_cell_len;
		offset += cell_len;
	}

	free(row);

	return DEXEC_OK;
}

// Fetch error text by its code
// NB: MySQL cannot be pushed an error string; it fetches error string by error code
int dexec_error(int error, char **error_msg) {
	if (! error_msg)
		return DEXEC_OK;

	// NB: "error" is our internal error code + 1000
	*error_msg = mem_alloc(strlen(error_map[error - 1000]) + 1);
	memcpy(*error_msg, error_map[error - 1000], strlen(error_map[error - 1000]));
	return DEXEC_OK;
}

