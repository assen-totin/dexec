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
#include "lib.h"

// Validate magic
int validate_magic(char *magic) {
	int ret = memcmp(magic, MAGIC_STRING, strlen(MAGIC_STRING));

	if (ret) {
		char buffer[strlen(MAGIC_STRING) + 1];
		memcpy(&buffer[0], magic, strlen(MAGIC_STRING));
		memset(&buffer[strlen(MAGIC_STRING)], '\0', 1);
		system_log(LOG_ERR, "Message magic failed validation: expected %s given %s", MAGIC_STRING, &buffer[0]);
	}

	return ret;
}

// Prepare message header
void prepare_header(char *buffer_out, int *bytes_out, int type, int size) {
	int tmp, offset = 0;

	// Copy magic
	memcpy(buffer_out + offset, MAGIC_STRING, strlen(MAGIC_STRING));
	offset += strlen(MAGIC_STRING);

	// Add size
	if (size)
		tmp = htonl(size);
	else
		tmp = htonl(0);
	memcpy(buffer_out + offset, &tmp, FOUR_BYTES);
	offset += FOUR_BYTES;	

	// Add message type
	tmp = htonl(type);
	memcpy(buffer_out + offset, &tmp, FOUR_BYTES);
	offset += FOUR_BYTES;

	*bytes_out = offset;
}

// Prepare message for type 0 (CONNECT)
char *prepare_message0(int *bytes, uint64_t *job_id, uint32_t tier) {
	char *buffer;
	uint64_t tmp64;
	uint32_t tmp32, offset;

	// Full message size
	*bytes = MESSAGE_HEADER_SIZE + 2 * EIGHT_BYTES + FOUR_BYTES;
	buffer = mem_alloc(*bytes);

	// Add header
	prepare_header(buffer, &offset, DEXEC_MESSAGE_CONNECT, *bytes);

	// Add job ID
	tmp64 = htobe64(job_id[0]);
	memcpy(buffer + offset, &tmp64, EIGHT_BYTES);
	offset += EIGHT_BYTES;
	tmp64 = htobe64(job_id[1]);
	memcpy(buffer + offset, &tmp64, EIGHT_BYTES);
	offset += EIGHT_BYTES;

	// Add tier
	tmp32 = htonl(tier);
	memcpy(buffer + offset, &tmp32, FOUR_BYTES);
	offset += FOUR_BYTES;

	return buffer;
}

// Prepare message for type 1 (JOB)
char *prepare_message1(int *bytes, struct tier_s *tiers, int tiers_len) {
	char *ep, *buffer;
	int ep_size, offset;

	// Prepare an execution plan
	dexec_prepare_execution_plan(&ep, &ep_size, tiers, tiers_len);

	// Create memory for message
	*bytes = MESSAGE_HEADER_SIZE + ep_size;
	buffer = mem_alloc(*bytes);

	// Add header
	prepare_header(buffer, &offset, DEXEC_MESSAGE_JOB, *bytes);
	memcpy(buffer + offset, ep, ep_size);

	if (ep)
		free(ep);

	return buffer;
}

// Prepare execution plan (exported in library, hence the name)
int dexec_prepare_execution_plan(char **ep, int *bytes, struct tier_s *tiers, int tiers_len) {
	char *buffer;
	int i, j, offset = 0;
	uint32_t ui32;

	// Calc space for message - start with the header size
	*bytes = 0;

	// Calc space for number of tiers
	*bytes += FOUR_BYTES;

	// Calc space for tier elements: routes len, number of senders, functions len, input schema len, output schema len, GROUP key len, DISTINCT key len, stats task type, stats query type, stats next hop, stats port
	*bytes += tiers_len * 11 * FOUR_BYTES;

	// Calc space for input and output schema in each tier
	for (i=0; i<tiers_len; i++) {
		*bytes += tiers[i].input_schema_len * FOUR_BYTES;	
		if (! tiers[i].copy_schema)
			*bytes += tiers[i].output_schema_len * FOUR_BYTES;	
	}

	// Calc space for GROUP key columns
	for (i=0; i<tiers_len; i++)
		*bytes += tiers[i].key_len * FOUR_BYTES;	

	// Calc space for DISTINCT key columns
	for (i=0; i<tiers_len; i++)
		*bytes += tiers[i].distinct_len * FOUR_BYTES;

	// Calc space for functions and their agrs
	for (i=0; i<tiers_len; i++) {
		for (j=0; j<tiers[i].functions_len; j++)
			// We need space for the function's ID, argument length and the argument iself
			*bytes += FOUR_BYTES + FOUR_BYTES + tiers[i].functions[j].arg_len;
	}

	// Calc space for routes (fro, to, ip, port)
	for (i=0; i<tiers_len; i++)
		*bytes += tiers[i].routes_len * (ONE_BYTE + ONE_BYTE + FOUR_BYTES + FOUR_BYTES);

	// Allocate full message size
	buffer = mem_alloc(*bytes);

	// Set number of tiers
	ui32 = htonl(tiers_len);
	memcpy(buffer + offset, &ui32, FOUR_BYTES);
	offset += FOUR_BYTES;

	for (i=0; i<tiers_len; i++) {
		// Set input schema length
		ui32 = htonl(tiers[i].input_schema_len);
		memcpy(buffer + offset, &ui32, FOUR_BYTES);
		offset += FOUR_BYTES;

		// Set input schema
		for (j=0; j < tiers[i].input_schema_len; j++) {
			ui32 = htonl(tiers[i].input_schema[j]);
			memcpy(buffer + offset, &ui32, FOUR_BYTES);
			offset += FOUR_BYTES;
		}

		// Set output schema: if same as input, just set the copy flag; else, include full output schema
		if (tiers[i].copy_schema) {
			// Set output schema len to zero
			ui32 = htonl(0);
			memcpy(buffer + offset, &ui32, FOUR_BYTES);
			offset += FOUR_BYTES;
		}
		else {
			// Set output schema length
			ui32 = htonl(tiers[i].output_schema_len);
			memcpy(buffer + offset, &ui32, FOUR_BYTES);
			offset += FOUR_BYTES;

			// Set output schema
			for (j=0; j < tiers[i].output_schema_len; j++) {
				ui32 = htonl(tiers[i].output_schema[j]);
				memcpy(buffer + offset, &ui32, FOUR_BYTES);
				offset += FOUR_BYTES;
			}
		}

		// Set GROUP key columns length
		ui32 = htonl(tiers[i].key_len);
		memcpy(buffer + offset, &ui32, FOUR_BYTES);
		offset += FOUR_BYTES;

		// Set GROUP key columns
		for (j=0; j < tiers[i].key_len; j++) {
			ui32 = htonl(tiers[i].key[j]);
			memcpy(buffer + offset, &ui32, FOUR_BYTES);
			offset += FOUR_BYTES;
		}

		// Set length of columns with DISTINCT key
		ui32 = htonl(tiers[i].distinct_len);
		memcpy(buffer + offset, &ui32, FOUR_BYTES);
		offset += FOUR_BYTES;

		// Set columns with DISTINCT key
		for (j=0; j < tiers[i].distinct_len; j++) {
			ui32 = htonl(tiers[i].distinct[j]);
			memcpy(buffer + offset, &ui32, FOUR_BYTES);
			offset += FOUR_BYTES;
		}

		// Set number of senders in the tier
		ui32 = htonl(tiers[i].senders_len);
		memcpy(buffer + offset, &ui32, FOUR_BYTES);
		offset += FOUR_BYTES;

		// Set number of functions
		ui32 = htonl(tiers[i].functions_len);
		memcpy(buffer + offset, &ui32, FOUR_BYTES);
		offset += FOUR_BYTES;

		// Set functions
		for (j=0; j<tiers[i].functions_len; j++) {
			// Set function ID
			ui32 = htonl(tiers[i].functions[j].function);
			memcpy(buffer + offset, &ui32, FOUR_BYTES);
			offset += FOUR_BYTES;

			// Set argument length
			ui32 = htonl(tiers[i].functions[j].arg_len);
			memcpy(buffer + offset, &ui32, FOUR_BYTES);
			offset += FOUR_BYTES;

			// Set argument
			memcpy(buffer + offset, tiers[i].functions[j].arg, tiers[i].functions[j].arg_len);
			offset += tiers[i].functions[j].arg_len;
		}

		// Routes
		ui32 = htonl(tiers[i].routes_len);
		memcpy(buffer + offset, &ui32, FOUR_BYTES);
		offset += FOUR_BYTES;

		for (j=0; j<tiers[i].routes_len; j++) {
			memcpy(buffer + offset, &tiers[i].routes[j].from, ONE_BYTE);
			offset += ONE_BYTE;

			memcpy(buffer + offset, &tiers[i].routes[j].to, ONE_BYTE);
			offset += ONE_BYTE;

			ui32 = htonl(tiers[i].routes[j].ip);
			memcpy(buffer + offset, &ui32, FOUR_BYTES);
			offset += FOUR_BYTES;

			ui32 = htonl(tiers[i].routes[j].port);
			memcpy(buffer + offset, &ui32, FOUR_BYTES);
			offset += FOUR_BYTES;
		}

		// Set up stats: task type, query type, host and port
		ui32 = htonl(tiers[i].stats_task_type);
		memcpy(buffer + offset, &ui32, FOUR_BYTES);
		offset += FOUR_BYTES;

		ui32 = (tiers[i].stats_task_type) ? htonl(tiers[i].stats_query_type) : 0;
		memcpy(buffer + offset, &ui32, FOUR_BYTES);
		offset += FOUR_BYTES;

		ui32 = (tiers[i].stats_task_type) ? htonl(tiers[i].stats_ip) : 0;
		memcpy(buffer + offset, &ui32, FOUR_BYTES);
		offset += FOUR_BYTES;

		ui32 = (tiers[i].stats_task_type) ? htonl(tiers[i].stats_port) : 0;
		memcpy(buffer + offset, &ui32, FOUR_BYTES);
		offset += FOUR_BYTES;
	}

	*ep = buffer;

	return 0;
}

// Prepare message for type 3 (KILL)
char *prepare_message3(int *bytes, uint64_t *job_id) {
	char *buffer;
	uint64_t tmp64;
	uint32_t offset;

	// Full message size
	*bytes = MESSAGE_HEADER_SIZE + 2 * EIGHT_BYTES + FOUR_BYTES;
	buffer = mem_alloc(*bytes);

	// Add header
	prepare_header(buffer, &offset, DEXEC_MESSAGE_KILL, *bytes);

	// Add job ID
	tmp64 = htobe64(job_id[0]);
	memcpy(buffer + offset, &tmp64, EIGHT_BYTES);
	offset += EIGHT_BYTES;
	tmp64 = htobe64(job_id[1]);
	memcpy(buffer + offset, &tmp64, EIGHT_BYTES);
	offset += EIGHT_BYTES;

	return buffer;
}

// Prepare message for type 4 (END)
char *prepare_message4(int *bytes) {
	char *buffer;
	int offset;

	// Full message size
	*bytes = MESSAGE_HEADER_SIZE;
	buffer = mem_alloc(*bytes);

	// Add header
	prepare_header(buffer, &offset, DEXEC_MESSAGE_END, *bytes);

	return buffer;
}

// Prepare message for type 5 (ERROR)
char *prepare_message5(int *bytes, uint64_t *job_id, uint32_t node_id, uint32_t error, char *error_msg) {
	char *buffer;
	uint64_t tmp64;
	uint32_t tmp32, offset;

	// Full message size
	*bytes = MESSAGE_HEADER_SIZE + 2 * EIGHT_BYTES + 3 * FOUR_BYTES;
	if (error_msg)
		*bytes += strlen(error_msg);
	buffer = mem_alloc(*bytes);

	// Add header
	prepare_header(buffer, &offset, DEXEC_MESSAGE_ERROR, *bytes);

	// Add job ID
	tmp64 = htobe64(job_id[0]);
	memcpy(buffer + offset, &tmp64, EIGHT_BYTES);
	offset += EIGHT_BYTES;
	tmp64 = htobe64(job_id[1]);
	memcpy(buffer + offset, &tmp64, EIGHT_BYTES);
	offset += EIGHT_BYTES;

	// Add the node ID
	tmp32 = htonl(node_id);
	memcpy(buffer + offset, &tmp32, FOUR_BYTES);
	offset += FOUR_BYTES;

	// Add the error
	tmp32 = htonl(error);
	memcpy(buffer + offset, &tmp32, FOUR_BYTES);
	offset += FOUR_BYTES;

	// Add the error message length and message itself
	if (error_msg)
		tmp32 = htonl(strlen(error_msg));
	else
		tmp32 = 0;
	memcpy(buffer + offset, &tmp32, FOUR_BYTES);
	offset += FOUR_BYTES;

	if (error_msg)
		memcpy(buffer + offset, error_msg, strlen(error_msg));

	return buffer;
}

// Process message header
struct message_header_s *process_header(char *buffer) {
	int offset = 0;
	struct message_header_s *message = mem_alloc(sizeof(struct message_header_s));

	// Extract magic: strlen(MAGIC_STRING bytes)
	char magic[strlen(MAGIC_STRING) + 1];
	memcpy(&message->magic[0], buffer + offset, strlen(MAGIC_STRING));
	memcpy(&magic[0], buffer + offset, strlen(MAGIC_STRING));
	memset(&magic[strlen(MAGIC_STRING)], '\0', 1);
	offset += strlen(MAGIC_STRING);
	system_log(LOG_DEBUG, "Message MAGIC_STRING: %s", &magic[0]);

	// Extract message size
	message->size = extract_int32(buffer + offset);
	offset += FOUR_BYTES;
	system_log(LOG_DEBUG, "Message SIZE: %i", message->size);

	// Extract message type
	message->type = extract_int32(buffer + offset);
	offset += FOUR_BYTES;
	system_log(LOG_DEBUG, "Message TYPE: %i", message->type);

	return message;
}


// Process message for type 0 (CONNECT)
struct message0_s *process_message0(char *buffer) {
	int offset = MESSAGE_HEADER_SIZE;
	struct message0_s *message = mem_alloc(sizeof(struct message0_s));

	// NB: the header has already been extracted from the buffer

	// Extract the job ID
	message->job_id[0] = extract_int64(buffer + offset);
	offset += EIGHT_BYTES;
	message->job_id[1] = extract_int64(buffer + offset);
	offset += EIGHT_BYTES;

	// Extract tier
	message->tier = extract_int32(buffer + offset);

	system_log(LOG_DEBUG, "Message JOB ID: %016lx%016lx, TIER: %u", message->job_id[0], message->job_id[1], message->tier);

	return message;
}

// Process message for type 1 (JOB)
struct message1_s *process_message1(char *buffer) {
	int i, j, offset = MESSAGE_HEADER_SIZE;
	struct message1_s *message = mem_alloc(sizeof(struct message1_s));

	// NB: the header has already been extracted from the buffer

	// Extract number of tiers
	message->tiers_len = extract_int32(buffer + offset);
	offset += FOUR_BYTES;
	system_log(LOG_DEBUG, "Message TIERS: %u", message->tiers_len);

	// Allocate memory for tiers
	message->tiers = mem_alloc(message->tiers_len * sizeof(struct tier_s));
	for (i=0; i<message->tiers_len; i++) {
		// Extract input_schema length
		message->tiers[i].input_schema_len = extract_int32(buffer + offset);
		offset += FOUR_BYTES;
		system_log(LOG_DEBUG, "Message TIER %i INPUT SCHEMA LENGTH: %u", i, message->tiers[i].input_schema_len);

		// Extract input schema
		message->tiers[i].input_schema = mem_alloc(message->tiers[i].input_schema_len * FOUR_BYTES);
		for (j=0; j < message->tiers[i].input_schema_len; j++) {
			message->tiers[i].input_schema[j] = extract_int32(buffer + offset);
			system_log(LOG_DEBUG, "Message TIER %i INPUT SCHEMA ENTRY: %u", i, message->tiers[i].input_schema[j]);
			offset += FOUR_BYTES;
		}

		// Extract output_schema length
		message->tiers[i].output_schema_len = extract_int32(buffer + offset);
		offset += FOUR_BYTES;
		system_log(LOG_DEBUG, "Message TIER %i OUTPUT SCHEMA LENGTH: %u", i, message->tiers[i].output_schema_len);

		// Extract output schema if available, else copy the input schema
		if (message->tiers[i].output_schema_len) {
			message->tiers[i].output_schema = mem_alloc(message->tiers[i].output_schema_len * FOUR_BYTES);
			for (j=0; j < message->tiers[i].output_schema_len; j++) {
				message->tiers[i].output_schema[j] = extract_int32(buffer + offset);
				system_log(LOG_DEBUG, "Message TIER %i OUTPUT SCHEMA ENTRY: %u", i, message->tiers[i].output_schema[j]);
				offset += FOUR_BYTES;
			}
			message->tiers[i].copy_schema = 0;
		}
		else {
			message->tiers[i].output_schema_len = message->tiers[i].input_schema_len;
			message->tiers[i].output_schema = mem_alloc(message->tiers[i].output_schema_len * FOUR_BYTES);
			memcpy(message->tiers[i].output_schema, message->tiers[i].input_schema, message->tiers[i].input_schema_len * FOUR_BYTES);
			message->tiers[i].copy_schema = 1;
			system_log(LOG_DEBUG, "Message TIER %i OUTPUT SCHEMA: copied INPUT schema", i);
		}

		// Extract GROUP key length
		message->tiers[i].key_len = extract_int32(buffer + offset);
		offset += FOUR_BYTES;
		system_log(LOG_DEBUG, "Message TIER %i GROUP KEY LENGTH: %u", i, message->tiers[i].key_len);

		// Extract GROUP key
		message->tiers[i].key = NULL;
		if (message->tiers[i].key_len) {
			message->tiers[i].key = mem_alloc(message->tiers[i].key_len * FOUR_BYTES);
			for (j=0; j < message->tiers[i].key_len; j++) {
				message->tiers[i].key[j] = extract_int32(buffer + offset);
				system_log(LOG_DEBUG, "Message TIER %i GROUP KEY ENTRY: %u", i, message->tiers[i].key[j]);
				offset += FOUR_BYTES;
			}
		}

		// Extract length of columns with DISTINCT key
		message->tiers[i].distinct_len = extract_int32(buffer + offset);
		offset += FOUR_BYTES;
		system_log(LOG_DEBUG, "Message TIER %i DISTINCT COLUMNS: %u", i, message->tiers[i].distinct_len);

		// Extract columns with DISTINCT key
		message->tiers[i].distinct = NULL;
		if (message->tiers[i].distinct_len) {
			message->tiers[i].distinct = mem_alloc(message->tiers[i].distinct_len * FOUR_BYTES);
			for (j=0; j < message->tiers[i].distinct_len; j++) {
				message->tiers[i].distinct[j] = extract_int32(buffer + offset);
				system_log(LOG_DEBUG, "Message TIER %i DISTINCT COLUMN: %u", i, message->tiers[i].distinct[j]);
				offset += FOUR_BYTES;
			}
		}

		// Extract number of senders
		message->tiers[i].senders_len = extract_int32(buffer + offset);
		offset += FOUR_BYTES;
		system_log(LOG_DEBUG, "Message TIER %i SENDERS: %u", i, message->tiers[i].senders_len);

		// Extract number of functions
		message->tiers[i].functions_len = extract_int32(buffer + offset);
		offset += FOUR_BYTES;
		system_log(LOG_DEBUG, "Message TIER %i FUNCTIONS: %u", i, message->tiers[i].functions_len);

		// Extract functions
		if (message->tiers[i].functions_len) {
			message->tiers[i].functions = mem_alloc(message->tiers[i].functions_len * sizeof(struct function_s));
			for (j=0; j<message->tiers[i].functions_len; j++) {
				message->tiers[i].functions[j].function = extract_int32(buffer + offset);
				offset += FOUR_BYTES;
				system_log(LOG_DEBUG, "Message TIER %i FUNCTION %i: %u", i, j, message->tiers[i].functions[j].function);

				message->tiers[i].functions[j].arg_len = extract_int32(buffer + offset);
				offset += FOUR_BYTES;
				system_log(LOG_DEBUG, "Message TIER %i FUNCTION %i ARG LEN: %u", i, j, message->tiers[i].functions[j].arg_len);

				message->tiers[i].functions[j].arg = NULL;
				if (message->tiers[i].functions[j].arg_len) {
					message->tiers[i].functions[j].arg = mem_alloc(message->tiers[i].functions[j].arg_len);
					memcpy(message->tiers[i].functions[j].arg, buffer + offset, message->tiers[i].functions[j].arg_len);
					offset += message->tiers[i].functions[j].arg_len;
				}
			}
		}
		else
			message->tiers[i].functions = NULL;

		// Extract routes
		message->tiers[i].routes_len = extract_int32(buffer + offset);
		offset += FOUR_BYTES;
		system_log(LOG_DEBUG, "Message TIER %i TOTAL ROUTES: %u", i, message->tiers[i].routes_len);

		if (message->tiers[i].routes_len) {
			message->tiers[i].routes = mem_alloc(message->tiers[i].routes_len * sizeof(struct route_s));
			for (j=0; j<message->tiers[i].routes_len; j++) {
				message->tiers[i].routes[j].from = extract_char(buffer + offset);
				offset += ONE_BYTE;

				message->tiers[i].routes[j].to = extract_char(buffer + offset);
				offset += ONE_BYTE;

				message->tiers[i].routes[j].ip = extract_int32(buffer + offset);
				offset += FOUR_BYTES;

				message->tiers[i].routes[j].port = extract_int32(buffer + offset);
				offset += FOUR_BYTES;

				message->tiers[i].routes[j].socket = -1;

				system_log(LOG_DEBUG, "Message TIER %i ROUTE %i: FROM %02x TO %02x NEXT HOP %08x:%u", i, j, message->tiers[i].routes[j].from, message->tiers[i].routes[j].to, message->tiers[i].routes[j].ip, message->tiers[i].routes[j].port);
			}
		}
		else
			message->tiers[i].routes = NULL;

		// Extract stats: task type, query type, stats host and UDP port
		message->tiers[i].stats_task_type = extract_int32(buffer + offset);
		offset += FOUR_BYTES;
		system_log(LOG_DEBUG, "Message TIER %i STATS TASK TYPE: %u", i, message->tiers[i].stats_task_type);

		message->tiers[i].stats_query_type = extract_int32(buffer + offset);
		offset += FOUR_BYTES;
		system_log(LOG_DEBUG, "Message TIER %i STATS QUERY TYPE: %u", i, message->tiers[i].stats_query_type);

		message->tiers[i].stats_ip = extract_int32(buffer + offset);
		offset += FOUR_BYTES;
		system_log(LOG_DEBUG, "Message TIER %i STATS IP: %08x", i, message->tiers[i].stats_ip);

		message->tiers[i].stats_port = extract_int32(buffer + offset);
		offset += FOUR_BYTES;
		system_log(LOG_DEBUG, "Message TIER %i STATS PORT: %u", i, message->tiers[i].stats_port);
	}

	return message;
}

// Process message for type 3 (KILL)
struct message3_s *process_message3(char *buffer) {
	int offset = MESSAGE_HEADER_SIZE;
	struct message3_s *message = mem_alloc(sizeof(struct message3_s));

	// NB: the header has already been extracted from the buffer

	// Extract the job ID
	message->job_id[0] = extract_int64(buffer + offset);
	offset += EIGHT_BYTES;
	message->job_id[1] = extract_int64(buffer + offset);
	offset += EIGHT_BYTES;

	system_log(LOG_DEBUG, "Message JOB ID: %016lx%016lx", message->job_id[0], message->job_id[1]);

	return message;
}

// Process message for type 5 (ERROR)
struct message5_s *process_message5(char *buffer) {
	int offset = MESSAGE_HEADER_SIZE;
	int msg_len;
	struct message5_s *message = mem_alloc(sizeof(struct message5_s));

	// NB: the header has already been extracted from the buffer

	// Extract the job ID
	message->job_id[0] = extract_int64(buffer + offset);
	offset += EIGHT_BYTES;
	message->job_id[1] = extract_int64(buffer + offset);
	offset += EIGHT_BYTES;

	// Extract node ID 
	message->node_id = extract_int32(buffer + offset);
	offset += FOUR_BYTES;

	// Extract error 
	message->error = extract_int32(buffer + offset);
	offset += FOUR_BYTES;

	// Extract error message
	msg_len = extract_int32(buffer + offset);
	offset += FOUR_BYTES;

	message->error_msg = mem_alloc(msg_len + 1);
	memcpy(message->error_msg, buffer + offset, msg_len);

	system_log(LOG_DEBUG, "Message ERROR ID: %016lx%016lx, ERROR: %u", message->job_id[0], message->job_id[1], message->error);

	return message;
}

