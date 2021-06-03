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

/*
Example DEXec client using the DExec client library. 

You must have installed dexecclient.so and its header file, mammothdb/dexec.h.

To compile: 
gcc -o client-lib client-lib.c -I /usr/include/mammothdb -l dexecclient

You probably want to tweak the defines (and maybe the code) below before compiling.
*/

#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <time.h>
#include <mammothdb/dexec.h>

#define QUERY "SELECT a, b, c  FROM test.f_test1"
#define AGGR_HOST "172.20.96.18"
#define QE_HOST "172.20.96.16"
#define TIERS 1
#define QE_SENDERS 3
#define TCP_PORT 62671	// Only for intermittent tiers; last tier listens on a random port!

int main(int argc, char **argv) {
	// Message variables
	uint64_t job_id[2];

	struct dexec_listener_s listener;

	// Helper variables
	int i, j, k, row_count, len;
	int a, b, c, d;

	// Create a random job ID
	srand(time(NULL));
	a = rand();
	b = rand();
	job_id[0] = a * b;
	c = rand();
	d = rand();
	job_id[1] = c * d;

	printf("JOB ID: %016lx%016lx\n", job_id[0], job_id[1]);

	// Init listener to obtain a TCP port number
	if (dexec_listener_init(&listener)) {
		printf("ERROR initialising listener");
		exit(EXIT_FAILURE);
	}
	printf("Listener initialised (port is %i).\n", listener.port);

	listener.senders = QE_SENDERS;

	// Start listener
	if (dexec_listener_start(&listener)) {
		printf("ERROR starting listener\n");
		exit(EXIT_FAILURE);
	}
	printf("Listener started on port %i.\n", listener.port);

	// Define schema with 5 columns
	int schema_len = 3;
	int schema[schema_len];
	schema[0] = DEXEC_DATA_LONG;
	schema[1] = DEXEC_DATA_LONG;
	schema[2] = DEXEC_DATA_LONG;

	// Allocate memory for tiers
	struct tier_s *tiers = malloc(TIERS * sizeof(struct tier_s));
	bzero(tiers, TIERS * sizeof(struct tier_s));

	// Define input schema for first tier
	tiers[0].input_schema_len = schema_len;
	tiers[0].input_schema = schema;
	tiers[0].copy_schema = 1;

	// Output schema  for first tier
	tiers[0].copy_schema = 1;
	tiers[0].output_schema_len = schema_len;
	tiers[0].output_schema = schema;

	// Define routes hop for first tier (one default route to QE)
	tiers[0].routes_len = 1;
	tiers[0].routes = mem_alloc(tiers[0].routes_len * sizeof(struct route_s));
	tiers[0].routes[0].from = 0;
	tiers[0].routes[0].to = 255;
	char host[] = QE_HOST;
	struct in_addr tmp_addr;
	inet_aton(host, &tmp_addr);
	tiers[0].routes[0].ip = ntohl(tmp_addr.s_addr);
	tiers[0].routes[0].port = listener.port;

	// Disable stats on first tier
	tiers[0].stats_task_type = 0;

	// Define number of senders for first tier
	tiers[0].senders_len = 0;

	// Define GROUP key length and keys for first tier
	//tiers[0].key_len = 1;
	tiers[0].key_len = 0;

//	tiers[0].key = malloc(tiers[0].key_len * sizeof(int));
//	bzero(tiers[0].key, tiers[0].key_len * sizeof(int));
//	tiers[0].key[0] = 0;

	// Define DISTINCT key length and keys for first tier
	tiers[0].distinct_len = 0;

	// Define functions for first tier
	tiers[0].functions_len = 1;

	// Allocate memory for functions in first tier
	tiers[0].functions = malloc(tiers[0].functions_len * sizeof(struct function_s));
	bzero(tiers[0].functions, tiers[0].functions_len * sizeof(struct function_s));
	bzero(tiers[0].functions, tiers[0].functions_len * sizeof(struct function_s));

	// Define functions in first tier
	int arg, offset = 0;
	tiers[0].functions[0].function = DEXEC_FUNCTION_MYSQL;
	tiers[0].functions[0].arg = malloc(FOUR_BYTES + strlen(QUERY) + 1);
	bzero(tiers[0].functions[0].arg, FOUR_BYTES + strlen(QUERY) + 1);
	arg = htonl(DEXEC_STORAGE_OUTPUT);
	memcpy(tiers[0].functions[0].arg, &arg, FOUR_BYTES);
	offset += FOUR_BYTES;
	sprintf(tiers[0].functions[0].arg + offset, QUERY);
	tiers[0].functions[0].arg_len = FOUR_BYTES + strlen(QUERY);

/*
	// Define input and output schema for second tier
	tiers[1].input_schema_len = schema_len;
	tiers[1].input_schema = schema;
	tiers[1].copy_schema = 1;

	// Disable stats on first tier
	tiers[1].stats_task_type = 0;

	// Define next hop for second tier
	inet_aton(QE_HOST, &tmp_addr);
	tiers[1].next_hop_ip = ntohl(tmp_addr.s_addr);

	// Set port from listener
	tiers[1].next_hop_port = listener.port;

	// Define number of senders for second tier
	tiers[1].senders_len = 3;

	// Define key length and keys for second tier
	tiers[1].key_len = 0;
	tiers[1].key = 0;

	// Define functions for second tier
	tiers[1].functions_len = 1;

	// Allocate memory for functions in second tier
	tiers[1].functions = malloc(tiers[1].functions_len * sizeof(struct function_s));
	bzero(tiers[1].functions, tiers[1].functions_len * sizeof(struct function_s));
	bzero(tiers[1].functions, tiers[1].functions_len * sizeof(struct function_s));

	// Define functions in second tier
//	tiers[1].functions[0].function = DEXEC_FUNCTION_DUMP_INPUT;
//	tiers[1].functions[0].arg = 0;
*/

	// Prepare Execution Plan
	char *ep;
	int ep_len = 0;
	dexec_prepare_execution_plan(&ep, &ep_len, tiers, TIERS);
	printf("Execution Plan prepared (%i bytes).\n", ep_len);

	// Prepare nodes list
	//TODO: Create a library function for this?
	struct sockaddr_in tmp_add;
	listener.nodes_len = 3;
	char *hosts[listener.nodes_len];
	listener.nodes = malloc(listener.nodes_len * sizeof(uint32_t));
	bzero(listener.nodes, listener.nodes_len * sizeof(uint32_t));
	hosts[0] = "172.20.96.17";
	hosts[1] = "172.20.96.18";
	hosts[2] = "172.20.96.19";
	for (i=0; i < listener.nodes_len; i++) {
		if (inet_aton(hosts[i], &tmp_addr) == 0) {
			printf("ERROR: unable to set host %s: %s\n", hosts[i], strerror(errno));
			exit(EXIT_FAILURE);
		}
		listener.nodes[i] = ntohl(tmp_addr.s_addr);
	}

	// Submit job
	printf("Submitting job...\n");
	if (dexec_submit_job(ep, ep_len, listener.nodes, listener.nodes_len, &job_id[0])) {
		printf("ERROR submitting job\n");
		exit(EXIT_FAILURE);
	}
	free(ep);
	printf("Job submitted.\n");

	// Fetch results
	char *mysql_row, *error_msg=NULL;
	int mysql_row_len, rnd_res, loop=1;
	while (loop) {
		rnd_res = dexec_rnd_next(&listener, &schema[0], schema_len, &mysql_row, &mysql_row_len, &error_msg);
		switch(rnd_res) {
			case 0:
				printf("MYSQL ROW LEN: %i\n", mysql_row_len);
				if (mysql_row)
					free(mysql_row);
				break;

			case DEXEC_HA_ERR_END_OF_FILE:
				loop = 0;
				break;

			default:
				// Any other value indicates an error
				loop = 0;

				printf("Received error %i: %s", rnd_res, error_msg);
				if (error_msg)
					free(error_msg);

				dexec_error(rnd_res, &error_msg);
				printf("Generic error message is: %s", error_msg);
				if (error_msg)
					free(error_msg);

				break;
		}
	}

	// Clean thread
	dexec_listener_end(&listener);

	// Clean-up
	if (tiers) {
		for (i=0; i < TIERS; i++) {
			if (tiers[i].functions) {
				for (j=0; j < tiers[i].functions_len; j++) {
					if (tiers[i].functions[j].arg)
						free(tiers[i].functions[j].arg);
				}
				free(tiers[i].functions);
			}
			if (tiers[i].key)
				free(tiers[i].key);
			if (tiers[i].routes)
				free(tiers[i].routes);
		}

		free(tiers);
	}

	return EXIT_SUCCESS;
}

