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

// Header file for libdexeclient.so

// Copied from my_base.h since the idiots from MySQL do not distribute this in the devel package, so we can't include it
#define DEXEC_HA_ERR_END_OF_FILE   137
#define DEXEC_HA_ERR_CRASHED_ON_USAGE   145

#define ONE_BYTE 1
#define TWO_BYTES 2
#define FOUR_BYTES 4
#define EIGHT_BYTES 8
#define SIXTEEN_BYTES 16

// ENUMERATORS

// Input and output as int
enum {
	DEXEC_STORAGE_INPUT = 0,
	DEXEC_STORAGE_OUTPUT,
	DEXEC_STORAGE_TMP
};

// Type of functions as int
enum {
	DEXEC_FUNCTION_NONE = 0,
	DEXEC_FUNCTION_MYSQL,
	DEXEC_FUNCTION_MIN,
	DEXEC_FUNCTION_MAX,
	DEXEC_FUNCTION_SUM,
	DEXEC_FUNCTION_AVG,
	DEXEC_FUNCTION_COUNT,
	DEXEC_FUNCTION_SUM_DISTINCT,
	DEXEC_FUNCTION_AVG_DISTINCT,
	DEXEC_FUNCTION_COUNT_DISTINCT,
	DEXEC_FUNCTION_GLOBAL_DISTINCT,
};

// Type of data in columns as int
// In QE, we map certain values from Java column type defintions to our values below (https://docs.oracle.com/javase/7/docs/api/java/sql/Types.html)
enum {
	DEXEC_DATA_NONE = 0,
	DEXEC_DATA_LONG,		// 64-bit integer: covers TINYINT, BOOL, BOOLEAN, SMALLINT, MEDIUMINT, INT, BIGINT
	DEXEC_DATA_DOUBLE,		// 64-bit integer in IEEE 754 format: coverts FLOAT, DOUBLE
	DEXEC_DATA_DECIMAL,		// Complex type (struct) & math implementing DECIMAL(65,30)
	DEXEC_DATA_TIMESTAMP,	// 32-bit integer: covers DATE, TIME, DATETIME, TIMESTAMP
	DEXEC_DATA_VARCHAR,		// Any-length char (NULL-terminated): covers CHAR, VARCHAR, TEXT, MEDIUMTEXT, BIGTEXT
	DEXEC_DATA_VARBINARY	// Any other data type (no processing)
};

// Error codes for data manipulating functions
enum {
	DEXEC_OK = 0,				// No error occurred
	DEXEC_ERROR_SYSTEM,			// System error - thread creation failed, OOM etc.
	DEXEC_ERROR_OVERFLOW,		// Overflow (most significant digit loss)
	DEXEC_ERROR_TRUNCATED,		// Least significant digit loss
	DEXEC_ERROR_UNSUPPORTED,	// Unsupported value (function, column type, action etc.)
	DEXEC_ERROR_NETWORK,		// Failed to init network or disconnected from remote host while sending data
	DEXEC_ERROR_RANGE,			// Value is out of range
	DEXEC_ERROR_MYSQL,			// MySQL error
	DEXEC_ERROR_DIVISION,		// Division by zero error
	DEXEC_ERROR_FILESYSTEM,		// Filesystem access error
	DEXEC_ERROR_KILLED			// JOb was killed while in progress
};

// STRUCTURES

// Mapping between a function to apply and a argument to pass it
struct function_s {
	int function;
	int arg_len;
	void *arg;
	int storage_in;
	int col_in;
	int storage_out;
	int col_out;
	struct btree_root_s *btree;	// Local BTree instance for DISTINCT keys
	int btree_res;				// Result form last BTree call
};

// Route
struct route_s {
	unsigned char from;
	unsigned char to;
	uint32_t ip;
	uint32_t port;
	int socket;
};

// Tier (a round of data processing specification)
struct tier_s {
	uint32_t senders_len;
	uint32_t input_schema_len;
	uint32_t *input_schema;
	uint32_t output_schema_len;
	uint32_t *output_schema;
	uint32_t key_len;
	uint32_t *key;
	uint32_t *distinct;			// Indexes of columns on which DISTINCT key is enabled
	uint32_t distinct_len;		// Length of DISTINCT index (columns * FOUR_BYTES)
	struct function_s *functions;
	uint32_t functions_len;
	struct route_s *routes;
	uint32_t routes_len;
	uint32_t output_format;
	int copy_schema;
	int stats_task_type;
	int stats_query_type;
	uint32_t stats_ip;
	uint32_t stats_port;
};

// Listener (you need to fill in the commented fields)
struct dexec_listener_s {
	pthread_t thread_id;
	int port;						// TCP port to listen at
	int socket;
	int error;
	uint32_t *nodes;				// Array of IPv4 addresses in network byte order (used in KILL)
	int nodes_len;					// Number of nodes (used in KILL)
	int senders;					// Number of nodes that will send us data back
	struct socket_storage_s *ss;
	struct fifo_s *fifo_chunks;
	struct fifo_s *fifo_rows;
	struct fs_root_s *queue;
};

// Error codes as string (order should match the error codes above!)
static const char *error_map[] = {
	// DEXEC_OK,
	"No error",
	// DEXEC_ERROR_SYSTEM,
	"System error",
	// DEXEC_ERROR_OVERFLOW,
	"Overflow (most significant digit loss)",
	// DEXEC_ERROR_TRUNCATED,
	"Underflow (least significant digit loss)",
	// DEXEC_ERROR_UNSUPPORTED,	
	"Unsupported value (function, column type, action etc.)",
	// DEXEC_ERROR_NETWORK,	
	"Network error (failed to init network, resolve host or disconnected while sending data)",
	// DEXEC_ERROR_RANGE,
	"Value out of range",
	// DEXEC_ERROR_MYSQL
	"MySQL error",
	//DEXEC_ERROR_DIVISION
	"Division by zero",
	//DEXEC_ERROR_FILESYSTEM
	"Filesystem error",
	//DEXEC_ERROR_KILLED
	"Job was killed"
};

// PUBLIC METHODS PROTOTYPES

int dexec_listener_init(struct dexec_listener_s *listener) __attribute__ ((visibility ("default") ));
int dexec_listener_start(struct dexec_listener_s *listener)  __attribute__ ((visibility ("default") ));
int dexec_listener_close(struct dexec_listener_s *listener) __attribute__ ((visibility ("default") ));
int dexec_listener_end(struct dexec_listener_s *listener) __attribute__ ((visibility ("default") ));
int dexec_prepare_execution_plan(char **ep, int *bytes, struct tier_s *tiers, int tiers_len) __attribute__ ((visibility ("default") ));
int dexec_submit_job(char *ep, int ep_len, uint32_t *hosts, int hosts_len, uint64_t *job_id) __attribute__ ((visibility ("default") ));
int dexec_kill_job(uint32_t *hosts, int hosts_len, uint64_t *job_id) __attribute__ ((visibility ("default") ));
int dexec_rnd_next(struct dexec_listener_s *listener, int *schema, int schema_len, char **mysql_row, int *mysql_row_len, char **error_msg) __attribute__ ((visibility ("default") ));
int dexec_error(int error, char **error_msg) __attribute__ ((visibility ("default") ));


