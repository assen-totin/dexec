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

#include <arpa/inet.h>
#include <dirent.h>
#include <dlfcn.h>
#include <endian.h>
#include <errno.h>
#include <fcntl.h>
#include <float.h>
#include <ifaddrs.h>
#include <limits.h>
#include <math.h>
#include <mysql.h>
#include <netdb.h>
#include <netinet/in.h>
#include <pthread.h>
#include <pwd.h>
#include <signal.h>
#include <stdarg.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <syslog.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/socket.h>
#include <sys/syscall.h>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <sys/un.h>
#include <unistd.h>

#include "../config.h"

#ifdef HAVE_MDB_STATS
#include <mammothdb/stats.h>
#endif

#ifdef HAVE_SYSTEMD
#include <systemd/sd-daemon.h>
#include <systemd/sd-journal.h>
#endif

// NB: Some definitions and enums are in lib.h!

//// DEFINITIONS

// Config defaults (override them in /etc/mammothdb/mammothdb-env.sh or in /etc/sysconfig/mdb-dexec)
#define DEFAULT_LOG_LEVEL LOG_DEBUG

// Built-in defaults (cannot be overridden)
#define RUN_AS_USER "mammothdb"
#define MAX_RAM 1000000000				// Size of used RAM to enable FS layer: 1 GB
#define MAX_MALLOC_SIZE 1000000000		// 1 GB
#define MAX_ROWS_PER_CHUNK 10000		// Max number of rows in a message type 2
#define MAX_FIFO_SIZE 1000000			// Msx number of elements in a FIFO
#define THREAD_SET_STEP 100
#define TRULY_RANDOM_NUMBER 42
#define MAGIC_STRING "MMRD"
#define MAGIC_STRING_LEN 4				// GCC is an idiot which refuses to use strlen() of a #define in struct definition
#define MESSAGE_HEADER_SIZE 12
#define DEFAULT_TCP_PORT 62671
#define STATS_SEND_FETCHING_EACH_ROWS 1000
#define STATS_MAX_PAYLOAD_SIZE 1000
#define ERROR_MESSAGE_LEN 1024			// Because it is used in stats and must fit with the stats message in single UDP packet

//// ENUMS

// Job status
enum {
	DEXEC_JOB_STATUS_NONE = 0,
	DEXEC_JOB_STATUS_RUNNING,
	DEXEC_JOB_STATUS_QUERY,
	DEXEC_JOB_STATUS_EXTRACT,
	DEXEC_JOB_STATUS_AGGREGATE,
	DEXEC_JOB_STATUS_DISTINCT,
	DEXEC_JOB_STATUS_COMPLETE,
	DEXEC_JOB_STATUS_FAILURE,
	DEXEC_JOB_STATUS_KILLED
};

// Data and socket storage cleanup modes
enum {
	DEXEC_CLEAN_NONE = 0,
	DEXEC_CLEAN_RESET,		// Set to NULL without free() - e.g., for newly allocated
	DEXEC_CLEAN_FREE,		// free() if not NULL, then set to NULL
	DEXEC_CLEAN_CUSTOM,		// Custom cleanup, see inside the invoked function for meaning
	DEXEC_CLEAN_CUSTOM2
};

// Type of messages
enum {
	DEXEC_MESSAGE_CONNECT = 0,
	DEXEC_MESSAGE_JOB,
	DEXEC_MESSAGE_DATA,
	DEXEC_MESSAGE_KILL,
	DEXEC_MESSAGE_END, 
	DEXEC_MESSAGE_ERROR
};

// Sender mode
enum {
	DEXEC_SENDER_NONE = 0,
	DEXEC_SENDER_DIRECT,
	DEXEC_SENDER_CHUNKS,
	DEXEC_SENDER_BUCKETS,
	DEXEC_SENDER_END,
};

// Chunk/msg types
enum {
	DEXEC_TYPE_NONE = 0,
	DEXEC_TYPE_RAM,
	DEXEC_TYPE_QUEUE,
	DEXEC_TYPE_END,
};

// Filesystem hirerachy types (denotes what type of items are stored in the hierarchy)
enum {
	DEXEC_FS_NONE = 0,
	DEXEC_FS_QUEUE,
	DEXEC_FS_BUCKET,
};

// Meta data building flags (powers of 2!)
enum {
	DEXEC_META_VIEW = 1,
	DEXEC_META_ROW = 2,
};

// MySQL modes
enum {
	DEXEC_MYSQL_STREAM = 0,
	DEXEC_MYSQL_BUFFER,
};

// BTree Results
enum {
	BTREE_RESULT_OK = 0,
	BTREE_RESULT_ADDED,
	BTREE_RESULT_FOUND,
};

// Btree Key length
enum {
	BTREE_DEPTH_32 = 32,
	BTREE_DEPTH_128 = 128,
};

//// STRUCTS

// Buffers for reading data from sockets
struct socket_storage_s {
	int connected;
	int socket;
	void *buffer;
	uint32_t sofar;
	uint32_t remaining;
	uint32_t size;
	uint32_t type;
	int message_complete;
	int thread_slot;
};

// Data chunk (read from network)
struct chunk_s {
	void *buffer;
	struct chunk_s *next;
};

// Message header
struct message_header_s {
	char magic[MAGIC_STRING_LEN];
	uint32_t size;
	uint32_t type;
};

// Message type 0: CONNECT
struct message0_s {
	uint64_t job_id[2];
	int32_t tier;
};

// Message type 1: JOB
struct message1_s {
	uint64_t job_id[2];
	int tiers_len;
	struct tier_s *tiers;
};

// Message type 3: KILL
struct message3_s {
	uint64_t job_id[2];
};

// Message type 5: ERROR
struct message5_s {
	uint64_t job_id[2];
	uint32_t node_id;
	uint32_t error;
	char *error_msg;
};

// Cell definition (similar to view_s, but null element is not pointer)
struct cell_s {
	void *data;
	uint32_t len;
	char null;
};

// View element (cell) definition
struct view_s {
	void *data;			// Points to the beginning of data
	uint32_t len;
	char *null;			// Points to the NULL flag in the cell
	void *distinct;		// Points to the DISTINCT key of the cell, if any
};

// Row element definitions (row meta)
struct row_meta_s {
	void *key;
	uint32_t key_len;
	int node_id;			// The ID of the node that sent us the data (used for deduplication)
	void *data;				// Pointer to the beginning of the full row
	uint32_t len;			// Length of the full row
	void *cells;			// Pointer to the beginning of the cells
	uint32_t cells_len;		// Length of the cells
};

// Node ID detection structure
struct node_s {
	int node_id;
	char *ip;
};

// Chunks handlers
struct chunk_handler_s {
	void *data;					// Data poiner, if saved to RAM
	uint64_t size;
	uint64_t row_count;
	uint64_t id;				// FSQueue ID, if saved to FS Queue
	char type;
};

// Data storage
struct data_storage_s {
	void *chunk;				// Currently loaded chunk and its length
	uint64_t chunk_len;
	struct chunk_handler_s *chunks;		// Pointer to all chunks and their number
	uint32_t chunks_len;
	int32_t chunk_id;			// ID of the currently loaded chunk
	struct view_s *view;
	struct row_meta_s *row_meta;
	uint32_t row_count;
	uint32_t *schema;
	uint32_t schema_len;
	uint32_t *key;				// Indexes of columns which form the GROUP key (used in GROUP BY)
	uint32_t key_len;			// Length of the GROUP key index (columns * FOUR_BYTES)
	uint32_t *distinct;			// Indexes of columns on which DISTINCT key is enabled
	uint32_t distinct_len;		// Length of DISTINCT index (columns * FOUR_BYTES)
	uint32_t slot;				// Thread slot where the storage belongs - needed to init FS layer on-demand
	uint32_t index;				// Storage ID - needed to init FS layer on-demand
	struct fs_root_s *queue;	// Disk queue
};

// FIFO
struct fifo_s {
	struct fifo_node_s *write;
	struct fifo_node_s *read;
	uint32_t size;
};

struct fifo_node_s {
	struct fifo_node_s *next;
	void *data;					// Data pointer, if saved to RAM
	uint64_t size;
	uint64_t id;				// FS Queue ID, if saved to FS Queue
	char type;
};

// Job definition
struct job_s {
	uint64_t job_id[2];
	uint64_t task_id[2];
	int index;					// The index of the slot from the parent's thread set that current thread runs in
	int status;

	int error;
	char *error_msg;

	struct data_storage_s *storage;
	uint32_t storage_len;

	uint32_t tiers_len;		
	struct tier_s *tiers;

	uint32_t functions_len;
	struct function_s *functions;

	struct fs_root_s *buckets;

	uint32_t routes_len;
	struct route_s *routes;

	uint32_t mysql_connection_id;
	MYSQL *mysql_connection;

	uint32_t stats_ip;
	int stats_port;
	int stats_task_type;		// Type of stats events to generate on this tier - NONE, SQL, EXEC
	int stats_query_type;		// Query type of this task - SELECT, INSERT etc.
	struct timeval stats_tv;
	char stats_push;			// Flag to tell us whether we have pushed data before
};

// Thread definition
struct thread_set_s {
	uint64_t job_id[2];
	int32_t tier;
	int32_t senders_pending;	// How many mappers have yet to send data
	int index;
	int busy;
	pthread_t thread_id;		// POSIX thread ID
	pid_t tid;					// Kernel thread ID
	struct fifo_s *fifo;
	struct job_s *job;
};

// Value union
union value_u {
	int64_t l;					// long
	double d;					// double
	char *s;					// char *
	struct decimal_s *dec;		// decimal_s *
};

// Accumulator DMem results
struct accumulator_s {
	union value_u value;
	uint64_t count;
	uint32_t s_len;				// string length inside accumulator
	char null;					// null flag for value
};

// Memory usage tracker
struct mem_usage_s {
	int64_t used;
	int64_t threshold;
};

// BTree node structure
struct btree_node_s {
	void *left;
	void *right;
};

// BTree root
struct btree_root_s {
	struct btree_node_s *root;
	int depth;
};

//// PROTOTYPES

// btree.c
struct btree_root_s *btree_init(int depth);
int btree_put (struct btree_root_s *root, void *key);
void btree_destroy(struct btree_root_s *root);

// common.c
void system_log(int, char *, ...);
uid_t getuidbyname(const char *);
gid_t getgidbyname(const char *);
void init_daemon(const char *, int);
void init_signal_handler(struct sigaction *, struct sigaction *);
void signal_handler (int, siginfo_t *, void *);
void *mem_alloc(unsigned int);
void *mem_realloc(void *, unsigned int);
int get_tcp_port(void);
void get_node_id(void);
void clear_ss(struct socket_storage_s *, int);
void clear_data_storage(struct data_storage_s *storage, int mode);
void cleanup_thread(int, char *);
void clear_fifo(void *data, uint64_t, char size);

// daemon-thread.c
void *thread_main(void *);

// fifo.c
struct fifo_s *fifo_new();
int fifo_write_queue(struct fifo_s *fifo, uint64_t id, uint64_t size);
int fifo_write_ram(struct fifo_s *fifo, void *data, uint64_t size);
int fifo_write_msg(struct fifo_s *fifo, int msg);
int fifo_read(struct fifo_s *fifo, void **data, uint64_t *id, uint64_t *size, char *type);
int fifo_len(struct fifo_s *);
void fifo_free(struct fifo_s *, void (*f)(void *, uint64_t, char));
int fifo_is_empty(struct fifo_s *);

// functions.c
int function_mysql(struct job_s *, void *, int);
int function_init(struct job_s *job, void *value, uint32_t value_len, struct accumulator_s *acc, int col_type);
int function_min(struct job_s *job, void *value, uint32_t value_len, struct accumulator_s *payload, int col_type);
int function_max(struct job_s *job, void *value, uint32_t value_len, struct accumulator_s *payload, int col_type);
int function_sum(struct job_s *job, void *value, uint32_t value_len, struct accumulator_s *payload, int col_type);
int function_distinct(struct job_s *job);

// messages.c
int validate_magic(char *);
void prepare_header(char *, int *, int, int);
char *prepare_message0(int *, uint64_t *, uint32_t);
char *prepare_message1(int *, struct tier_s *, int);
char *prepare_message3(int *, uint64_t *);
char *prepare_message4(int *);
char *prepare_message5(int *, uint64_t *, uint32_t, uint32_t, char *);
struct message_header_s *process_header(char *);
struct message0_s *process_message0(char *);
struct message1_s *process_message1(char *);
struct message3_s *process_message3(char *);
struct message5_s *process_message5(char *);

// network.c
int init_tcp_socket(struct sockaddr_in *, int, short, int);
int init_tcp_socket2(struct sockaddr_in *, char *, short, int);
int write_to_socket (int, char *, int);
int read_data(int, struct socket_storage_s *);

// sender.c
int sender_connect(struct job_s *job, struct route_s *route);
int sender_next_hop(struct job_s *, int storage_id, int mode);
void sender_close_all(struct job_s *, int mode);
int sender_error(struct job_s *);

// stats.c
void prepare_task_id(uint64_t *query_id, int node_id, int task_type, void *res);

//// GLOBALS

// Global variables which are needed by the cleanup signal handler
int node_id;
int host_ip;
int tcp_socket;
int log_level;
int thread_set_len;
struct thread_set_s *thread_set;
struct socket_storage_s *ss;
void *stats_dl;
int (*stats_send)(int node_id, int event, int task_type, uint64_t *task_id_bin, int query_type, uint64_t *query_id_bin, char *payload, struct timeval *tv, uint32_t host, int port);

// Global variables for thread local storage keys
pthread_key_t tls_thread_slot;

// Global read-only variables (once set)
uint32_t *btree_mask32;
uint64_t *btree_mask128;

// Memory accounting
struct mem_usage_s mem_usage;
uint32_t max_rows_per_chunk;

//// INLINE FUNCTIONS

// Extract char from buffer
static inline unsigned char extract_char(void *buffer_in) {
	unsigned char value;

	memcpy(&value, buffer_in, sizeof(char));
	return value;
}

// Extract uint16_t from buffer
static inline uint16_t extract_int16(void *buffer_in) {
	uint16_t value;

	memcpy(&value, buffer_in, sizeof(uint16_t));
	return ntohs(value);
}

// Extract uint32_t from buffer
static inline uint32_t extract_int32(void *buffer_in) {
	uint32_t value;

	memcpy(&value, buffer_in, sizeof(uint32_t));
	return ntohl(value);
}

// Extract uint64_t from buffer
static inline uint64_t extract_int64(void *buffer_in) {
	uint64_t value;

	memcpy(&value, buffer_in, sizeof(uint64_t));
	return be64toh(value);
}

