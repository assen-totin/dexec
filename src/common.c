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
#include "decimal.h"
#include "fs.h"

// Allocate memory (and optionally exit on failure)
void *mem_alloc(unsigned int size) {
	void *p;

	if (! size) {
		system_log(LOG_WARNING, "Nothing to allocate");
		return NULL;
	}

	// Refuse to allocate too much memory
	if (size > MAX_MALLOC_SIZE) {
		system_log(LOG_ERR, "Refusing to allocate %u bytes of memory: block too large (limit is %u)", size, MAX_MALLOC_SIZE);
		exit(EXIT_FAILURE);
	}

	// Allocate from RAM
	if (! (p = malloc(size))) {
		system_log(LOG_CRIT, "Failed to allocate %u bytes of memory", size);
		exit(ENOMEM);
	}

	bzero(p, size);

	return p;
}

// Re-allocate memory (and optionally exit in failure)
void *mem_realloc(void *source, unsigned int size) {
	// Refuse to allocate too much memory
	if (size > MAX_MALLOC_SIZE) {
		system_log(LOG_ERR, "Refusing to allocate %u bytes of memory: block too large (limit is %u)", size, MAX_MALLOC_SIZE);
		return NULL;
	}

	void *p = realloc(source, size);

	if (! p) {
		system_log(LOG_CRIT, "Failed to reallocate %u bytes of memory", size);
		exit(ENOMEM);
	}

	return p;
}

// Chose a TCP port to work with
int get_tcp_port() {
	char *env;

	// Decide which TCP port to use
	if (( env = getenv("MAMMOTHDB_DEXEC_PORT")) )
		return atoi(env);
	else
		return DEFAULT_TCP_PORT;
}

// Daemon init
void init_daemon(const char *ident, int drop_privileges) {
	pid_t pid;
	uid_t uid;
	gid_t gid;
	char *mdb_log_level;

// Disable forking when compiling with SystemD
#ifndef HAVE_SYSTEMD
	// Initial fork to become a daemon
	pid = fork();
	if (pid < 0) {
		system_log(LOG_CRIT, "Unable to fork a daemon process. Exiting.");
		exit(1); 
	}

	// Parent goes away immediately
	if (pid > 0) 
		exit(0);

	// Make the process a group leader, session leader, and lose control tty 
	setsid();

	// Close file descriptor
	close(STDIN_FILENO);
	close(STDOUT_FILENO);
	close(STDERR_FILENO);

	// Lose file creation mode mask inherited by parent
	umask(0); 

	// Change to working dir
	chdir("/");
#endif

	// Obtain current log level
	mdb_log_level = getenv("MAMMOTHDB_LOG4J_LEVEL");

	// Determine actual log level to use (log_level is a global variable)
	if (!mdb_log_level)
		log_level = DEFAULT_LOG_LEVEL;
	else if (! strcasecmp(mdb_log_level, "DEBUG"))
		log_level = LOG_DEBUG;
	else if (! strcasecmp(mdb_log_level, "INFO"))
		log_level = LOG_INFO;
	else if (! strcasecmp(mdb_log_level, "WARN"))
		log_level = LOG_WARNING;
	else if (! strcasecmp(mdb_log_level, "ERROR"))
		log_level = LOG_ERR;

	// Init syslog as LOG_DAEMON
	openlog(ident, LOG_CONS, LOG_LOCAL5);
	system_log(LOG_DEBUG, "Daemon process created.");

	// Drop privileges to user mammothdb
	if ((getuid() == 0) && drop_privileges) {
		gid = getgidbyname(RUN_AS_USER);
		if ((int)gid < 0)
			system_log(LOG_WARNING, "Unable to get GID for username %s", RUN_AS_USER);
		else if (setgid(gid) != 0)
			system_log(LOG_WARNING, "Unable to drop group privileges: %s", strerror(errno));
		else
			system_log(LOG_NOTICE, "Dropped group privileges to user %s (gid=%i)", RUN_AS_USER, gid);

		uid = getuidbyname(RUN_AS_USER);
		if ((int)uid < 0)
			system_log(LOG_WARNING, "Unable to get UID for username %s", RUN_AS_USER);
	    else if (setuid(uid) != 0)
			system_log(LOG_WARNING, "Unable to drop user privileges: %s", strerror(errno));
		else
			system_log(LOG_NOTICE, "Dropped user privileges to user %s (uid=%i)", RUN_AS_USER, uid);
	}
}

// Get user's UID by his name
uid_t getuidbyname(const char *name) {
	if(name) {
		struct passwd *pwd = getpwnam(name); // don't free, see 'man getpwnam' for details
		if(pwd) 
			return pwd->pw_uid;
	}

	return -1;
}

// Get user's GID by his name
gid_t getgidbyname(const char *name) {
	if(name) {
		struct passwd *pwd = getpwnam(name); // don't free, see 'man getpwnam' for details
		if(pwd) 
			return pwd->pw_gid;
	}

	return -1;
}

// Signal handler init
void init_signal_handler(struct sigaction *old_action, struct sigaction *new_action) {
	// Handle SIGINT, SIGHUP and SIGTERM
	new_action->sa_sigaction = signal_handler;
	new_action->sa_flags = SA_SIGINFO|SA_RESTART;
	sigemptyset (&new_action->sa_mask);

	sigaction (SIGINT, NULL, old_action);
	if (old_action->sa_handler != SIG_IGN)
		sigaction (SIGINT, new_action, NULL);

	sigaction (SIGHUP, NULL, old_action);
	if (old_action->sa_handler != SIG_IGN)
		sigaction (SIGHUP, new_action, NULL);

	sigaction (SIGTERM, NULL, old_action);
	if (old_action->sa_handler != SIG_IGN)
		sigaction (SIGTERM, new_action, NULL);

	sigaction (SIGCHLD, NULL, old_action);
	if (old_action->sa_handler != SIG_IGN)
		sigaction (SIGCHLD, new_action, NULL);

	sigaction (SIGUSR2, NULL, old_action);
	if (old_action->sa_handler != SIG_IGN)
		sigaction (SIGUSR2, new_action, NULL);
}

// Signal handler for SIGTERM, SIGINT, SIGHUP
void signal_handler (int signo, siginfo_t *sinfo, void *context) {
	extern int tcp_socket;
	extern struct thread_set_s *thread_set;
	extern int thread_set_len;
	int i, err;

	system_log(LOG_NOTICE, "Received signal %u", signo);

	switch(signo) {
		case SIGUSR2:
			cleanup_thread(DEXEC_JOB_STATUS_KILLED, "Killed.");
			pthread_exit(NULL);
			break;
		case SIGINT:
		case SIGHUP:
		case SIGTERM:
			system_log(LOG_NOTICE, "Shutting down on signal %u", signo);

			// Kill worker threads
			system_log(LOG_DEBUG, "Stopping threads...");
			for (i=0; i<thread_set_len; i++) {
				if (thread_set[i].busy) {
					if ((err = pthread_kill(thread_set[i].thread_id, SIGUSR2)) > 0)
						system_log(LOG_ERR, "Unable to send SIGUSR2 to thread %lu: %s", thread_set[i].thread_id, strerror(err));
					if ((err = pthread_join(thread_set[i].thread_id, NULL)) > 0)
						system_log(LOG_ERR, "Unable to join thread %lu: %s", thread_set[i].thread_id, strerror(err));
				}
			}

			// Clear sockets
			for (i = 0; i < FD_SETSIZE; ++i) {
				clear_ss(&ss[i], DEXEC_CLEAN_FREE);
				if (ss[i].connected) {
					if ((close(i)) < 0)
						system_log(LOG_ERR, "Error closing TCP socket: %s", strerror(errno));
				}
			}

			// Close main socket
			if (tcp_socket) {
				system_log(LOG_DEBUG, "Closing TCP socket.");
				if ((close(tcp_socket)) < 0)
					system_log(LOG_ERR, "Error closing TCP socket: %s", strerror(errno));
			}

			if (thread_set)
				free(thread_set);

			pthread_key_delete(tls_thread_slot);

			mysql_library_end();

			// Close stats
#ifdef HAVE_MDB_STATS
			if (stats_send)
				dlclose(stats_dl);
#endif

			// Free DMem masks
			free(btree_mask32);
			free(btree_mask128);

			system_log(LOG_NOTICE, "Shutting down complete.");

			exit(0);
	}
}

// Logging
void system_log(int msg_facility_level, char *format, ...) {
	int mask, msg_level;
	va_list argptr;

	// Extract the last 3 bits which denote the log level
	mask = 7;
	msg_level = mask & msg_facility_level;

	// Update format with kernel thread id
	char newformat[strlen(format) + 32];
	snprintf(&newformat[0], strlen(format) + 32, "[%lu] %s", syscall(SYS_gettid), format);

	if (msg_level <= log_level) {
#ifdef IS_CLI
		// Give IS_CLI priority as it may be compild with or without HAVE_STSYEMD
		va_start(argptr, format);
		vprintf(format, argptr);
		printf("\n");
		fflush(stdout);
#elif HAVE_SYSTEMD
		va_start(argptr, &newformat[0]);

		//FIXME: we want to avoid sening LOG_DEBUG to journal, but want it in syslog
		// See https://bugzilla.redhat.com/show_bug.cgi?id=1370195
/*
		if (msg_level == LOG_DEBUG)
			...
		else
*/
		sd_journal_printv(msg_level, &newformat[0], argptr);
#else
		va_start(argptr, &newformat[0]);

		// Send everything to syslog
		vsyslog(msg_facility_level, &newformat[0], argptr);
#endif

		va_end(argptr);
	}
}

// Get Node ID from environment and current IP addresses
void get_node_id() {
	struct ifaddrs *ifaddr, *ifa;
	struct in_addr tmp_addr;
	int family, s, n, i;
	char host[NI_MAXHOST];

	int cnt = 1;
	char **c, *c1, *c2, *worker_nodes;

	// Defaults
	node_id = -1;
	host_ip = 0;

	// Get worker's list from env
	worker_nodes = getenv("MAMMOTHDB_ENVIRONMENT_WORKERS");
	if (! worker_nodes)
		return;

	c = mem_alloc(sizeof(char *));

	c1 = strtok(&worker_nodes[0], ",");
	c[0] = mem_alloc(strlen(c1) + 1);
	strcpy(c[0], c1);

	while (c1 = strtok(NULL, ",")) {
		c = realloc(c, (cnt + 1) * sizeof(char *));
		c[cnt] = mem_alloc(strlen(c1) + 1);
		strcpy(c[cnt], c1);
		cnt++;
	}

	struct node_s nodes[cnt];
	for (i=0; i<cnt; i++) {
		c2 = strtok(c[i], ":");
		nodes[i].ip = mem_alloc(strlen(c2) + 1);
		strcpy(nodes[i].ip, c2);

		c2 = strtok(NULL, ":");
		nodes[i].node_id = atoi(c2);
	}

	// Get local IP addresses
	if (getifaddrs(&ifaddr) == -1) {
		system_log(LOG_CRIT, "Unable to get local network interfaces");
		exit(EXIT_FAILURE);
	}

	// Walk through linked list, maintaining head pointer so we can free list later
	for (ifa = ifaddr, n = 0; ifa != NULL; ifa = ifa->ifa_next, n++) {
		if (ifa->ifa_addr == NULL)
			continue;

		family = ifa->ifa_addr->sa_family;
		if (family == AF_INET) {
			if ((s = getnameinfo(ifa->ifa_addr,
				(family == AF_INET) ? sizeof(struct sockaddr_in) : sizeof(struct sockaddr_in6),
				host, NI_MAXHOST, NULL, 0, NI_NUMERICHOST)) != 0) {

				system_log(LOG_CRIT, "Unable to get IP address for interface: %s", gai_strerror(s));
				exit(EXIT_FAILURE);
			}

			// See if we have this in the list of nodes; set node_id and remember host_ip
			for (i=0; i<cnt; i++) {
				if (! strcmp(nodes[i].ip, host)) {
					node_id = nodes[i].node_id;

				if (inet_aton(nodes[i].ip, &tmp_addr) == 0)
					system_log(LOG_ERR, "Unable to resolve IP address: %s", nodes[i].ip);
				else
					host_ip = ntohl(tmp_addr.s_addr);

					break;
				}
			}
		}

		if (node_id > -1)
			break;
	}

	// Cleanup
	for(i=0; i<cnt; i++) {
		free(c[i]);
		free(nodes[i].ip);
	}
	free(c);

	freeifaddrs(ifaddr);

	system_log(LOG_NOTICE, "Setting Node ID to %i", node_id);
}

void cleanup_thread(int mode, char *error) {
	// Thread handler (needed for cleanup)
	struct thread_set_s *self = (struct thread_set_s *)pthread_getspecific(tls_thread_slot);
	if (! self)
		return;

	// Safety net agains race condition between regular cleanup and KILL-induced one
	// NB: self->tier is 32-bit, hence atomic on x86_64
	if (self->tier < 0)
		return;

	// Remove ourselves from matching list as early as possible
	self->tier = -1;
	self->job_id[0] = 0;
	self->job_id[1] = 0;

	int i, j, stats_event;
	struct chunk_s *chunk, *chunk_parent;

	// Clean up socket storage and job
	system_log(LOG_INFO, "Starting thread clean-up...");

#ifdef HAVE_MDB_STATS
	// If stats is enabled, send a packet as per the cleanup mode
	if (stats_send && self->job->stats_task_type) {
		switch(mode) {
			case DEXEC_JOB_STATUS_COMPLETE:
				stats_event = STATS_EVENT_WN_DONE;
			case DEXEC_JOB_STATUS_FAILURE:
				stats_event = STATS_EVENT_WN_FAIL;
			case DEXEC_JOB_STATUS_KILLED:
				stats_event = STATS_EVENT_WN_KILL;
		}

		// Chose error
		if (! error)
			error = self->job->error_msg;

		if ((*(stats_send))(node_id, stats_event, self->job->stats_task_type, &self->job->task_id[0], self->job->stats_query_type, &self->job->job_id[0], NULL, NULL, self->job->stats_ip, self->job->stats_port) < 0)
			system_log(LOG_ERR, "Error sending stats message.");
	}
#endif

	// Clean up storage
	if (self->job->storage) {
		for (i=0; i < self->job->storage_len; i++)
			clear_data_storage(&self->job->storage[i], DEXEC_CLEAN_FREE);
		free(self->job->storage);
	}

	// Clean up buckets
	if (self->job->buckets)
		fs_destroy(self->job->buckets, DEXEC_CLEAN_FREE);

	// Clean up job tiers (functions, args, routes, sockets)
	for (i=0; i < self->job->tiers_len; i++) {
		if (self->job->tiers[i].key_len)
			free(self->job->tiers[i].key);
		if (self->job->tiers[i].distinct_len)
			free(self->job->tiers[i].distinct);
		if (self->job->tiers[i].input_schema)
			free(self->job->tiers[i].input_schema);
		if (self->job->tiers[i].output_schema)
			free(self->job->tiers[i].output_schema);
		if (self->job->tiers[i].functions) {
			for (j=0; j < self->job->tiers[i].functions_len; j++) {
				if (self->job->tiers[i].functions[j].arg)
					free(self->job->tiers[i].functions[j].arg);
				if (self->job->tiers[i].functions[j].btree)
					btree_destroy(self->job->tiers[i].functions[j].btree);
			}
			free(self->job->tiers[i].functions);
		}
		if (self->job->tiers[i].routes) {
			for (j=0; j < self->job->tiers[i].routes_len; j++) {
				if (self->job->tiers[i].routes[j].socket > -1)
					close(self->job->tiers[i].routes[j].socket);
			}
			free (self->job->tiers[i].routes);
		}
	}
	if (self->job->tiers)
		free(self->job->tiers);

	// Clean error
	if (self->job->error_msg) 
		free(self->job->error_msg);

	// Clean job
	if (self->job) {
		free(self->job);
		self->job = NULL;
	}

	// Clean up the FIFO
	if (self->fifo) {
		fifo_free(self->fifo, clear_fifo);
		self->fifo = NULL;
	}

	// Vacate thread set slot
	self->busy = 0;

	system_log(LOG_INFO, "Thread clean-up complete. Memory usage: %li", mem_usage.used);

	pthread_exit(NULL);
}

// Clear socket storage
void clear_ss (struct socket_storage_s *ss, int mode) {
	ss->size = 0;
	ss->sofar = 0;
	ss->remaining = 0;
	ss->type = 0;
	ss->message_complete = 0;

	switch(mode) {
		case DEXEC_CLEAN_FREE:
			if (ss->buffer)
				free(ss->buffer);
			// NB: No break here!
		case DEXEC_CLEAN_RESET:
			ss->buffer = NULL;
	}
}

// Clear storage
void clear_data_storage(struct data_storage_s *storage, int mode) {
	uint32_t i;

	switch(mode) {
		case DEXEC_CLEAN_CUSTOM:
			// Partial clean - remove loaded chunk entirely
			if (storage->chunk) {
				free(storage->chunk);
				mem_usage.used -= storage->chunk_len;
			}
			storage->chunk = NULL;
			if (storage->chunks) {
				storage->chunks[storage->chunk_id].data = NULL;
				storage->chunks[storage->chunk_id].size = 0;
			}

			// No break here - continue with next!

		case DEXEC_CLEAN_CUSTOM2:
			// Partial clean - only reset chunk pointer and lenght, but do not free() the chunk
			storage->chunk = NULL;
			storage->chunk_len = 0;

			// Remove view & row meta
			if (storage->row_meta)
				free(storage->row_meta);
			storage->row_meta = NULL;

			if (storage->view)
				free(storage->view);
			storage->view = NULL;

			storage->row_count = 0;

			return;	// Exit the function here!

		case DEXEC_CLEAN_FREE:
			// If chunk exists and came from FS Queue, free it 
			if (storage->chunk && (storage->chunks[storage->chunk_id].type ==  DEXEC_TYPE_QUEUE)) {
				free(storage->chunk);
				mem_usage.used -= storage->chunk_len;
			}

			if (storage->chunks) {
				for (i=0; i < storage->chunks_len; i++) {
					if (storage->chunks[i].data && (storage->chunks[i].type == DEXEC_TYPE_RAM)) {
						mem_usage.used -= storage->chunk_len;
						free(storage->chunks[i].data);
					}
				}
				free(storage->chunks);
			}

			if (storage->schema)
				free(storage->schema);

			if (storage->view)
				free(storage->view);

			if (storage->row_meta)
				free(storage->row_meta);

			if (storage->key_len)
				free(storage->key);

			if (storage->distinct_len)
				free(storage->distinct);

			if (storage->queue)
				fs_destroy(storage->queue, DEXEC_CLEAN_FREE);

			// NB: No break here!

		case DEXEC_CLEAN_RESET:
			storage->row_meta = NULL;
			storage->schema = NULL;
			storage->chunk = NULL;
			storage->chunks = NULL;
			storage->view = NULL;
			storage->key = NULL;
			storage->distinct = NULL;
			storage->queue = NULL;
	}

	storage->chunk_len = 0;
	storage->chunks_len = 0;
	storage->row_count = 0;
	storage->schema_len = 0;
	storage->key_len = 0;
	storage->distinct_len = 0;
}

// Free a FIFO element payload when destroying the FIFO queue
void clear_fifo(void *data, uint64_t size, char type) {
	if (! data)
		return;

	if (type == DEXEC_TYPE_RAM) {
		free(data);	
		mem_usage.used -= size;
	}
}

