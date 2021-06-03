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

// INit TCP socket from int
// NB: The "ip" param must be in MSB!
int init_tcp_socket(struct sockaddr_in *my_addr, int ip, short port, int bind_socket) {
	int sockfd;

	if ((sockfd = socket (AF_INET, SOCK_STREAM, 0)) == -1) {
		system_log(LOG_ERR, "Unable to create TCP socket.");
		return -1;
	}

	// Zero the structure, set socket type and port
	memset(my_addr, '\0', sizeof(struct sockaddr_in));
	my_addr->sin_family = AF_INET;						
	my_addr->sin_port = htons(port);

	// Set IP address (must be provided in LSB!)
	my_addr->sin_addr.s_addr = htonl(ip);

	// If binding is requested, bind to socket
	if ( bind_socket && (bind(sockfd, (const struct sockaddr *) my_addr, sizeof(*my_addr)) == -1) ) {
		system_log(LOG_ERR, "Unable to bind to TCP socket: %s.", strerror(errno));
		return -1;
	}

	return sockfd;
}

// Init TCP socket from string
int init_tcp_socket2(struct sockaddr_in *my_addr, char *host, short port, int bind_socket) {
	struct in_addr tmp_addr;
	uint32_t ip;

	// If host is not set, set to any local
	if (! host)
		ip = htonl(INADDR_ANY);
	else {
		if (inet_aton(host, &tmp_addr) == 0) {
			system_log(LOG_ERR, "Unable to set host %s: %s", host, strerror(errno));
			return -1;
		}

		ip = ntohl(tmp_addr.s_addr);
	}

	return init_tcp_socket(my_addr, ip, port, bind_socket);
}

// Read from socket
int read_from_socket (int socket, int size, char *data, int offset) {
	int bytes;
	bzero(data + offset, size);

	system_log(LOG_DEBUG, "Trying to read %i bytes from socket %u and place them at offset %u", size, socket, offset);

	bytes = read(socket, data + offset, size);
	system_log(LOG_DEBUG, "Read %i bytes from socket %u", bytes, socket);

	if (bytes < 0) {
		// Read error
		system_log(LOG_ERR, "Unable to read from socket %u", socket);
		return -1;
	}
	else if (bytes == 0) {
		// EOF
		system_log(LOG_ERR, "Client has closed connection on socket %u", socket);
		return -1;
	}
	else 
		return bytes;
}

// Read a block of data and save it to socket storage 
int read_data(int socket, struct socket_storage_s *ss) {
	int bytes, to_read;
	char *buffer;
	struct message_header_s *header;

	// If we have been force disocnnected, return
	if (! ss->connected)
		return -1;

	// If we have not yet read from this socket, init its header
	if (! ss->buffer) {
		ss->buffer = mem_alloc(MESSAGE_HEADER_SIZE);
		ss->remaining = MESSAGE_HEADER_SIZE;
		ss->size = MESSAGE_HEADER_SIZE;
	}

	// Read data
	if ((bytes = read_from_socket(socket, ss->remaining, ss->buffer, ss->sofar)) < 0) {
		system_log(LOG_ERR, "Unable to read from socket: %s", strerror(errno));
		return -1;
	}

	// Record the bytes read, see if we need to wait for more
	ss->sofar += bytes;
	ss->remaining -= bytes;
	if (ss->remaining)
		return 0;

	// Check if we need to process header
	if (ss->size == MESSAGE_HEADER_SIZE) {
		// Process the header
		header = process_header(ss->buffer);

		// Validate magic, drop connection on error
		if (validate_magic(&header->magic[0])) {
			free(header);
			return -1;
		}

		ss->size = header->size;
		ss->remaining = header->size - ss->sofar;
		ss->type = header->type;
		free(header);

		// If there will be no body, set flag
		if (ss->size == MESSAGE_HEADER_SIZE) {
			ss->message_complete = 1;
			return 0;
		}

		// If the message size is bigger than header, create space for it
		buffer = mem_alloc(ss->size);
		memcpy(buffer, ss->buffer, MESSAGE_HEADER_SIZE);
		free(ss->buffer);
		ss->buffer = buffer;

		return 0;
	}

	ss->message_complete = 1;

	return 0;
}

// Write to a TCP socket
int write_to_socket (int socket, char *data, int len) {
	int bytes;

	if ((bytes = send (socket, data, len, MSG_NOSIGNAL)) < 0)  
		system_log(LOG_ERR, "Unable to send to remote socket %i: %s", socket, strerror(errno));

	system_log(LOG_DEBUG, "Wrote %i bytes from %i requested to socket %i", bytes, len, socket);

	return bytes;
}

