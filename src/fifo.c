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

#include <stdlib.h>
#include <errno.h>
#include "fs.h"
#include "common.h"

// New FIFO queue
static inline void fifo_init(struct fifo_s *fifo) {
	fifo->read = NULL;
	fifo->write = NULL;
	fifo->size = 0;
}

// New FIFO queue
struct fifo_s *fifo_new() {
	struct fifo_s *fifo = mem_alloc(sizeof(struct fifo_s));
	fifo_init(fifo);
	return fifo;
}

// Add to the writing end of the fifo
static inline int fifo_write(struct fifo_s *fifo, struct fifo_node_s *node) {
	if (! fifo)
		return EINVAL;

	if (fifo->size > MAX_FIFO_SIZE)
		return ENOSPC;

	if (fifo->write == NULL)
		fifo->read = fifo->write = node;
	else {
		fifo->write->next = node;
		fifo->write = node;
	}

	fifo->size ++;

	return 0;
}

// Wapper writing functions
int fifo_write_queue(struct fifo_s *fifo, uint64_t id, uint64_t size) {
	struct fifo_node_s *node = mem_alloc(sizeof(struct fifo_node_s));

	node->id = id;
	node->size = size;
	node->type = DEXEC_TYPE_QUEUE;
	node->next = NULL;

	return fifo_write(fifo, node);
}

int fifo_write_ram(struct fifo_s *fifo, void *data, uint64_t size) {
	struct fifo_node_s *node = mem_alloc(sizeof(struct fifo_node_s));

	node->data = data;
	node->size = size;
	node->type = DEXEC_TYPE_RAM;
	node->next = NULL;

	return fifo_write(fifo, node);
}

int fifo_write_msg(struct fifo_s *fifo, int msg) {
	struct fifo_node_s *node = mem_alloc(sizeof(struct fifo_node_s));

	node->id = -1;
	node->data = NULL;
	node->size = 0;
	node->type = msg;
	node->next = NULL;

	return fifo_write(fifo, node);
}

// Remove from the reading end of the fifo
int fifo_read(struct fifo_s *fifo, void **data, uint64_t *id, uint64_t *size, char *type) {
	struct fifo_node_s *node;

	if (! fifo)
		return 0;

	if ((node = fifo->read) == NULL) {
		*data = NULL;
		*size = 0;
		return 0;
	}

	*id = node->id;	
	*data = node->data;
	*size = node->size;
	*type = node->type;

	if ((fifo->read = node->next) == NULL)
		fifo->write = NULL;

	fifo->size --;

	free(node);

	return 1;
}

// Get the length of the fifo
int fifo_len(struct fifo_s *fifo) {
	if (! fifo)
		return 0;

	return fifo->size;
}

// Free an entire fifo
// You may pass a function to also clear the payload of each element: it should be void and accept a void pointer and its size (which is char!)
void fifo_free(struct fifo_s *fifo, void (*f)(void *, uint64_t, char)) {
	void *data;
	uint64_t id, size;
	char type;

	if (! fifo)
		return;

	while(fifo_read(fifo, &data, &id, &size, &type)) {
		if (*f)
			(*f)(data, size, type);
	}

	free(fifo);
}

int fifo_is_empty(struct fifo_s *fifo) {
	if (! fifo)
		return 0;

        return (fifo->read == NULL);
}

