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
#include "murmur3.h"

// Create unique task ID from query ID and other params
// res pointer should be able to store 128 bits!
void prepare_task_id(uint64_t *query_id, int node_id, int task_type, void *res) {
	char *data = mem_alloc(64);		// Currently, only 40 used
	int offset = 0;
	struct timeval tv;

	// Copy input to buffer
	memcpy(data + offset, query_id, 2 * EIGHT_BYTES);
	offset += 2 * EIGHT_BYTES;

	memcpy(data + offset, &node_id, FOUR_BYTES);
	offset += FOUR_BYTES;

	memcpy(data + offset, &task_type, FOUR_BYTES);
	offset += FOUR_BYTES;

	gettimeofday(&tv, NULL);
	memcpy(data + offset, &tv.tv_sec, sizeof(time_t));
	offset += sizeof(time_t);
	memcpy(data + offset, &tv.tv_usec, sizeof(suseconds_t));
	offset += sizeof(suseconds_t);

	//TODO: Consider using the buffer as ID instead of hashing to save CPU
	murmur3(data, offset, TRULY_RANDOM_NUMBER, res);

	free(data);
}


