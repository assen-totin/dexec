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

#include <stdint.h>

#define QUEUE_DIR "/tmp"				// Root directory for queued files
#define QUEUE_DIR_PREFIX "mdb-dexec-queue-"
#define QUEUE_FILE_PREFIX "queue-"

#define BUCKET_COUNT	256
#define BUCKET_DIR	"/tmp"
#define BUCKET_DIR_PREFIX	"mdb-dexec-buckets-"
#define BUCKET_FILE_PREFIX	"bucket-"

enum {
	FS_LOCATION_NONE = 0,
	FS_LOCATION_RAM,
	FS_LOCATION_DISK
};

struct queue_s {
	uint64_t counter;
};

struct bucket_s {
	int fd;
	char *file;
	void *data;
	uint32_t row_count;
	uint32_t len;
	char location;
};

struct fs_root_s {
	uint32_t slot;			// Thread slot
	uint32_t id;			// Bucket instance ID inside the slot
	char *dir_slot;			// First-level directory: thread slot ID
	char *dir_id;			// Second-level directory: bucket instance ID
	int type;				// From the enum
	int depth;				// Btree depth in bits
	struct bucket_s buckets[256];
	struct queue_s queue;
};

struct fs_root_s *fs_init(int type, uint32_t slot, uint32_t instance_id);
int fs_read(struct fs_root_s *root, uint64_t id, void **data, uint64_t *len);
int fs_destroy(struct fs_root_s *root, int mode);

int bucket_seek(struct bucket_s *bucket, uint32_t offset);
int bucket_write(struct bucket_s *bucket, uint32_t offset, void *buffer, uint32_t size);
int bucket_append(struct bucket_s *bucket, void *buffer, uint32_t size);

int queue_write(struct fs_root_s *root, void *data, uint64_t len, uint64_t *id);
int queue_overwrite(struct fs_root_s *root, void *data, uint64_t len, uint64_t id);

