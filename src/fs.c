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

#include "fs.h"
#include "common.h"
#include "lib.h"

//// UTILITY FUNCTIONS

// Get filename for given ID
static inline char *get_filename(struct fs_root_s *root, uint64_t id) {
	char *filename;

	filename = mem_alloc(strlen(root->dir_id) + strlen(QUEUE_FILE_PREFIX) + 22);		// One "/", 20 for id (64-bit) and NULL
	sprintf(filename, "%s/%s%lu", root->dir_id, QUEUE_FILE_PREFIX, id);	

	return filename;
}

// Check if a directory exists; try to create otherwise
static inline int dir_check(char *dir) {
	int res;

	// Try to create dir (it is OK if it exists)
	res = mkdir(dir, S_IRWXU|S_IRGRP|S_IXGRP);
	if (res && (errno != EEXIST)) {
		if (errno == ENOTDIR)
			system_log(LOG_ERR, "File exists and is not a directory: %s: %s", dir, strerror(errno));
		else
			system_log(LOG_ERR, "Unable to create directory %s: %s", dir, strerror(errno));

		return 1;
	}

	return 0;
}

// Reset bucket
static inline void bucket_reset(struct bucket_s *bucket, void *header, uint32_t header_len) {
	bucket->fd = -1;
	bucket->row_count = 0;
	bucket->location = FS_LOCATION_RAM;

	if (header) {
		bucket->data = mem_alloc(header_len);
		bucket->len = header_len;
		bucket_write(bucket, 0, header, header_len);
		mem_usage.used += header_len;
	}
	else {
		bucket->data = 0;
		bucket->len = 0;
	}
}

//// FILESYSTEM (COMMON) FUNCTIONS

// Init a new fs hierarchy
struct fs_root_s *fs_init(int type, uint32_t slot, uint32_t id) {
	struct fs_root_s *root;
	int i;
	uint32_t offset=0, ui32, header_len=MESSAGE_HEADER_SIZE + FOUR_BYTES;
	char *dir, *dir_custom, *dir_prefix, *file_prefix, header[header_len];

	// Init root
	root = mem_alloc(sizeof(struct fs_root_s));

	// Find out which directory to use (with fallbacks to common system variables or build-ins)
	switch(type) {
		case DEXEC_FS_QUEUE:
			dir = getenv("MAMMOTHDB_DEXEC_QUEUE_DIR");
			dir_prefix = QUEUE_DIR_PREFIX;
			file_prefix = QUEUE_FILE_PREFIX;
			dir_custom = QUEUE_DIR;
			break;
		case DEXEC_FS_BUCKET:
			dir = getenv("MAMMOTHDB_DEXEC_BUCKET_DIR");
			dir_prefix = BUCKET_DIR_PREFIX;
			file_prefix = BUCKET_FILE_PREFIX;
			dir_custom = BUCKET_DIR;
			break;
	}
	if (! dir) {
		if (getenv("MAMMOTHDB_DEXEC_TMPDIR"))
			dir = getenv("MAMMOTHDB_DEXEC_TMPDIR");
		else if (getenv("MAMMOTHDB_TMPDIR"))
			dir = getenv("MAMMOTHDB_TMPDIR");
		else 
			dir = dir_custom;
	}

	// Check if root dir exists; make otherwise
	if (dir_check(dir)) {
		free(root);
		return NULL;
	}

	// Prepare slot directory
	root->dir_slot = mem_alloc(strlen(dir) + strlen(dir_prefix) + 12);	// one "/"; 10 digits for slot; NULL
	sprintf(root->dir_slot, "%s/%s%u", dir, dir_prefix, slot);
	if (dir_check(root->dir_slot)) {
		free(root->dir_slot);
		free(root);
		return NULL;
	}

	// Check if second subdir exists; make otherwise
	root->dir_id = mem_alloc(strlen(root->dir_slot) + 12);			// one "/"; 10 digits for id; NULL
	sprintf(root->dir_id, "%s/%u", root->dir_slot, id);
	if (dir_check(root->dir_id)) {
		free(root->dir_id);
		free(root->dir_slot);
		free(root);
		return NULL;
	}

	root->type = type;
	root->slot = slot;
	root->id = id;

	// Type-specific init
	switch(type) {
		case DEXEC_FS_QUEUE:
			// Int queue (as on-disk files)
			root->queue.counter = 0;
			break;

		case DEXEC_FS_BUCKET:
			// Get a blank header of type DATA and length 0 (ignore the returned length as we have a larger block to accomodate for row count)
			prepare_header(&header[0], &i, DEXEC_MESSAGE_DATA, 0);

			// Init buckets
			for (i=0; i<BUCKET_COUNT; i++) {
				// Set file name in bucket
				root->buckets[i].file = mem_alloc(strlen(root->dir_id) + strlen(file_prefix) + 5); // one "/"; 3 digits for index; NULL
				sprintf(root->buckets[i].file, "%s/%s%u", root->dir_id, file_prefix, i);

				// Reset bucket
				bucket_reset(&root->buckets[i], &header[0], header_len);
			}
			break;
	}

	return root;
}

// Read a file (and sometimes delete it from disk)
int fs_read(struct fs_root_s *root, uint64_t id, void **data, uint64_t *len) {
	int fd;
	struct stat filestat;
	char *filename = NULL, *data_in;
	uint64_t len_in;

	// Determine the file to read
	switch(root->type) {
		case DEXEC_FS_QUEUE:
			filename = get_filename(root, id);

			if (stat(filename, &filestat) == -1) {
				system_log(LOG_ERR, "Unable to get stat for file %s: %s", filename, strerror(errno));
				free(filename);
				*data = NULL;
				*len = 0;
				return DEXEC_ERROR_FILESYSTEM;
			}

			if ((fd = open(filename, O_RDONLY)) == -1) {
				system_log(LOG_ERR, "Unable to open file %s: %s", filename, strerror(errno));
				free(filename);
				*data = NULL;
				*len = 0;
				return DEXEC_ERROR_FILESYSTEM;
			}

			len_in = filestat.st_size;

			break;

		case DEXEC_FS_BUCKET:
			// If the data is still in RAM, return a pointer to it and remove from bucket
			if (root->buckets[id].location == FS_LOCATION_RAM) {
				*len = root->buckets[id].len;
				*data = root->buckets[id].data;

				root->buckets[id].data = NULL;
				root->buckets[id].len = 0;

				return DEXEC_OK;
			}

			// If data is on the disk, rewind FD to beginning of file
			if ((lseek(root->buckets[id].fd, 0, SEEK_SET)) == -1) {
				system_log(LOG_ERR, "Unable to set bucket offset to 0 file %s: %s", root->buckets[id].file, strerror(errno));
				return DEXEC_ERROR_FILESYSTEM;
			}

			filename = root->buckets[id].file;
			fd = root->buckets[id].fd;
			len_in = root->buckets[id].len;
			break;
	}

	data_in = mem_alloc(len_in);
	mem_usage.used += len_in;

	len_in = len_in;

	if ((read(fd, data_in, len_in)) == -1) {
		system_log(LOG_ERR, "Unable to read %lu bytes from file %s: %s", filestat.st_size, filename, strerror(errno));
		close(fd);
		if (data)
			*data = NULL;
		if (len)
			*len = 0;

		mem_usage.used -= len_in;

		free(filename);

		return DEXEC_ERROR_FILESYSTEM;
	}

	// Close FD and unlink
	switch(root->type) {
		case DEXEC_FS_QUEUE:
			*data = data_in;
			*len = len_in;

			// Close FD and unlink file (consider failure non-critical)
			if (close(fd) == -1)
				system_log(LOG_ERR, "Unable to close file %s: %s", filename, strerror(errno));

			if (unlink(filename) == -1)
				system_log(LOG_ERR, "Unable to unlink queue file %s: %s", filename, strerror(errno));

			free(filename);
			break;

		case DEXEC_FS_BUCKET:
			*data = data_in;
			*len = len_in;
			break;
	}

	return DEXEC_OK;
}

// Destroy a fs hierarchy
int fs_destroy(struct fs_root_s *root, int mode) {
	int i;
	DIR *dir;
	struct dirent *ep;
	char *filename;

	switch(root->type) {
		case DEXEC_FS_QUEUE:
			if ((dir = opendir(root->dir_id)) == NULL) {
				if (errno != ENOENT)
					system_log(LOG_ERR, "Unable to read queue directory %s: %s", root->dir_id, strerror(errno));
				break;
			}

			while (ep = readdir(dir)) {
				// Only unlink files - needed to avoid "." and ".."
				if (ep->d_type == DT_REG) {
					filename = mem_alloc(strlen(root->dir_id) + strlen(ep->d_name) + 2);
					sprintf(filename, "%s/%s", root->dir_id, ep->d_name);
					if (unlink(filename) == -1)
						system_log(LOG_ERR, "Unable to unlink queue file %s: %s", filename, strerror(errno));
					free(filename);
				}
			}

			if (closedir(dir) == -1)
				system_log(LOG_ERR, "Unable to close queue directory %s: %s", root->dir_id, strerror(errno));

			break;

		case DEXEC_FS_BUCKET:
			// Loop around buckets: close files, remove from disk
			for (i=0; i<BUCKET_COUNT; i++) {
				// If data is in RAM, free it
				if (root->buckets[i].location == FS_LOCATION_RAM) { 
					if (root->buckets[i].data) {
						free(root->buckets[i].data);
						mem_usage.used -= root->buckets[i].len;
					}
				}
				else if (root->buckets[i].location == FS_LOCATION_DISK) {
					// Else, if data is on disk, close file and remove it
					if ((close(root->buckets[i].fd)) == -1)
						system_log(LOG_ERR, "Unable to close bucket file %s: %s", root->buckets[i].file, strerror(errno));

					if (unlink(root->buckets[i].file) == -1)
						system_log(LOG_ERR, "Unable to unlink bucket file %s: %s", root->buckets[i].file, strerror(errno));
				}

				if (mode == DEXEC_CLEAN_RESET)
					bucket_reset(&root->buckets[i], NULL, 0);
				else if (mode == DEXEC_CLEAN_FREE)
					free(root->buckets[i].file);
			}
			break;
	}

	// Free resources
	if (mode == DEXEC_CLEAN_FREE) {
		// Remove directories (slot - only if it is empty)
		if ((rmdir(root->dir_id)) == -1)
			system_log(LOG_ERR, "Unable to unlink directory %s: %s", root->dir_id, strerror(errno));
		if ((rmdir(root->dir_slot) == -1) && (errno != ENOTEMPTY))
			system_log(LOG_ERR, "Unable to unlink directory %s: %s", root->dir_id, strerror(errno));

		free(root->dir_id);
		free(root->dir_slot);
		free(root);  
	}

	return DEXEC_OK;
}

//// QUEUE SPECIFIC FUNCTIONS

// Add a file
int queue_write(struct fs_root_s *root, void *data, uint64_t len, uint64_t *id) {
	int fd;

	root->queue.counter++;
	*id = root->queue.counter;

	return queue_overwrite(root, data, len, *id);
}

// Overwrite an existing file
int queue_overwrite(struct fs_root_s *root, void *data, uint64_t len, uint64_t id) {
	int fd;
	char *filename;

	filename = get_filename(root, id);

	if ((fd = open(filename, O_RDWR|O_CREAT|O_TRUNC, S_IRUSR|S_IWUSR|S_IRGRP)) == -1) {
		system_log(LOG_ERR, "Unable to create new queue file %s: %s", filename, strerror(errno));
		free(filename);
		return DEXEC_ERROR_FILESYSTEM;
	}

	if (ftruncate(fd, len) < 0) {
		system_log(LOG_ERR, "Unable to resize queue file %s to size %lu: %s", filename, len, strerror(errno));
		free(filename);
		close(fd);
		return DEXEC_ERROR_FILESYSTEM;
	}

	if (write(fd, data, len) == -1) {
		system_log(LOG_ERR, "Unable to write %lu bytes to queue file %s: %s", len, filename, strerror(errno));
		close(fd);
		unlink(filename);
		free(filename);
		return DEXEC_ERROR_FILESYSTEM;
	}

	if (close(fd) == -1) {
		system_log(LOG_ERR, "Unable to close queue file %s: %s", filename, strerror(errno));
		free(filename);
		return DEXEC_ERROR_FILESYSTEM;
	}

	free(filename);

	return DEXEC_OK;
}

//// BUCKET SPECIFIC FUNCTIONS

// Write to bucket
int bucket_write(struct bucket_s *bucket, uint32_t offset, void *buffer, uint32_t size) {
	// If data is in RAM
	if (bucket->location == FS_LOCATION_RAM) {
		// Check if we need to realloc
		if ((offset + size) > bucket->len) {
			bucket->data = mem_realloc(bucket->data, offset + size);
			mem_usage.used += (offset + size - bucket->len);
			bucket->len = offset + size;
		}

		// Write data
		memcpy(bucket->data + offset, buffer, size);

		// Check if we need to write it to disk
		if (mem_usage.used > mem_usage.threshold) {
			// Create disk file
			if ((bucket->fd = open(bucket->file, O_RDWR|O_CREAT|O_TRUNC, S_IRUSR|S_IWUSR|S_IRGRP)) == -1) {
				system_log(LOG_ERR, "Unable to create bucket file %s: %s", bucket->file, strerror(errno));
				return DEXEC_ERROR_FILESYSTEM;
			}

			// Write the data
			if ((write(bucket->fd, bucket->data, bucket->len)) == -1) {
				system_log(LOG_ERR, "Unable to write to bucket file %s: %s", bucket->file, strerror(errno));
				return DEXEC_ERROR_FILESYSTEM;
			}

			bucket->location = FS_LOCATION_DISK;
			free(bucket->data);
			bucket->data = NULL;
			mem_usage.used -= bucket->len;
		}

		return DEXEC_OK;
	}

	// Since we don't know where last WRITE was, always SEEK
	if ((lseek(bucket->fd, offset, SEEK_SET)) == -1) {
		system_log(LOG_ERR, "Unable to set bucket offset to %u file %s: %s", offset, bucket->file, strerror(errno));
		return DEXEC_ERROR_FILESYSTEM;
	}

	// Write to disk
	if ((write(bucket->fd, buffer, size)) == -1) {
		system_log(LOG_ERR, "Unable to write to bucket file %s: %s", bucket->file, strerror(errno));
		return DEXEC_ERROR_FILESYSTEM;
	}

	if ((offset + size) > bucket->len)
		bucket->len = offset + size;

	return DEXEC_OK;
}

// Append to bucket
int bucket_append(struct bucket_s *bucket, void *buffer, uint32_t size) {
	return bucket_write(bucket, bucket->len, buffer, size);
}

