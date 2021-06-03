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
#include "functions-common.h"

// Check if a key matches the mask
static inline int match(void *key, int depth, int level) {
	switch (depth) {
		case BTREE_DEPTH_32:
			if (*(uint32_t *)key & *(btree_mask32 + level * FOUR_BYTES))
				return 1;
		break;

		case BTREE_DEPTH_128:
			if (level >= BTREE_DEPTH_128 / 2) {
				// Check higher 64 bits
				if (*((uint64_t *)(key + EIGHT_BYTES)) & *(btree_mask128 + (2 * level + 1) * EIGHT_BYTES))
					return 1;
			}
			else {
				// Check lower 64 bits
				if (*((uint64_t *)key) & *(btree_mask128 + (2 * level) * EIGHT_BYTES))
					return 1;
			}
		break;
	}

	return 0;
}

static inline void *add_node() {
	void *new;

	new = mem_alloc(sizeof(struct btree_node_s));
	((struct btree_node_s *)new)->left = NULL;
	((struct btree_node_s *)new)->right = NULL;

	return new;
}

struct btree_root_s *btree_init(int depth) {
	struct btree_root_s *root = mem_alloc(sizeof(struct btree_root_s));

	root->depth = depth;

	root->root = mem_alloc(sizeof (struct btree_node_s));
	root->root->left = NULL;
	root->root->right = NULL;

	return root;
}

int btree_put (struct btree_root_s *root, void *key) {
	int i, ret = BTREE_RESULT_FOUND;
	void *branch = root->root;

	for (i=0; i < root->depth; i++) {
		if (match(key, root->depth, i)) {
			// Bit is 1, we need to go right
			if (! ((struct btree_node_s *)branch)->right) {
				((struct btree_node_s *)branch)->right = add_node();
				ret = BTREE_RESULT_ADDED;
			}
			branch = ((struct btree_node_s *)branch)->right;
		}
		else {
			// Bit is 0, we need to go left
			if (! ((struct btree_node_s *)branch)->left) {
				((struct btree_node_s *)branch)->left = add_node();
				ret = BTREE_RESULT_ADDED;
			}
			branch = ((struct btree_node_s *)branch)->left;
		}
	}

	return ret;
}

// Remove all elements from the tree
// NB: This function is invoked recursively!
static inline void btree_purge(int depth, void *branch, int level) {
	// At last level, free payload
	if (level == depth) {
		free(((struct btree_node_s *)branch)->left);
		return;
	}

	// Dive further
	if (((struct btree_node_s *)branch)->left) {
		btree_purge(depth, ((struct btree_node_s *)branch)->left, level + 1);
		// Free our next level
		free(((struct btree_node_s *)branch)->left);
	}

	if (((struct btree_node_s *)branch)->right) {
		btree_purge(depth, ((struct btree_node_s *)branch)->right, level + 1);
		// Free our next level
		free(((struct btree_node_s *)branch)->right);
	}
}

void btree_destroy(struct btree_root_s *root) {
	if (! root)
		return;

	btree_purge(root->depth, root->root, 0);
	free(root->root);
	free(root);
}

