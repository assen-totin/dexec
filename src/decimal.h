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

// DEFINITIONS
#define DECIMAL_SIGN 1			// Size of sign in bytes
#define DECIMAL_CHUNK 8			// Chunk size in bytes
#define DECIMAL_LEN 5			// Number of chunks to use
#define DECIMAL_CHUNK_32 4		// Chunk size in bytes when using 32-bit chunks
#define DECIMAL_LEN_32 10		// Number of chunks to use  when using 32-bit chunks
#define DECIMAL_BLOB 41			// DECIMAL_SIGN + DECIMAL_CHUNK * DECIMAL_LEN
#define DECIMAL_PRINT_LEN 100	// The size of the array to print DECIMAL_LEN x DECIMAL_CHUNK bytes in human-readable form plus a sign, a dot and NULL
#define FRACTION_DIGITS 30
#define FRACTION_ZEROES "000000000000000000000000000000"	// Empty fraction - length should match FRACTION_DIGITS
#define DECIMAL_POINT "."


// STRUCTURES
struct decimal_s {
	char sign;							// 1 for positive (and zero), 0 for negative numbers
	uint64_t decimal[DECIMAL_LEN];		// Bytes for the decimal part; most senior part is in most senior index
};

struct dec32_s {
	int32_t sign;						// 1 for positive (and zero), 0 for negative numbers
	uint32_t decimal[DECIMAL_LEN_32];	// Pointer to memory area storing the bytes for the decimal part in MSB
};


// PROTOTYPES
void dec_add64(uint64_t *, uint64_t *, int, uint64_t *, int *);
void dec_sub64(uint64_t *, uint64_t *, int, uint64_t *, int *);

struct decimal_s *decimal_new(void);
void decimal_zero(struct decimal_s *);
void decimal_set_min(struct decimal_s *);
void decimal_set_max(struct decimal_s *);
int decimal_cmp(struct decimal_s *, struct decimal_s *);
int decimal_cmp_abs(struct decimal_s *, struct decimal_s *);
void decimal_cpy(struct decimal_s *, struct decimal_s *);
int decimal_sum(struct decimal_s *, struct decimal_s *, struct decimal_s *);
int decimal_avg(struct decimal_s *a, uint32_t b, struct decimal_s **res);
void decimal_to_str(struct decimal_s *, char *) ;
struct decimal_s *decimal_from_str(char *);
struct decimal_s *decimal_from_int64(int64_t);
struct decimal_s *decimal_from_uint64(uint64_t);
struct decimal_s *decimal_from_double(double);
void decimal_to_blob(struct decimal_s *d, char *);
struct decimal_s *decimal_from_blob(char *buffer);

struct dec32_s *dec32_new(void);
void dec32_zero(struct dec32_s *d);
struct dec32_s *decimal_to_dec32(struct decimal_s *decimal);
struct decimal_s *dec32_to_decimal(struct dec32_s *d32);
int dec32_avg(struct dec32_s *a, uint32_t b, struct dec32_s **);

