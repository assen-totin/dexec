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

// Create new decimal
struct decimal_s *decimal_new() {
	struct decimal_s *d = mem_alloc(sizeof(struct decimal_s));

	decimal_zero(d);

	return d;
}

// Zero a decimal
void decimal_zero(struct decimal_s *d) {
	int i;

	d->sign = 1;

	for (i=0; i<DECIMAL_LEN; i++)
		d->decimal[i] = 0L;
}

// Set the minimal possible value for a decimal 
void decimal_set_min(struct decimal_s *decimal) {
	int i;

	for (i=0; i<DECIMAL_LEN; i++)
		decimal->decimal[i] = ULONG_MAX;

	decimal->sign = 0;
}

// Set the maximum possible value for a decimal 
void decimal_set_max(struct decimal_s *decimal) {
	int i;

	for (i=0; i<DECIMAL_LEN; i++)
		decimal->decimal[i] = ULONG_MAX;

	decimal->sign = 1;
}

// Compare two decimals with sign
int decimal_cmp(struct decimal_s *a, struct decimal_s *b) {
	int i;

	// Positive sign (1) is always bigger than negative sign (0)
	if (a->sign > b->sign)
		return 1;
	if (a->sign < b->sign)
		return -1;

	// For the same sign, compare until difference is found
	for (i=DECIMAL_LEN-1; i>=0; i--) {
		if (a->decimal[i] > b->decimal[i]) {
			if (a->sign == 1)
				return 1;
			else
				return -1;
		}
		else if (a->decimal[i] < b->decimal[i]) {
			if (a->sign == 1)
				return -1;
			else
				return 1;
		}
	}

	return 0;
}

// Compare two decimals (absolute values)
int decimal_cmp_abs(struct decimal_s *a, struct decimal_s *b) {
	int i;

	// Compare until difference is found
	for (i=DECIMAL_LEN-1; i>=0; i--) {
		if (a->decimal[i] > b->decimal[i])
			return 1;
		else if (a->decimal[i] < b->decimal[i])
			return -1;
	}

	return 0;
}

// Copy one decimal to another
void decimal_cpy(struct decimal_s *to, struct decimal_s *from) {
	int i;

	for (i=0; i<DECIMAL_LEN; i++)
		to->decimal[i] = from->decimal[i];

	to->sign = from->sign;
}


// Add two numbers with the same sign
void dec_add64(uint64_t *a, uint64_t *b, int carry, uint64_t *res, int *overflow) {
	// Same-sign decimals: addition
	// overflow will be set to 1 if we need to carry
	// carry will be set to 1 if have carried
	if ((*a > ULONG_MAX - *b - carry) || (*b > ULONG_MAX - *a - carry)) {
		*overflow = 1;
		*res = *a - ULONG_MAX - 1 + *b + carry;
	}
	else {
		*overflow = 0;
		*res = *a + *b + carry;
	}
}

// Add two numbers with different signs; A should be the positive one and B the negative
void dec_sub64(uint64_t *a, uint64_t *b, int borrow, uint64_t *res, int *overflow) {
	// Different-sign decimals: subtraction
	// overflow will be set to -1 if we need to borrow
	// borrow will be set to -1 if have borrowed
	if ((*a + borrow) >= *b) {
		*res = *a + borrow - *b;
		*overflow = 0;
	}
	else {
		*res = *a + borrow - *b + ULONG_MAX + 1;
		*overflow = -1;
	}
}

int decimal_sum(struct decimal_s *a, struct decimal_s *b, struct decimal_s *res) {
	int i, carry=0, overflow=0, cmp;
	struct decimal_s *tmp1, *tmp2;

	if (a->sign == b->sign) {
		// The decimals
		res->sign = a->sign;
		for (i=0; i<DECIMAL_LEN; i++) {
			carry = overflow;

			// Check if we are done early and return if so
			if (! a->decimal[i] && ! b->decimal[i] && ! carry)
				return DEXEC_OK;

			dec_add64(&a->decimal[i], &b->decimal[i], carry, &res->decimal[i], &overflow);
		}

		// If we had an overflow in the last round of decimals, extend res
		if (overflow)
				return DEXEC_ERROR_OVERFLOW;
	}
	else {
		cmp = decimal_cmp_abs(a, b);
		if (! cmp) {
			for (i=0; i<DECIMAL_LEN; i++)
				res->decimal[i] = 0L;
			res->sign = 1;
			return DEXEC_OK;
		}
		else if (cmp == 1) {
			tmp1 = a;
			tmp2 = b;
			res->sign = a->sign;
		}
		else {
			tmp1 = b;
			tmp2 = a;
			res->sign = b->sign;
		}

		for (i=0; i<DECIMAL_LEN; i++) {
			carry = overflow;

			// Check if we are done early and return if so
			if (! a->decimal[i] && ! b->decimal[i] && ! carry)
				return DEXEC_OK;

			dec_sub64(&tmp1->decimal[i], &tmp2->decimal[i], carry, &res->decimal[i], &overflow);
		}
	}

	return DEXEC_OK;
}

// Division by 32-bit integer
// NB: Divisor b is UINT! Adjust together with dec32_avg() if you need signed divisor.
int decimal_avg(struct decimal_s *a, uint32_t b, struct decimal_s **res) {
	struct dec32_s *d32_res;
	int error;

	if (! b)
		return DEXEC_ERROR_DIVISION;

	// Convert decimal to one which has internal array of 32-bit integers
	struct dec32_s *d32 = decimal_to_dec32(a);

	if (error = dec32_avg(d32, b, &d32_res)) {
		free(d32);
		return error;
	}

	*res = dec32_to_decimal(d32_res);

	free(d32);
	free(d32_res);

	return DEXEC_OK;
}

// Convert decimal to BLOB (e.g., to send over the network)
// The BLOB can also be used for direct comparison of decimals with memcmp()
void decimal_to_blob(struct decimal_s *d, char *buffer) {
	uint64_t chunk;
	int i, offset = 0;

	// Copy sign 
	memcpy(buffer, &d->sign, DECIMAL_SIGN);
	offset += DECIMAL_SIGN;

	// Copy value
	for (i=DECIMAL_LEN - 1; i>=0; i--) {
		chunk = htobe64(d->decimal[i]);
		memcpy(buffer + offset, &chunk, DECIMAL_CHUNK);
		offset += DECIMAL_CHUNK;
	}
}

// Convert BLOB to decimal (e.g., when reading from network)
struct decimal_s *decimal_from_blob(char *buffer) {
	int i, offset = 0;
	struct decimal_s *d;

	d = mem_alloc(sizeof(struct decimal_s));

	d->sign = *(char *)(buffer);

	for (i=DECIMAL_LEN - 1; i>=0; i--)
		d->decimal[i] = be64toh(*(uint64_t *)(buffer + DECIMAL_SIGN + (DECIMAL_LEN - 1 - i) * DECIMAL_CHUNK));

	return d;
}

// Convert a decimal into NULL-terminated string
void decimal_to_str(struct decimal_s *a, char *s) {
	unsigned char letter[2], h[2 * DECIMAL_LEN * DECIMAL_CHUNK + 1];
	int i, j=0, l, carry, offset=0;
	unsigned dec[DECIMAL_PRINT_LEN], tmp;

	// Set sign if number is negative
	if (! a->sign) {
		sprintf(s, "-");
		offset ++;
	}

	// Zero variables
	letter[1] = 0;
	bzero(&dec[0], sizeof(dec));

	for (i=DECIMAL_LEN-1; i>=0; i--) {
		// Skip until first chunk with data is encountered
		if (! a->decimal[i] && ! j)
			continue;

		j++;

		// For the first chunk with data, skip left zeroes
		if (j == 1)
			sprintf(&h[0], "%lx", a->decimal[i]);
		else
			sprintf(&h[strlen(&h[0])], "%016lx", a->decimal[i]);
	}

	// Build a LE array of decimal numbers
	i = 0;
	while (strlen(&h[i])) {
		memcpy(&letter[0], &h[i], sizeof(char));
		l = strtol(&letter[0], NULL, 16);

		//sum = sum * 16 + l;
		//FIXME: make a smart counter of d[] size!
		carry = l;
		for (j=0; j<DECIMAL_PRINT_LEN; j++) {
			tmp = 16 * dec[j] + carry;
			dec[j] = tmp % 10;
			carry = (tmp - dec[j]) / 10;
		}

		i++;
	}

	// Copy the whole
	j = 0;
	for (i=DECIMAL_PRINT_LEN - 1; i>=FRACTION_DIGITS; i--) {
		// Skip leading zeroes
		if (! dec[i] && ! j)
			continue;

		snprintf(s + offset, 2, "%u", dec[i]);

		offset++;
		j++;
	}

	// If the whole is zero, still leave one zero
	if (! j) {
		snprintf(s + offset, 2, "%u", 0);
		offset++;
	}

	// Decimal dot
	sprintf(s + offset, DECIMAL_POINT);
	offset ++;
	
	// Copy the fraction
	for (i=FRACTION_DIGITS - 1; i>0; i--) {
		snprintf(s + offset, 2, "%u", dec[i]);
		offset++;
	}

	// Remove trailing zeroes
	for (i=0; i<DECIMAL_PRINT_LEN; i++) {
		if (! memcmp(s + offset - i - 1, "0", 1))
			memset(s + offset - i - 1, '\0', 1);
		else if (! memcmp(s + offset - i - 1, DECIMAL_POINT, 1)) {
			sprintf(s + offset - i, "0");
			break;
		}
		else
			break;
	}
}

// Create decimal from string
struct decimal_s *decimal_from_str(char *input) {
	int i, j, l, carry, offset=0, bin[8 * DECIMAL_LEN * DECIMAL_CHUNK];
	unsigned tmp;
	uint64_t base;
	char *token, *saveptr, s1[DECIMAL_PRINT_LEN];
	unsigned char letter[2];
	struct decimal_s *ret = decimal_new();

	// Copy input
	char *s = mem_alloc(strlen(input) + 1);
	strcpy(s, input);

	// Check if we have a sign
	if (! memcmp(s, "-", 1)) {
		ret->sign = 0;
		offset++;
	}
	else if (! memcmp(s, "+", 1))
		offset++;

	// Split by the decimal point, get the whole
	token = strtok_r(s + offset, DECIMAL_POINT, &saveptr);

	// Copy the whole to a temporary char array
	strcpy(&s1[0], token);

	// Get the fraction
	token = strtok_r(NULL, DECIMAL_POINT, &saveptr);

	// Right-fill the fraction with zeroes (required for proper arithmethics)
	if (token) {
		sprintf(&s1[strlen(&s1[0])], token);
		snprintf(&s1[strlen(&s1[0])], FRACTION_DIGITS - strlen(token) + 1, FRACTION_ZEROES);
	}
	else
		sprintf(&s1[strlen(&s1[0])], FRACTION_ZEROES);

	// Zero variables
	letter[1] = 0;
	bzero(&bin[0], sizeof(bin));

	// Build a LE array of binary numbers
	i = 0;
	while (strlen(&s1[0] + i)) {
		memcpy(&letter[0], &s1[0] + i, sizeof(char));
		l = strtol(&letter[0], NULL, 10);

		//sum = sum * 16 + l;
		//FIXME: make a smart counter of bin[] size!
		carry = l;
		for (j=0; j<8 * DECIMAL_LEN * DECIMAL_CHUNK; j++) {
			tmp = 10 * bin[j] + carry;
			bin[j] = tmp % 2;
			carry = (tmp - bin[j]) / 2;
		}

		i++;
	}

	// Convert to uint64_t
	for (i=0; i<DECIMAL_LEN; i++) {
		base = 1;
		for (j=0; j<8 * DECIMAL_CHUNK; j++) {
			if (bin[i * 8 * DECIMAL_CHUNK + j])
				ret->decimal[i] += base;

			base = base << 1;
		}
	}

	free(s);

	return ret;
}


// Create decimal from int64 (by converting it to string)
struct decimal_s *decimal_from_int64(int64_t i64) {
	char str[24];	// 20 digits, sign, dot, zero, NULL

	bzero(&str[0], 24);
	sprintf(&str[0], "%li", i64);

	return decimal_from_str(&str[0]);
}

// Create decimal from uint64 (by converting it to string)
struct decimal_s *decimal_from_uint64(uint64_t ui64) {
	char str[24];	// 20 digits, sign, dot, zero, NULL

	bzero(&str[0], 24);
	sprintf(&str[0], "%lu", ui64);

	return decimal_from_str(&str[0]);
}

// Create decimal from double (by converting it to string)
struct decimal_s *decimal_from_double(double d) {
	char str[DECIMAL_PRINT_LEN];

	bzero(&str[0], DECIMAL_PRINT_LEN);
	sprintf(&str[0], "%f", d);

	return decimal_from_str(&str[0]);
}

///// DECIMALS WITH ARRAY OF 32-BIT INTEGERS

// Create new dec32
struct dec32_s *dec32_new() {
	struct dec32_s *d32 = malloc(sizeof(struct dec32_s));

	dec32_zero(d32);

	d32->sign = 1;

	return d32;
}

// Zero a dec32
void dec32_zero(struct dec32_s *d32) {
	int i;

	d32->sign = 0;

	for (i=0; i<DECIMAL_LEN_32; i++)
		d32->decimal[i] = 0;
}

// Convert 64-base decimal to 32-base decimal
struct dec32_s *decimal_to_dec32(struct decimal_s *d) {
	int i;
	uint64_t ui64;

	struct dec32_s *d32 = dec32_new();

	// Sign
	d32->sign = d->sign;

	for(i=0; i<DECIMAL_LEN; i++) {
		// Lower 32 bits
		d32->decimal[2 * i] = d->decimal[i];

		// Higher 32 bits
		ui64 = d->decimal[i];
		d32->decimal[2 * i + 1] = ui64 >> 32;
	}

	return d32;
}

// Convert 32-base decimal to 64-base decimal
struct decimal_s *dec32_to_decimal(struct dec32_s *d32) {
	int i;
	struct decimal_s *d = decimal_new();

	// Sign
	d->sign = d32->sign;

	for(i=0; i<DECIMAL_LEN; i++) {
		// Higher 32 bits
		d->decimal[i] = d32->decimal[2 * i + 1];
		d->decimal[i] = d->decimal[i] << 32;
		d->decimal[i] += d32->decimal[2 * i];
	}

	return d;
}

// Divide decimal by an integer using long division
// NB: Divisor b is set to UINT! Adjust if you need signed division
int dec32_avg(struct dec32_s *a, uint32_t b, struct dec32_s **ret) {
	int i, started = 0;
	uint32_t carry = 0, res;
	uint64_t d;

	// Division by zero
	if (! b) {
		*ret = NULL;
		return DEXEC_ERROR_DIVISION;
	}

	*ret = dec32_new();

	// Set sign and se a positive b
	// NB: Below commented out code is for signed divisor!
/*
	if (b < 0) {
		if (a->sign)
			(*ret)->sign = 0;
		b = -1 * b;
	}
	else {
		if (! a->sign)
			(*ret)->sign = 0;
	}
*/
	(*ret)->sign = a->sign;

	for (i=(DECIMAL_LEN_32 - 1); i>=0; i--) {
		// Skip leading zeroes
		if (! a->decimal[i] && ! started)
			continue;
		started = 1;

		// If we have carry from previous position, use it
		if (carry) {
			d = (uint64_t)carry;
			d = d << 32;
		}
		else
			d = 0;

		d += a->decimal[i]; 

		if (d > b) {
			// If position value is bigger than divident, divide immediately
			res = d / b;
			carry = d - (res * b);
			(*ret)->decimal[i] = res;
		}
		else
			// Else create carry (ret is zeroed anyway)
			carry = d;
	}

	return DEXEC_OK;
}

