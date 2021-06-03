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
#include "timestamp.h"

// Convert string to timestamp
// Possible input:
// HH:MI:SS (8 chars)
// YYYY-MM-DD (10 chars)
// YYYY-MM-DD HH:MI:SS (19 chars)
// YYYY-MM-DD HH:MI:SS.mmm (23 chars)
uint64_t timestamp_from_str(char *s) {
	char str[18];
	bzero(&str[0], 18);

	// Process as per input type
	switch(strlen(s)) {
		case 8:
			// HH:MI:SS (8 chars)
			// Start with up zeroes
			strcpy(&str[0], "00000000");

			// Copy hours, minutes, seconds
			memcpy(&str[8], s, 2);
			memcpy(&str[10], s+3, 2);
			memcpy(&str[12], s+6, 2);

			// Fill up with zeroes
			strcpy(&str[14], "000");
			break;

		case 10:
			// YYYY-MM-DD (10 chars)
			// Copy year, month, day
			memcpy(&str[0], s, 4);
			memcpy(&str[4], s+5, 2);
			memcpy(&str[6], s+8, 2);

			// Fill up with zeroes
			strcpy(&str[8], "000000000");
			break;

		case 19:
			// Copy year, month, day, hour, minute, second
			memcpy(&str[0], s, 4);
			memcpy(&str[4], s+5, 2);
			memcpy(&str[6], s+8, 2);
			memcpy(&str[8], s+11, 2);
			memcpy(&str[10], s+14, 2);
			memcpy(&str[12], s+17, 2);

			// Fill up with zeroes
			strcpy(&str[14], "000");
			break;

		case 23:
			// Copy year, month, day, hour, minute, second, millisecond
			memcpy(&str[0], s, 4);
			memcpy(&str[4], s+5, 2);
			memcpy(&str[6], s+8, 2);
			memcpy(&str[8], s+11, 2);
			memcpy(&str[10], s+14, 2);
			memcpy(&str[12], s+17, 2);
			memcpy(&str[14], s+19, 3);
			break;

		default:
			system_log(LOG_ERR, "timestamp_from_str: Unknown parameter length %i", strlen(s));
	}

	return atol(&str[0]);
}

// Convert timestamp to string
char *timestamp_to_str(uint64_t t, int mode) {
	char s1[18];
	char *s2 = mem_alloc(TIMESTAMP_PRINT_LEN);

	// Fill in the result string from right to left;
	// Then move left, copying one group less each time. 
	// Finally, add the separators
	switch (mode) {
		case TIMESTAMP_CONVERT_TIME:
			// Convert to string
			sprintf(s1, "%06lu", t);

			memcpy(s2 + 2, &s1[0], 6);
			memcpy(s2 + 1, &s1[0], 4);
			memcpy(s2, &s1[0], 2);

			memcpy(s2 + 2, ":", 1);
			memcpy(s2 + 5, ":", 1);

			memset(s2 + 8, '\0', 1);
			break;

		case TIMESTAMP_CONVERT_DATE:
			// Convert to string
			sprintf(s1, "%08lu", t);

			memcpy(s2 + 2, &s1[0], 8);
			memcpy(s2 + 1, &s1[0], 6);
			memcpy(s2, &s1[0], 4);

			memcpy(s2 + 4, "-", 1);
			memcpy(s2 + 7, "-", 1);

			memset(s2 + 10, '\0', 1);
			break;

		case TIMESTAMP_CONVERT_DATETIME_S:
			// Convert to string
			sprintf(s1, "%017lu", t);

			memcpy(s2 + 6, &s1[0], 17);
			memcpy(s2 + 5, &s1[0], 14);
			memcpy(s2 + 4, &s1[0], 12);
			memcpy(s2 + 3, &s1[0], 10);
			memcpy(s2 + 2, &s1[0], 8);
			memcpy(s2 + 1, &s1[0], 6);
			memcpy(s2, &s1[0], 4);

			memcpy(s2 + 4, "-", 1);
			memcpy(s2 + 7, "-", 1);
			memcpy(s2 + 10, " ", 1);
			memcpy(s2 + 13, ":", 1);
			memcpy(s2 + 16, ":", 1);

			memset(s2 + 19, '\0', 1);
			break;

		case TIMESTAMP_CONVERT_DATETIME_MS:
			// Convert to string
			sprintf(s1, "%017lu", t);

			memcpy(s2 + 6, &s1[0], 17);
			memcpy(s2 + 5, &s1[0], 14);
			memcpy(s2 + 4, &s1[0], 12);
			memcpy(s2 + 3, &s1[0], 10);
			memcpy(s2 + 2, &s1[0], 8);
			memcpy(s2 + 1, &s1[0], 6);
			memcpy(s2, &s1[0], 4);

			memcpy(s2 + 4, "-", 1);
			memcpy(s2 + 7, "-", 1);
			memcpy(s2 + 10, " ", 1);
			memcpy(s2 + 13, ":", 1);
			memcpy(s2 + 16, ":", 1);

			memcpy(s2 + 19, ".", 1);
			break;
	}

	return s2;
}

// Compare two timestamps
int timestamp_cmp(uint64_t a, uint64_t b) {
	if (a > b)
		return 1;

	if (a < b)
		return -1;

	return 0;
};

