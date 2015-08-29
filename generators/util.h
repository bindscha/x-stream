/*
 * X-Stream
 *
 * Copyright 2013 Operating Systems Laboratory EPFL
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef HPGP_GENERATORS_UTIL_H
#define HPGP_GENERATORS_UTIL_H

#include <sys/time.h>
#include <cstdio>
#include "defs.h"
#include "output.h"

inline double get_time()
{
  struct timeval tv;
  gettimeofday(&tv, NULL);
  return tv.tv_sec + tv.tv_usec * 1.e-6;
}

inline size_t calculate_buffer_size(const size_t cmdline_value)
{
  size_t buffer_size = cmdline_value;
  if (!buffer_size) {
    buffer_size = 256 * DEFAULT_BLOCK_SIZE * sizeof(struct edge_struct);
  }
  if (buffer_size % sizeof(struct edge_struct) != 0) {
    fprintf(stderr, "Error: Buffer size must be a multiple of edge_struct size\n");
    exit(1);
  }
  if (buffer_size % DEFAULT_BLOCK_SIZE != 0) {
    fprintf(stderr, "Error: Buffer size must be a multiple of 4096 (disk sector size)\n");
    exit(1);
  }
  return buffer_size;
}

#endif /* HPGP_GENERATORS_UTIL_H */
