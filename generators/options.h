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

#ifndef HPGP_GENERATORS_OPTIONS_H
#define HPGP_GENERATORS_OPTIONS_H

#include <string>
#include "defs.h"

struct global {
  std::string graphname;
  int nthreads;
  size_t buffer_size;
  int buffers_per_thread;
  bool symmetric;
};

const int DEFAULT_NTHREADS = 32;
const int DEFAULT_BUFFERS_PER_THREAD = 2;

struct erdos_renyi {
  vertex_t vertices;
  edge_t   edges;
  bool     self_loops;
  vertex_t bipartite;
};

const vertex_t DEFAULT_ER_VERTICES   = (vertex_t)1 << 16;
const vertex_t DEFAULT_ER_EDGES      = (edge_t)1 << 20;

struct rmat {
  int    scale;
  edge_t edges;
  double a;
  double b;
  double c;
};

const int    DEFAULT_RMAT_SCALE = 16;
const edge_t DEFAULT_RMAT_EDGES = (edge_t)1 << 20;
const double DEFAULT_RMAT_A     = 0.57;
const double DEFAULT_RMAT_B     = 0.19;
const double DEFAULT_RMAT_C     = 0.19;

struct rng {
  uint64_t userseed1;
  uint64_t userseed2;
};

const uint64_t DEFAULT_RNG_USERSEED1 = (uint64_t)1;
const uint64_t DEFAULT_RNG_USERSEED2 = (uint64_t)2;

struct options {
  struct global global;
  struct erdos_renyi erdos_renyi;
  struct rmat rmat;
  struct rng rng;
};

int process_options(int argc, char** argv, bool rmat_generator, struct options* options);

#endif /* HPGP_GENERATORS_OPTIONS_H */
