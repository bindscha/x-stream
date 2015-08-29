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

#include <cstdio>
#include <boost/thread/thread.hpp>
#include "defs.h"
#include "options.h"
#include "util.h"
#include "output.h"
#include "prng/splittable_mrg.h"
#include "prng/utils.h"

inline static vertex_t generate_vertex(mrg_state* pstate, const vertex_t nvertices)
{
  /* Generate a pseudorandom number in the range [0, vertices) without modulo bias. */
  vertex_t limit = VT_MAX % nvertices;
  vertex_t v;  

  do {
    v = mrg_get_uint_orig(pstate);
#ifndef VERTEX_TYPE_32
    v = (v << 32) + mrg_get_uint_orig(pstate);
#endif
  } while (UNLIKELY(v < limit));
  return v % nvertices;
}

static void generate(thread_buffer* buffer, const mrg_state& state, const vertex_t nvertices, 
                     const edge_t start, const edge_t end, const bool allow_self_loops, const bool symmetric)
{
  for (edge_t ei = start; ei < end; ++ei)
  {
    mrg_state new_state = state;
    mrg_skip(&new_state, 0, ei, 0);
    struct edge_struct* edge = buffer->edge_struct();
    edge->src = generate_vertex(&new_state, nvertices);
    do {
      edge->dst = generate_vertex(&new_state, nvertices);
    } while (UNLIKELY(edge->src == edge->dst && !allow_self_loops));
#ifdef WEIGHT
    edge->weight = (value_t)mrg_get_double_orig(&new_state);
#endif
    if (symmetric) {
      struct edge_struct* reverse_edge = buffer->edge_struct();
      reverse_edge->src = edge->dst;
      reverse_edge->dst = edge->src;
#ifdef WEIGHT
      reverse_edge->weight = edge->weight;
#endif
    }
  }
  buffer->flush();
}

static void generate_bipartite(thread_buffer* buffer, const mrg_state& state, const vertex_t nleft, const vertex_t nvertices,
                               const edge_t start, const edge_t end, const bool symmetric)
{
  for (edge_t ei = start; ei < end; ++ei)
  {
    mrg_state new_state = state;
    mrg_skip(&new_state, 0, ei, 0);
    struct edge_struct* edge = buffer->edge_struct();
    edge->src = generate_vertex(&new_state, nvertices);
    if (edge->src < nleft) {
      edge->dst = nleft + generate_vertex(&new_state, nvertices - nleft);
    } else {
      edge->dst = generate_vertex(&new_state, nleft);
    }
#ifdef WEIGHT
    edge->weight = (value_t)mrg_get_double_orig(&new_state);
#endif
    if (symmetric) {
      struct edge_struct* reverse_edge = buffer->edge_struct();
      reverse_edge->src = edge->dst;
      reverse_edge->dst = edge->src;
#ifdef WEIGHT
      reverse_edge->weight = edge->weight;
#endif
    }
  }
  buffer->flush();
}

int main(int argc, char** argv)
{
  struct options options;
  if (process_options(argc, argv, false, &options) != 0)
    return 0;

  uint_fast32_t seed[5];
  make_mrg_seed(options.rng.userseed1, options.rng.userseed2, seed);
  mrg_state state;
  mrg_seed(&state, seed);
  //mrg_skip(&new_state, 50, 7, 0); // Do an initial skip?

  edge_t total_edges = options.erdos_renyi.edges;
  if (options.global.symmetric) {
    total_edges *= 2;
  }

  printf("Generator type: Erdos-Renyi\n");
  printf("Vertices: %" PRIvt "\n", options.erdos_renyi.vertices);
  printf("Edges: %" PRIet "\n", total_edges);
  printf("Self-loops:%s allowed\n", (options.erdos_renyi.self_loops ? "" : " not"));

  double start = get_time();

  // io thread
  size_t buffer_size = calculate_buffer_size(options.global.buffer_size);
  buffer_queue flushq;
  buffer_manager manager(&flushq, options.global.buffers_per_thread, buffer_size);
  io_thread_func io_func(options.global.graphname.c_str(), total_edges, &flushq, &manager, buffer_size);
  boost::thread io_thread(boost::ref(io_func));

  // worker threads
  int nthreads = options.global.nthreads;
  edge_t edges_per_thread = options.erdos_renyi.edges / nthreads;
  threadid_t* workers[nthreads];
  boost::thread* worker_threads[nthreads];
  for (int i = 0; i < nthreads; i++) {
    workers[i] = new threadid_t(i);
    thread_buffer* buffer = manager.register_thread(*workers[i]);
    // last thread gets the remainder (if any)
    edge_t start = i * edges_per_thread;
    edge_t end = (i == nthreads-1) ? (options.erdos_renyi.edges) : ((i+1) * edges_per_thread);
    if (options.erdos_renyi.bipartite) {
      worker_threads[i] = new boost::thread(
        generate_bipartite, buffer,
        state, options.erdos_renyi.bipartite, options.erdos_renyi.vertices, start, end, options.global.symmetric
      );
    } else {
      worker_threads[i] = new boost::thread(
        generate, buffer,
        state, options.erdos_renyi.vertices, start, end, options.erdos_renyi.self_loops, options.global.symmetric
      );
    }
  }

  // Wait until work completes
  for (int i = 0; i < nthreads; i++) {
    worker_threads[i]->join();
  }
  io_func.stop();
  io_thread.join();

  // cleanup
  for (int i = 0; i < nthreads; i++) {
    manager.unregister_thread(*workers[i]);
    delete worker_threads[i];
    delete workers[i];
  }

  double elapsed = get_time() - start;  
  printf("Generation time: %fs\n", elapsed);

  make_ini_file(options.global.graphname.c_str(), options.erdos_renyi.vertices, total_edges);

  return 0;
}

