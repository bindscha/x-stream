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

static void generate_edge(mrg_state* pstate, const int scale,
                          double a, double b, double c, double d,
                          struct edge_struct* edge)
{
  vertex_t i = 0, j = 0;
  vertex_t bit = (vertex_t)1 << (scale-1);

  while (true)
  {
    double r  = mrg_get_double_orig(pstate);
    if (r > a) {               /* outside quadrant 1 */
      if (r <= a + b)          /* in quadrant 2 */
        j |= bit;
      else if (r <= a + b + c) /* in quadrant 3 */
        i |= bit;
      else {                   /* in quadrant 4 */
        j |= bit;
        i |= bit;
      }
    }
    
    if (1 == bit) break;

    /*
     * Noise is introduced by modifying the probabilites by +/- 5%
     * and normalising them.
     */
#ifdef NOISE
    r  = mrg_get_double_orig(pstate);
    a *= 0.95 + r/10;
    r  = mrg_get_double_orig(pstate);
    b *= 0.95 + r/10;
    r  = mrg_get_double_orig(pstate);
    c *= 0.95 + r/10;
    r  = mrg_get_double_orig(pstate);
    d *= 0.95 + r/10;

    double norm = 1.0 / (a + b + c + d);
    a *= norm;
    b *= norm;
    c *= norm;
    d = 1.0 - (a + b + c);
#endif

    /* Iterates scale times. */
    bit >>= 1;
  }
  
  edge->src = i;
  edge->dst = j;
#ifdef WEIGHT
  edge->weight = (value_t)mrg_get_double_orig(pstate);
#endif
}

static void generate(thread_buffer* buffer, const mrg_state& state, const int scale, const edge_t start, const edge_t end,
                     const double a, const double b, const double c, /*const double d,*/ const bool symmetric)
{
  const double d = 1 - (a + b + c);
  for (edge_t ei = start; ei < end; ++ei)
  {
    mrg_state new_state = state;
    mrg_skip(&new_state, 0, ei, 0);
    struct edge_struct* edge = buffer->edge_struct();
    generate_edge(&new_state, scale, a, b, c, d, edge);
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
  if (process_options(argc, argv, true, &options) != 0)
    return 0;

  if (options.rmat.a + options.rmat.b + options.rmat.c >= 1) {
    printf("Error: The sum of probabilities must equal 1\n");
    return 0;
  }
  double d = 1 - (options.rmat.a + options.rmat.b + options.rmat.c);

  uint_fast32_t seed[5];
  make_mrg_seed(options.rng.userseed1, options.rng.userseed2, seed);
  mrg_state state;
  mrg_seed(&state, seed);
  //mrg_skip(&new_state, 50, 7, 0); // Do an initial skip?

  edge_t total_edges = options.rmat.edges;
  if (options.global.symmetric) {
    total_edges *= 2;
  }

  printf("Generator type: R-MAT\n");
  printf("Scale: %d (%" PRIu64 " vertices)\n", options.rmat.scale, ((uint64_t)1 << options.rmat.scale));
  printf("Edges: %" PRIet "\n", total_edges);
  printf("Probabilities: A=%4.2f, B=%4.2f, C=%4.2f, D=%4.2f\n", options.rmat.a, options.rmat.b, options.rmat.c, d);

  double start = get_time();

  // io thread
  size_t buffer_size = calculate_buffer_size(options.global.buffer_size);
  buffer_queue flushq;
  buffer_manager manager(&flushq, options.global.buffers_per_thread, buffer_size);
  io_thread_func io_func(options.global.graphname.c_str(), total_edges, &flushq, &manager, buffer_size);
  boost::thread io_thread(boost::ref(io_func));
  
  // worker threads
  int nthreads = options.global.nthreads;
  edge_t edges_per_thread = options.rmat.edges / nthreads;  
  threadid_t* workers[nthreads];
  boost::thread* worker_threads[nthreads];
  for (int i = 0; i < nthreads; i++) {
    workers[i] = new threadid_t(i);
    thread_buffer* buffer = manager.register_thread(*workers[i]);
    // last thread gets the remainder (if any)
    edge_t start = i * edges_per_thread;
    edge_t end = (i == nthreads-1) ? (options.rmat.edges) : ((i+1) * edges_per_thread);
    worker_threads[i] = new boost::thread(
      generate, buffer,
      state, options.rmat.scale, start, end, options.rmat.a, options.rmat.b, options.rmat.c, /*d,*/ options.global.symmetric
    );
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

  make_ini_file(options.global.graphname.c_str(), (uint64_t)1 << options.rmat.scale, total_edges);

  return 0;
}

