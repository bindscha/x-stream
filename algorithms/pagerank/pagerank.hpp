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

#ifndef _PAGERANK_
#define _PAGERANK_

#include "../../core/x-lib.hpp"

namespace algorithm {
  namespace sg_simple {
    template <typename F>
    class pagerank {
    private:

      struct vertex {
	vertex_t degree;
	weight_t rank;
	weight_t sum;
      } __attribute__((__packed__));

      struct update {
	vertex_t target;
	weight_t rank;
      } __attribute__((__packed__));

      static const weight_t DAMPING_FACTOR = 0.85;
      static unsigned long niters;

    public:
    
      static unsigned long vertex_state_bytes() {
	return sizeof(struct vertex);
      }
      static unsigned long split_size_bytes() {
	return sizeof(struct update);
      }

      static unsigned long split_key(unsigned char* buffer, unsigned long jump)
      {
	struct update* u = (struct update*)buffer;
	vertex_t key = u->target;
	key = key >> jump;
	return key;
      }

      static bool init(unsigned char* vertex_state,
		       unsigned long vertex_index,
		       unsigned long bsp_phase,
		       per_processor_data *cpu_state)
      {
	struct vertex* vertices = (struct vertex*)vertex_state;
	vertices->sum  = 0;
	if(bsp_phase == 0) {
	  vertices->degree = 0;
	  vertices->rank = 1.0;
	}
	else if(bsp_phase == 1) {
	  // Properly set up source vertices (no incoming edges)
	  vertices->rank = (1 - DAMPING_FACTOR);
	}
	return true;
      }

      static bool need_init(unsigned long bsp_phase)
      {
	return true;
      }

      static void preprocessing()
      {
	// The extra +1 is for the iteration 0 which is used just to compute the vertex degrees
	niters = 1 + vm["pagerank::niters"].as<unsigned long>();
      }

      static bool apply_one_update(unsigned char* vertex_state,
				   unsigned char* update_stream,
				   per_processor_data* per_cpu_data,
				   unsigned long bsp_phase)
      {
	struct update* u = (struct update*)update_stream;
	struct vertex* vertices = (struct vertex*)vertex_state;
	struct vertex* v = &vertices[x_lib::configuration::map_offset(u->target)];

	v->sum += u->rank;
	v->rank = 1 - DAMPING_FACTOR + DAMPING_FACTOR * v->sum;

	if (bsp_phase == niters)
	  return false;
	else
	  return true;
      }

      static bool generate_update(unsigned char* vertex_state,
				  unsigned char* edge_format,
				  unsigned char* update_stream,
				  per_processor_data* per_cpu_data,
				  unsigned long bsp_phase)
      {
	vertex_t src, dst;
	F::read_edge(edge_format, src, dst);

	struct vertex* vertices = (struct vertex*)vertex_state;
	struct vertex* v = &vertices[x_lib::configuration::map_offset(src)];

	if (bsp_phase == 0) {
	  v->degree++;
	  return false;
	}
	else {
	  struct update* u = (struct update*)update_stream;
	  u->target = dst;
	  if(bsp_phase == 1) {
	    u->rank = 1.0/v->degree;
	  }
	  else {
	    u->rank = v->rank / v->degree;
	  }
	  return true;
	}
      }

      static per_processor_data* create_per_processor_data(unsigned long processor_id)
      {
	return NULL;
      }

      static unsigned long min_super_phases()
      {
	return 2;
      }
    
      static void postprocessing() {}

    };

    template<typename F>
    unsigned long pagerank<F>::niters;
  }
}
#endif
