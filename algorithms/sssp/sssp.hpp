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

#ifndef _SSSP_
#define _SSSP_
#include "../../utils/options_utils.h"
#include "../../core/x-lib.hpp"
#include <limits>

namespace algorithm {
  namespace sg_simple {
    template <typename F>
    class sssp {
      static unsigned long sssp_source;
    public:
    
      struct vertex {
	vertex_t predecessor;
	weight_t distance;
	unsigned long active_phase;
      } __attribute__((__packed__));

      struct update {
	vertex_t target;
	vertex_t predecessor;
	weight_t distance;
      } __attribute__((__packed__));

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

      static void preprocessing()
      {
	sssp_source = vm["sssp::source"].as<unsigned long>();
      }

      static bool init(unsigned char* vertex_state,
		       unsigned long vertex_index,
		       unsigned long bsp_phase,
		       per_processor_data *cpu_state)
      {
	bool should_start = false;
	struct vertex* sssp_vertex = (struct vertex*)vertex_state;
	sssp_vertex->predecessor = (vertex_t)-1;
	sssp_vertex->distance = (weight_t)std::numeric_limits<double>::max();
	sssp_vertex->active_phase = -1UL;
	if (vertex_index == sssp_source) {
	  should_start = true;
	  sssp_vertex->distance = 0;
	  sssp_vertex->active_phase = 0;
	}
	return should_start;
      }

      static bool need_init(unsigned long bsp_phase)
      {
	return (bsp_phase == 0);
      }

      static bool apply_one_update(unsigned char* vertex_state,
				   unsigned char* update_stream,
				   per_processor_data *per_cpu_data,
				   unsigned long bsp_phase)
      {
	struct update* u = (struct update*)update_stream;
	struct vertex* vertices = (struct vertex*)vertex_state;
	struct vertex* v = &vertices[x_lib::configuration::map_offset(u->target)];
	if (u->distance < v->distance) {
	  v->predecessor = u->predecessor;
	  v->distance = u->distance;
	  v->active_phase = bsp_phase;
	  return true;
	} else {
	  return false;
	}
      }

      static bool generate_update(unsigned char* vertex_state,
				  unsigned char* edge_format,
				  unsigned char* update_stream,
				  per_processor_data *per_cpu_data,
				  unsigned long bsp_phase)
      {
	vertex_t src, dst;
	weight_t edge_distance;
	F::read_edge(edge_format, src, dst, edge_distance);

	struct vertex* vertices = (struct vertex*)vertex_state;
	struct vertex* v = &vertices[x_lib::configuration::map_offset(src)];
	if (v->active_phase == bsp_phase) {
	  struct update* u = (struct update*)update_stream;
	  u->target = dst;
	  u->predecessor = src;
	  u->distance = v->distance + edge_distance;
	  return true;
	} else {
	  return false;
	}
      }

      // not used
      static void postprocessing() {}

      static per_processor_data * 
      create_per_processor_data(unsigned long processor_id)
      {
	return NULL;
      }

      static unsigned long min_super_phases()
      {
	return 1;
      }

    };
    template<typename F>
    unsigned long sssp<F>::sssp_source = ULONG_MAX;
  }
}
#endif
