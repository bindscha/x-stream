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

#ifndef _CHECKBIP_
#define _CHECKBIP_

#include "../../utils/boost_log_wrapper.h"
#include "../../core/x-lib.hpp"

// Tests if graph is bipartite

namespace algorithm {
  namespace sg_simple {
    template <typename F>
    class check_if_bipartite {
    private:

      struct vertex {
	int side;	    // -1 is left, 1 is right, 0 is not set
      } __attribute__((__packed__));

      struct update {
	vertex_t target;
	int side;
      } __attribute__((__packed__));

      static bool is_bip;

    public:
    
      static int open_vertex_format() {return -1;}
      static unsigned long vertex_format_bytes() {return 0;}

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
	vertices->side = 0;
	return true;
      }

      static bool need_init(unsigned long bsp_phase)
      {
	return (bsp_phase == 0);
      }

      static bool apply_one_update(unsigned char* vertex_state,
				   unsigned char* update_stream,
				   per_processor_data* per_cpu_data,
				   unsigned long bsp_phase)
      {
	struct update* u = (struct update*)update_stream;
	struct vertex* vertices = (struct vertex*)vertex_state;
	struct vertex* v = &vertices[x_lib::configuration::map_offset(u->target)];

	if (v->side == 0) {
	  v->side = -u->side;
	}

	if (v->side != -u->side) {
	  is_bip = false;
	}

	return false;
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

	if (src < dst) {
	  v->side = -1;
	} else {
	  v->side = 1;
	}

	struct update* u = (struct update*)update_stream;
	u->target = dst;
	u->side = v->side;

	return true;
      }


      static void preprocessing() {
      }

      static void postprocessing() {
	if (!is_bip) {
	  BOOST_LOG_TRIVIAL(info) << "ALGORITHM::CHECKBIP::NOT_BIPARTITE";
	}
      }

      static per_processor_data* create_per_processor_data(unsigned long processor_id) {return NULL;}

      static unsigned long min_super_phases()
      {
	return 1;
      }

    };

    template<typename F>
    bool check_if_bipartite<F>::is_bip = true;
  }
}
#endif
