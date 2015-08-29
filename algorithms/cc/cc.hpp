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

#ifndef _CC_
#define _CC_
#include "../../core/x-lib.hpp"

// Note: CC is defined for undirected graphs

namespace algorithm {
  namespace sg_simple {
    class cc_per_processor_data:public per_processor_data {
    public:
      static unsigned long cc_neg;
      double cc_neg_delta;
      cc_per_processor_data() 
	:cc_neg_delta(0)
      {}
      bool reduce(per_processor_data **per_cpu_array,
		  unsigned long processors) 
      {
	for(unsigned long i=0;i<processors;i++) {
	  cc_per_processor_data * data = 
	    static_cast<cc_per_processor_data *>(per_cpu_array[i]);
	  cc_neg += data->cc_neg_delta;
	  data->cc_neg_delta = 0;
	}
	return false;
      }
    } __attribute__((__aligned__(64))) ;
  
    template <typename F>
    class cc {
    private:

      struct vertex {
	vertex_t component;
	unsigned long active_phase;
	bool merged;
      } __attribute__((__packed__));

      struct update {
	vertex_t target;
	vertex_t component;
      } __attribute__((__packed__));

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
	vertices->component = vertex_index;
	vertices->active_phase = 0;
	vertices->merged = false;
	return true;
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

	if (u->component < v->component) {
	  v->component = u->component;
	  v->active_phase = bsp_phase;
	  if(!v->merged) {
	    static_cast<cc_per_processor_data*>(per_cpu_data)->cc_neg_delta ++; // One component merged
	    v->merged = true;
	  }
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
	F::read_edge(edge_format, src, dst);

	struct vertex* vertices = (struct vertex*)vertex_state;
	struct vertex* v = &vertices[x_lib::configuration::map_offset(src)];
      
	if (v->active_phase == bsp_phase) {
	  struct update* u = (struct update*)update_stream;
	  u->target = dst;
	  u->component = v->component;
	  return true;
	} else {
	  return false;
	}
      }

      static void preprocessing()
      {
      }

      static void postprocessing() 
      {
	BOOST_LOG_TRIVIAL(info) << "ALGORITHM::CC::CC_COUNT " << 
	  (pt.get<unsigned long>("graph.vertices") -
	   cc_per_processor_data::cc_neg);
      }

      static per_processor_data * 
      create_per_processor_data(unsigned long processor_id)
      {
	return new cc_per_processor_data();
      }

      static unsigned long min_super_phases()
      {
	return 1;
      }

    };
    unsigned long cc_per_processor_data::cc_neg = 0;
  }
}
#endif
