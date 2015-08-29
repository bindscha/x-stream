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

#ifndef _MIS_
#define _MIS_
#include "../../core/x-lib.hpp"

namespace algorithm {
  namespace sg_simple {
    class mis_per_processor_data:public per_processor_data {
    public:
      static unsigned long not_in_mis;
      unsigned long voted_out;
      mis_per_processor_data() 
	:voted_out(0)
      {}
      bool reduce(per_processor_data **per_cpu_array,
		  unsigned long processors)
      {
	unsigned long total_not_in_mis = 0;
	for(unsigned long i=0;i<processors;i++) {
	  mis_per_processor_data * data = 
	    static_cast<mis_per_processor_data *>(per_cpu_array[i]);
	  total_not_in_mis += data->voted_out;
	  data->voted_out = 0;
	}
	bool stop = (total_not_in_mis == not_in_mis);
	not_in_mis = total_not_in_mis;
	return stop;
      }
    }  __attribute__((__aligned__(64)))  ;

  
    template <typename F>
    class mis {
    private:
      struct vertex {
	bool in_mis;
      } __attribute__((__packed__));
    
      struct update {
	vertex_t target;
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
	vertices->in_mis = true;
	return true;
      }

      static bool need_init(unsigned long bsp_phase)
      {
	return true;
      }

      static bool apply_one_update(unsigned char* vertex_state,
				   unsigned char* update_stream,
				   per_processor_data* per_cpu_data,
				   unsigned long bsp_phase)
      {
	struct update * updt = (struct update *)update_stream;
	struct vertex* vertices = (struct vertex*)vertex_state;
	struct vertex* v = &vertices[x_lib::configuration::map_offset(updt->target)];
	if(v->in_mis) {
	  static_cast
	    <mis_per_processor_data *>(per_cpu_data)->voted_out++;
	  v->in_mis = false;
	}
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

	if ((v->in_mis) && (src < dst)) {
	  struct update * updt = (struct update *)update_stream;
	  updt->target = dst;
	  return true;
	}
	else {
	  return false;
	}
      }

      static per_processor_data * 
      create_per_processor_data(unsigned long processor_id)
      {
	return new mis_per_processor_data();
      }

      static unsigned long min_super_phases()
      {
	return 1;
      }

      static void preprocessing()
      {
      }

      static void postprocessing() 
      {
	BOOST_LOG_TRIVIAL(info) << "ALGORITHM::MIS::NOT_IN_MIS " 
				<< mis_per_processor_data::not_in_mis;
      }

    };
    unsigned long mis_per_processor_data::not_in_mis = 0;
  }
}
#endif
