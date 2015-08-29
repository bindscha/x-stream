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

#ifndef _RANGE_CHECK_
#define _RANGE_CHECK_
#include "../../core/x-lib.hpp"
#include "../../utils/options_utils.h"
#include "../../utils/boost_log_wrapper.h"
#include<errno.h>
#include<string>
namespace algorithm {
  namespace sg_simple {
    // Assumes a partition between even numbered vertices (black) and 
    // odd numbered vertices (red)
    template<typename F>
    class range_check {
      static vertex_t max_vertex_id;
    public:
      static unsigned long split_size_bytes()
      {
	return 1;
      }
    
      static unsigned long split_key(unsigned char *buffer,
				     unsigned long jump)
      {
	return 0;
      }

      static unsigned long vertex_state_bytes()
      {
	return 0;
      }

      static bool apply_one_update(unsigned char *vertex_state,
				   unsigned char *update_stream,
				   per_processor_data *per_cpu_data,
				   unsigned long bsp_phase)
      {
	BOOST_ASSERT_MSG(false, "Should not be called !");
	return false;
      }

      static bool generate_update(unsigned char *vertex_state,
				  unsigned char *edge_format,
				  unsigned char *update_stream,
				  per_processor_data *per_cpu_data,
				  unsigned long bsp_phase)
      {
	vertex_t src, dst;
	F::read_edge(edge_format, src, dst);
	if((src > max_vertex_id) || (dst > max_vertex_id)) {
	  BOOST_LOG_TRIVIAL(fatal) << "Range check failed for edge:" 
				   << src << "->" << dst;
	  exit(-1);
	}
	return false;
      }

      static bool init(unsigned char * vertex_state,
		       unsigned long vertex_id,
		       unsigned long bsp_phase,
		       per_processor_data *cpu_state)
      {
	return true;
      }

      static bool need_init(unsigned long bsp_phase)
      {
	return (bsp_phase == 0);
      }

      static void preprocessing()
      {
	max_vertex_id = pt.get<vertex_t>("graph.vertices") - 1;
      }

      static void postprocessing() 
      {
      }

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
    vertex_t range_check<F>::max_vertex_id = 0;
  }
}
#endif
