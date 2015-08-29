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

#ifndef _SPMV_
#define _SPMV_
#include "../../core/x-lib.hpp"
#include "../../utils/options_utils.h"
#include "../../utils/boost_log_wrapper.h"
#include<errno.h>
#include<string>
// Compute Y=XA
namespace algorithm {
  namespace sg_simple {
    template<typename F>
    class spmv {
    public:
      struct __attribute__((__packed__)) term {
	vertex_t column;
	weight_t value;
      };
    
      struct __attribute__((__packed__)) vector_element {
	weight_t value_out;
	weight_t value_in;
      };

      static unsigned long split_size_bytes()
      {
	return sizeof(struct term);
      }
    
      static unsigned long split_key(unsigned char *buffer,
				     unsigned long jump)
      {
	struct term *t = (struct term *)buffer;
	vertex_t key = t->column;
	key = key >> jump;
	return key;
      }

      static unsigned long vertex_state_bytes()
      {
	return sizeof(struct vector_element);
      }

      static bool apply_one_update(unsigned char *vertex_state,
				   unsigned char *update_stream,
				   per_processor_data *per_cpu_data,
				   unsigned long bsp_phase)
      {
      
	struct term * t = (struct term *)update_stream;
	unsigned long vindex = x_lib::configuration::map_offset(t->column);
	struct vector_element *vect= (struct vector_element *)vertex_state;
	vect[vindex].value_out += t->value; 
	return false;
      }
    
      static bool generate_update(unsigned char *vertex_state,
				  unsigned char *edge_format,
				  unsigned char *update_stream,
				  per_processor_data *per_cpu_data,
				  unsigned long bsp_phase)
      {
	vertex_t row, column;
	weight_t matrix_element;
	F::read_matrix_element(edge_format, row, column, matrix_element);
	unsigned long vindex = x_lib::configuration::map_offset(row);
	struct vector_element *vector_input = 
	  (struct vector_element *)vertex_state;
	struct term *t = (struct term *)update_stream;
	t->column = column;
	t->value = matrix_element*vector_input[vindex].value_in;
	return true;
      }
    
      static bool init(unsigned char* vertex_state,
		       unsigned long vertex_index,
		       unsigned long bsp_phase,
		       per_processor_data *cpu_state)
      {
	struct vector_element *vec = (struct vector_element *)vertex_state;
	vec->value_out = 0;
	vec->value_in = vertex_index;
	return true;
      }

      static bool need_init(unsigned long bsp_phase)
      {
	return (bsp_phase == 0);
      }

      static void postprocessing() {}
      static void preprocessing() {}
    
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
  }
}    

#endif
