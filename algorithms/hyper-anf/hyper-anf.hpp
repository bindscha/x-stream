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

#ifndef _HYPERANF_
#define _HYPERANF_
#include "../../utils/per_cpu_data.hpp"
#include "../../core/autotuner.hpp"
#include "../../utils/options_utils.h"
#include "../../utils/boost_log_wrapper.h"
#include "../../utils/hyperloglog.h"
#include<errno.h>
#include<string>
namespace algorithm {
  namespace sg_simple {
    class hyperanf_pp_data:public per_processor_data {
      static unsigned long iteration;
      static float global_neighbour_cnt;
      // stopping conditions (could be made static)
      float epsilon;
      unsigned long maxiters;
      bool run_to_completion;
    public:
      float local_neighbour_cnt;
      float local_seed_reach;
      vertex_t local_seed;
      hyperanf_pp_data()
	:local_neighbour_cnt(0),
	 local_seed_reach(0.0f),
	 local_seed(0)
      {
	epsilon  = vm["hyperanf::epsilon"].as<float>();
	maxiters = vm["hyperanf::maxiters"].as<unsigned long>();
	run_to_completion = (vm.count("hyperanf::run_to_completion") > 0);
      }
      bool reduce(per_processor_data ** per_cpu_array,
		  unsigned long processors)
      {
	if(iteration == 0) {
	  iteration++;
	  return false; // Nothing done in iteration zero
	}
	float sum = 0.0;
	float seed_reach = 0.0;
	vertex_t seed = (vertex_t)-1;
	for(unsigned long i=0;i<processors;i++) {
	  hyperanf_pp_data * cpu_data =
	    static_cast<hyperanf_pp_data *>(per_cpu_array[i]);
	  sum += cpu_data->local_neighbour_cnt;
	  cpu_data->local_neighbour_cnt = 0;
	  if(cpu_data->local_seed_reach  > seed_reach) {
	    seed_reach = cpu_data->local_seed_reach;
	    seed = cpu_data->local_seed;
	  }
	  cpu_data->local_seed_reach = 0.0f;
	  cpu_data->local_seed = 0;
	}
	BOOST_LOG_TRIVIAL(info) << "ALGORITHM::ANF " << 
	  (iteration - 1) << " " << std::fixed << sum;
	BOOST_LOG_TRIVIAL(info) << "ALGORITHM::ANF::SEED " << seed;
	BOOST_LOG_TRIVIAL(info) << "ALGORITHM::ANF::SEED_REACH " << 
	  std::fixed << seed_reach;
	iteration++;
	if(run_to_completion) {
	  return false;
	}
	// Convergence ?
	if(global_neighbour_cnt > 0.0 && 
	   (fabs(sum - global_neighbour_cnt)/global_neighbour_cnt) < epsilon) {
	  BOOST_LOG_TRIVIAL(info) << "ALGORITHM::ANF::epsilon_stop " << epsilon;
	  return true;
	}
	else {
	  global_neighbour_cnt = sum;
	  if(iteration >= maxiters) {
	    BOOST_LOG_TRIVIAL(info) << "ALGORITHM::ANF::iter_stop " << maxiters;
	    return true;
	  }
	  else {
	    return false;
	  }
	}
      }
    } __attribute__((__aligned__(64)));
    template<typename F>
    class hyperanf {
      static struct hyper_log_log_params hll_param;
    public:
      struct __attribute__((__packed__)) hyperanf_update {
	vertex_t dst;
	unsigned char hll_ctr[0] HLL_ALIGN;
      };
      struct __attribute__((__packed__)) hyperanf_vertex {
	bool changed;
	float count;
	unsigned char hll_ctr[0] HLL_ALIGN;
      };
      static unsigned long split_size_bytes()
      {
	return sizeof(struct hyperanf_update) + 
	  sizeof_hll_counter(&hll_param);
      }
      static unsigned long vertex_state_bytes()
      {
	return sizeof(hyperanf_vertex) +
	  sizeof_hll_counter(&hll_param);
      }
      
      static hyperanf_vertex * 
      index_vertices(unsigned char *vstream, unsigned long index)
      {
	return (hyperanf_vertex *)(vstream + index*vertex_state_bytes());
      }

      static unsigned long split_key(unsigned char *buffer,
				     unsigned long jump)
      {
	struct hyperanf_update *update = (struct hyperanf_update *)buffer;
	vertex_t key = update->dst;
	key = key >> jump;
	return key;
      }
      
      static bool apply_one_update(unsigned char *vertex_state,
				   unsigned char *update_stream,
				   per_processor_data *per_cpu_data,
				   unsigned long bsp_phase)
      {
	hyperanf_update *u   = (hyperanf_update *)update_stream;
	hyperanf_vertex *vertex = index_vertices
	  (vertex_state, x_lib::configuration::map_offset(u->dst));
	bool changed = hll_union(vertex->hll_ctr, u->hll_ctr, &hll_param);
	vertex->changed = vertex->changed || changed;
	return changed;
      }
    
      static bool generate_update(unsigned char *vertex_state,
				  unsigned char *edge_format,
				  unsigned char *update_stream,
				  per_processor_data *per_cpu_data,
				  unsigned long bsp_phase)
      {
	vertex_t src, dst;
	F::read_edge(edge_format, src, dst);
	hyperanf_vertex *vertex = index_vertices
	  (vertex_state, x_lib::configuration::map_offset(src));
	if(vertex->changed) {
	  hyperanf_update *u   = (hyperanf_update *)update_stream;
	  memcpy(u->hll_ctr, vertex->hll_ctr,
		 sizeof_hll_counter(&hll_param));
	  u->dst = dst;
	  return true;
	}
	else {
	  return false;
	}
      }
      
      static bool init(unsigned char *vertex_state,
		       unsigned long vertex_index,
		       unsigned long bsp_phase,
		       per_processor_data *cpu_state)
      {
	struct hyperanf_vertex *vstate = (struct hyperanf_vertex *)vertex_state;
	if(bsp_phase == 0) {
	  hll_init(vstate->hll_ctr, &hll_param);
	  unsigned long hash = jenkins(vertex_index, 0xdeadbeef);
	  add_hll_counter(&hll_param, vstate->hll_ctr, hash);
	  vstate->changed = true;
	  return true;
	}
	else {
	  // rollup counters
	  hyperanf_pp_data * pp_data = 
	    static_cast<hyperanf_pp_data *>(cpu_state);
	  if(vstate->changed) {
	    vstate->count = count_hll_counter(&hll_param, vstate->hll_ctr);
	    vstate->changed = false;
	  }
	  pp_data->local_neighbour_cnt += vstate->count;
	  if(vstate->count > pp_data->local_seed_reach) {
	    pp_data->local_seed_reach = vstate->count;
	    pp_data->local_seed       = vertex_index;
	  }
	  return false;
	}
      }
      
      static bool need_init(unsigned long bsp_phase)
      {
	return true;
      }

      static per_processor_data *
      create_per_processor_data(unsigned long processor_id)
      {
	return new hyperanf_pp_data();
      }
      
      static void preprocessing()
      {
	setup_hll_params(&hll_param, vm["hyperanf::rsd"].as<float>());
	print_hll_params(&hll_param);
	if(pt.get<unsigned long>("graph.vertices") > (1UL << 60)) {
	  BOOST_LOG_TRIVIAL(fatal) << "Graph too large for hyper anf !";
	  exit(-1);
	}
      }

      static void postprocessing()
      {
      
      }

      static unsigned long min_super_phases()
      {
	return 1;
      }
    };
    unsigned long hyperanf_pp_data::iteration     = 0;
    float hyperanf_pp_data::global_neighbour_cnt = 0.0;
    template<typename F>
    struct hyper_log_log_params hyperanf<F>::hll_param;
  }
}
#endif
