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

#ifndef _SG_DRIVER_ASYNC_
#define _SG_DRIVER_ASYNC_
#include<sys/time.h>
#include<sys/resource.h>
#include "x-lib.hpp"


// Implement a wrapper for simpler graph algorithms that alternate between
// scatter and gather and support asynchronous application of updates

namespace algorithm {

  struct sg_async_pcpu:public per_processor_data {
    unsigned long processor_id;
    static sg_async_pcpu ** per_cpu_array;
    static x_barrier *sync;
    // Stats
    unsigned long update_bytes_out;
    unsigned long update_bytes_in;
    unsigned long edge_bytes_streamed;
    unsigned long partitions_processed;
    // 
    
    /* begin work specs. */
    static unsigned long bsp_phase;
    static unsigned long current_step;
    static bool do_algo_reduce;
    /* end work specs. */

    static per_processor_data **algo_pcpu_array;
    per_processor_data *algo_pcpu;

    bool reduce(per_processor_data **per_cpu_array,
		unsigned long processors)
    {
      if(algo_pcpu_array[0] != NULL && do_algo_reduce) {
	return algo_pcpu_array[0]->reduce(algo_pcpu_array, processors);
      }
      else {
	return false; // Should be don't care
      }
    }
  } __attribute__((__aligned__(64)));

  template<typename A, typename F>
  class async_scatter_gather {
    sg_async_pcpu ** pcpu_array;
    bool heartbeat;
    x_lib::streamIO<async_scatter_gather> *graph_storage;
    unsigned long vertex_stream;
    unsigned long edge_stream;
    unsigned long updates0_stream;
    unsigned long updates1_stream;
    unsigned long init_stream;
    rtc_clock wall_clock;
    rtc_clock setup_time;

  public:
    async_scatter_gather();
    static void partition_pre_callback(unsigned long super_partition,
				       unsigned long partition,
				       per_processor_data* cpu_state);
    static void partition_callback(x_lib::stream_callback_state *state);
    static void partition_post_callback(unsigned long super_partition,
					unsigned long partition,
					per_processor_data *cpu_state);
    void operator() ();
    static unsigned long max_streams()
    {
      return 5; // vertices, edges, init_edges, updates0, updates1
    }
    static unsigned long max_buffers()
    {
      return 4;
    }

    static unsigned long vertex_state_bytes()
    {
      return A::vertex_state_bytes();
    }

    static unsigned long vertex_stream_buffer_bytes()
    {
      return A::split_size_bytes() + F::split_size_bytes();
    }

    static void state_iter_callback(unsigned long superp, 
				    unsigned long partition,
				    unsigned long index,
				    unsigned char *vertex,
				    per_processor_data *cpu_state)
    {
      unsigned long global_index = 
	x_lib::configuration::map_inverse(superp, partition, index);
      sg_async_pcpu *pcpu = static_cast<sg_async_pcpu *>(cpu_state);
      A::init(vertex, global_index,
	      sg_async_pcpu::bsp_phase,
	      sg_async_pcpu::algo_pcpu_array[pcpu->processor_id]);
    }

    static per_processor_data * 
    create_per_processor_data(unsigned long processor_id)
    {
      return sg_async_pcpu::per_cpu_array[processor_id];
    }
  
    static void do_cpu_callback(per_processor_data *cpu_state)
    {
      sg_async_pcpu *cpu = static_cast<sg_async_pcpu *>(cpu_state);
      if(sg_async_pcpu::current_step == superphase_begin) {
      }
      else if(sg_async_pcpu::current_step == phase_post_scatter) {
      }
      else if(sg_async_pcpu::current_step == phase_terminate) {
	BOOST_LOG_TRIVIAL(info)<< "CORE::PARTITIONS_PROCESSED " << cpu->partitions_processed;
	BOOST_LOG_TRIVIAL(info)<< "CORE::BYTES::EDGES_STREAMED " << cpu->edge_bytes_streamed;
	BOOST_LOG_TRIVIAL(info)<< "CORE::BYTES::UPDATES_OUT " << cpu->update_bytes_out;
	BOOST_LOG_TRIVIAL(info)<< "CORE::BYTES::UPDATES_IN " << cpu->update_bytes_in;
      }
    }
  };
  
  template<typename A, typename F>
  async_scatter_gather<A, F>::async_scatter_gather()
  {
    wall_clock.start();
    setup_time.start();
    heartbeat = (vm.count("heartbeat") > 0);
    unsigned long num_processors = vm["processors"].as<unsigned long>();
    per_processor_data **algo_pcpu_array = new per_processor_data *[num_processors];
    sg_async_pcpu::per_cpu_array = pcpu_array = new sg_async_pcpu *[num_processors];
    sg_async_pcpu::sync = new x_barrier(num_processors);
    sg_async_pcpu::do_algo_reduce = false;
    for(unsigned long i=0;i<num_processors;i++) {
      pcpu_array[i] = new sg_async_pcpu();
      pcpu_array[i]->processor_id = i;
      pcpu_array[i]->update_bytes_in = 0;
      pcpu_array[i]->update_bytes_out = 0;
      pcpu_array[i]->edge_bytes_streamed = 0;
      pcpu_array[i]->partitions_processed = 0;
      algo_pcpu_array[i] = A::create_per_processor_data(i);
    }
    sg_async_pcpu::algo_pcpu_array = algo_pcpu_array;
    A::preprocessing(); // Note: ordering critical with the next statement
    graph_storage = new x_lib::streamIO<async_scatter_gather>();
    sg_async_pcpu::bsp_phase = 0;
    vertex_stream = 
      graph_storage->open_stream("vertices", true, 
				 vm["vertices_disk"].as<unsigned long>(),
				 graph_storage->get_config()->vertex_size);
    if(graph_storage->get_config()->super_partitions == 1) {
      std::string efile = pt.get<std::string>("graph.name");
      edge_stream = 
	graph_storage->open_stream((const char *)efile.c_str(), false,
				   vm["input_disk"].as<unsigned long>(),
				   F::split_size_bytes());
    }
    else {
      edge_stream = 
	graph_storage->open_stream("edges", true, 
				   vm["edges_disk"].as<unsigned long>(),
				   F::split_size_bytes());
      std::string efile = pt.get<std::string>("graph.name");
      init_stream = 
	graph_storage->open_stream((const char *)efile.c_str(), false,
				   vm["input_disk"].as<unsigned long>(),
				   F::split_size_bytes(), 1);
    }
    updates0_stream = 
      graph_storage->open_stream("updates0", true, 
				 vm["updates0_disk"].as<unsigned long>(),
				 A::split_size_bytes());
    updates1_stream = 
      graph_storage->open_stream("updates1", true, 
				 vm["updates1_disk"].as<unsigned long>(),
				 A::split_size_bytes());
    setup_time.stop();
  }
  
  template<typename A, typename F>
  void async_scatter_gather<A, F>::operator() ()
  {
    const x_lib::configuration *config = graph_storage->get_config();
    // Edge split
    if(config->super_partitions > 1) {
      sg_async_pcpu::current_step = phase_edge_split;
      x_lib::do_stream< async_scatter_gather<A, F>, 
			edge_type_wrapper<F>, 
			edge_type_wrapper<F> >
	(graph_storage, 0, init_stream, edge_stream, NULL);
      graph_storage->close_stream(init_stream);
    }
    // Supersteps
    unsigned long PHASE = 0;
    bool global_stop = false;
    while(true) {
      sg_async_pcpu::current_step = superphase_begin;
      x_lib::do_cpu<async_scatter_gather<A, F> >(graph_storage, ULONG_MAX);
      unsigned long updates_in_stream = (PHASE == 0 ? updates1_stream:updates0_stream);
      unsigned long updates_out_stream = (PHASE == 0 ? updates0_stream:updates1_stream);
      graph_storage->rewind_stream(edge_stream);
      for(unsigned long i=0;i<graph_storage->get_config()->super_partitions;i++) {
	if(graph_storage->get_config()->super_partitions > 1) {
	  if(sg_async_pcpu::bsp_phase > 0) {
	    graph_storage->state_load(vertex_stream, i);
	  }
	  graph_storage->state_prepare(i);
	}
	else if(sg_async_pcpu::bsp_phase == 0) {
	  graph_storage->state_prepare(0);
	}
	if(A::need_init(sg_async_pcpu::bsp_phase)) {
	  x_lib::do_state_iter<async_scatter_gather<A, F> > (graph_storage, i);
	}
	sg_async_pcpu::current_step = phase_gather;
	x_lib::do_stream<async_scatter_gather<A, F>, 
			 update_type_wrapper<A>,
			 update_type_wrapper<A> >
	  (graph_storage, i, updates_in_stream, ULONG_MAX, NULL);
	graph_storage->reset_stream(updates_in_stream, i);
	sg_async_pcpu::current_step = phase_scatter;
	x_lib::do_cpu<async_scatter_gather<A, F> >(graph_storage, i);
	x_lib::do_stream<async_scatter_gather<A, F>, 
			 edge_type_wrapper<F>,
			 update_type_wrapper<A> >
	  (graph_storage, i, edge_stream, updates_out_stream, NULL, true);
	sg_async_pcpu::current_step = phase_post_scatter;
	if(i == (graph_storage->get_config()->super_partitions - 1)) {
	  sg_async_pcpu::do_algo_reduce = true;
	}
	global_stop = x_lib::do_cpu<async_scatter_gather<A, F> >(graph_storage, i);
	sg_async_pcpu::do_algo_reduce = false;
	if(graph_storage->get_config()->super_partitions > 1) {
	  graph_storage->state_store(vertex_stream, i);
	}
      }
      graph_storage->rewind_stream(updates_out_stream);
      if(graph_storage->get_config()->super_partitions > 1) {
	graph_storage->rewind_stream(vertex_stream);
      }
      PHASE = 1 - PHASE;
      sg_async_pcpu::bsp_phase++;
      if(heartbeat) {
	BOOST_LOG_TRIVIAL(info) << clock::timestamp() << " Completed phase " <<
	  sg_async_pcpu::bsp_phase;
      }
      if(sg_async_pcpu::bsp_phase > A::min_super_phases()) {
	if(global_stop) {
	  break;
	}
      }
    }
    if(graph_storage->get_config()->super_partitions == 1) {
      graph_storage->state_store(vertex_stream, 0);
    }
    A::postprocessing();
    sg_async_pcpu::current_step = phase_terminate;
    x_lib::do_cpu<async_scatter_gather<A, F> >(graph_storage, ULONG_MAX);
    setup_time.start();
    graph_storage->terminate();
    setup_time.stop();
    wall_clock.stop();
    BOOST_LOG_TRIVIAL(info) << "CORE::PHASES " << sg_async_pcpu::bsp_phase;
    setup_time.print("CORE::TIME::SETUP");
    wall_clock.print("CORE::TIME::WALL");
  }

  template<typename A, typename F>
  void async_scatter_gather<A, F>::partition_pre_callback(unsigned long superp, 
						    unsigned long partition,
						    per_processor_data *pcpu)
  {
    sg_async_pcpu *pcpu_actual = static_cast<sg_async_pcpu *>(pcpu);
    if(pcpu_actual->current_step == phase_gather) {
    }
  }

  
  template<typename A, typename F>
  void async_scatter_gather<A, F>::partition_callback
  (x_lib::stream_callback_state *callback)
  {
    sg_async_pcpu *pcpu = static_cast<sg_async_pcpu *>(callback->cpu_state);
    if(callback->loopback) {
      pcpu->update_bytes_in += callback->bytes_in;
      pcpu->partitions_processed++;
      while(callback->bytes_in) {
	A::apply_one_update(callback->state,
			    callback->bufin,
			    sg_async_pcpu::algo_pcpu_array[pcpu->processor_id],
			    true,
			    pcpu->bsp_phase);
	callback->bufin += A::split_size_bytes();
	callback->bytes_in -= A::split_size_bytes();
      }
      return;
    }
    switch(sg_async_pcpu::current_step) {
    case phase_edge_split: {
      unsigned long bytes_to_copy = 
	(callback->bytes_in < callback->bytes_out_max) ?
	callback->bytes_in:callback->bytes_out_max;
      callback->bytes_in -= bytes_to_copy;
      memcpy(callback->bufout, callback->bufin, bytes_to_copy);
      callback->bufin += bytes_to_copy;
      callback->bytes_out = bytes_to_copy;
      break;
    }
    case phase_gather: {
      pcpu->update_bytes_in += callback->bytes_in;
      while(callback->bytes_in) {
	A::apply_one_update(callback->state,
			    callback->bufin,
			    sg_async_pcpu::algo_pcpu_array[pcpu->processor_id],
			    false,
			    pcpu->bsp_phase);
	callback->bufin += A::split_size_bytes();
	callback->bytes_in -= A::split_size_bytes();
      }
      break;
    }
    case phase_scatter: {
      unsigned long tmp = callback->bytes_in;
      unsigned char *bufout = callback->bufout;
      while(callback->bytes_in) {
	if((callback->bytes_out + A::split_size_bytes()) >
	   callback->bytes_out_max) {
	  break;
	}
	bool up = A::generate_update(callback->state,
				     callback->bufin, 
				     bufout,
				     sg_async_pcpu::algo_pcpu_array[pcpu->processor_id],
				     sg_async_pcpu::bsp_phase);
	callback->bufin    += F::split_size_bytes();
	callback->bytes_in -= F::split_size_bytes();
	if(up) {
	  callback->bytes_out += A::split_size_bytes();
	  bufout += A::split_size_bytes();
	}
      }
      pcpu->update_bytes_out    += callback->bytes_out;
      pcpu->edge_bytes_streamed += (tmp - callback->bytes_in); 
      break;
    }
    default:
      BOOST_LOG_TRIVIAL(fatal) << "Unknown operation in stream callback !";
      exit(-1);
    }
  }

  template<typename A, typename F>
  void async_scatter_gather<A, F>::partition_post_callback(unsigned long superp, 
						     unsigned long partition,
						     per_processor_data *pcpu)
  {
    sg_async_pcpu *pcpu_actual = static_cast<sg_async_pcpu *>(pcpu);
    if(pcpu_actual->current_step == phase_gather) {
      pcpu_actual->partitions_processed++;
    }
  }
}
#endif
