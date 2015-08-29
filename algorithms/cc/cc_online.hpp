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

#ifndef _CC_ONLINE_
#define _CC_ONLINE_
#include<sys/time.h>
#include<sys/resource.h>
#include "../../core/x-lib.hpp"

// Online computation of wcc on a growing graph

namespace algorithm {
  namespace cc_online_ns {
    const static unsigned long step_split_edges         = 0;
    const static unsigned long step_updates_in          = 1;
    const static unsigned long step_updates_out         = 2;
    const static vertex_t SENTINEL = (vertex_t)ULONG_MAX;

    struct cc_online_pcpu:public per_processor_data {
      unsigned long processor_id;
      unsigned long component_merges_local;
      /* begin work specs. */
      static unsigned long current_step;
      static unsigned long bfs_iteration;
      static unsigned long batch;
      static unsigned long component_merges;
      /* end work specs. */
      bool reduce(per_processor_data **per_cpu_array,
		  unsigned long processors)
      {
	if(current_step == step_updates_out) {
	  for(unsigned long i=0;i<processors;i++) {
	    cc_online_pcpu *cpu =
	      static_cast<cc_online_pcpu *>(per_cpu_array[i]);
	    component_merges += cpu->component_merges_local;
	    cpu->component_merges_local = 0;
	  }
	}
	return false; 
      }
      cc_online_pcpu(unsigned long processor_id_in)
	:processor_id(processor_id_in),
	 component_merges_local(0)
      {
      }
    } __attribute__((__aligned__(64)));

    struct __attribute__((__packed__)) cc_online_vertex {
      vertex_t component;
      unsigned long update_iteration;
      unsigned long seen_batch;
    };

    struct __attribute__((__packed__)) cc_online_update {
      vertex_t component;
      vertex_t dst;
    };

    template<typename F>
    class cc_online {
      static cc_online_pcpu ** pcpu_array;
      bool heartbeat;
      x_lib::streamIO<cc_online> *graph_storage;
      unsigned long vertex_stream;
      unsigned long edges_stream;
      unsigned long updates_stream;
      ingest_t *ingest_buffer;
      rtc_clock wall_clock;
      rtc_clock setup_time;
    public:
      cc_online();
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
	return 3; 
      }
      
      static unsigned long max_buffers()
      {
	return 5;
      }

      static unsigned long vertex_state_bytes()
      {
	return sizeof(struct cc_online_vertex);
      }

      static unsigned long vertex_stream_buffer_bytes()
      {
	return sizeof(cc_online_vertex) + 
	  MAX(F::split_size_bytes(), sizeof(cc_online_update));
      }
    
      static void state_iter_callback(unsigned long superp, 
				      unsigned long partition,
				      unsigned long index,
				      unsigned char *vertex,
				      per_processor_data *cpu_state)
      {
	cc_online_vertex *v = (cc_online_vertex *)vertex;	
	unsigned long global_index = 
	  x_lib::configuration::map_inverse(superp, partition, index);
	v->component = global_index;
	v->update_iteration = 0; 
      }

      static per_processor_data * 
      create_per_processor_data(unsigned long processor_id)
      {
	return pcpu_array[processor_id];
      }
  
      static void do_cpu_callback(per_processor_data *cpu_state)
      {
      }

      void state_store(unsigned long superp)
      {
	if(graph_storage->get_config()->super_partitions > 1) {
	  graph_storage->state_store(vertex_stream, superp);
	}
      }

      void state_load(bool first_touch, unsigned long superp)
      {
	if(graph_storage->get_config()->super_partitions > 1) {
	  if(!first_touch) {
	    graph_storage->state_load(vertex_stream, superp);
	  }
	  graph_storage->state_prepare(superp);
	}
	else {
	  if(first_touch) {
	    graph_storage->state_prepare(superp);
	  }
	}
      }
    };
  
    template<typename F>
    cc_online<F>::cc_online()
    {
      wall_clock.start();
      setup_time.start();
      heartbeat = (vm.count("heartbeat") > 0);
      unsigned long num_processors = vm["processors"].as<unsigned long>();
      pcpu_array = new cc_online_pcpu *[num_processors];
      for(unsigned long i=0;i<num_processors;i++) {
	pcpu_array[i] = new cc_online_pcpu(i);
      }
      graph_storage = new x_lib::streamIO<cc_online>();
      vertex_stream = 
	graph_storage->open_stream("vertices", true, 
				   vm["vertices_disk"].as<unsigned long>(),
				   graph_storage->get_config()->vertex_size);
      std::string efile = pt.get<std::string>("graph.name");
      edges_stream = 
	graph_storage->open_stream("edges", true,
				   vm["edges_disk"].as<unsigned long>(),
				   F::split_size_bytes());
      updates_stream = 
	graph_storage->open_stream("updates", true, 
				   vm["updates0_disk"].as<unsigned long>(),
				   sizeof(cc_online_update));
      ingest_buffer =
	create_ingest_buffer(vm["cc_online::shm_key"].as<unsigned long>(),
			     graph_storage->get_config()->buffer_size);
      setup_time.stop();
    }
  
    template<typename F> 
    struct cc_online_edge_wrapper
    {
      static unsigned long item_size()
      {
	return F::split_size_bytes();
      }
    
      static unsigned long key(unsigned char *buffer)
      {
	return F::split_key(buffer, 0);
      }
    };

    struct cc_online_update_wrapper
    {
      static unsigned long item_size()
      {
	return sizeof(cc_online_update);
      }
    
      static unsigned long key(unsigned char *buffer)
      {
	return ((cc_online_update *)buffer)->dst;
      }
    };

    template<typename F>
    void cc_online<F>::operator() ()
    {
      const x_lib::configuration *config = graph_storage->get_config();
      unsigned long init_components = pt.get<unsigned long>("graph.vertices");
      // Main loop
      bool do_init = true;
      do {
	bool eof = x_lib::ingest<cc_online<F> > 
	  (graph_storage, edges_stream, ingest_buffer);
	if(eof) {
	  break;
	}
	do {
	  graph_storage->rewind_stream(edges_stream);
	  cc_online_pcpu::current_step = step_updates_out;
	  for(unsigned long i=0;i<config->super_partitions;i++) {
	    state_load(do_init, i);
	    do_init = false;
	    if(cc_online_pcpu::bfs_iteration == 0) {
	      x_lib::do_state_iter<cc_online<F> > (graph_storage, i);
	    }
	    x_lib::do_stream< cc_online<F>, 
			      cc_online_edge_wrapper<F>, 
			      cc_online_update_wrapper >
	      (graph_storage, i, edges_stream, updates_stream, NULL);
	    state_store(i);
	  }
	  x_lib::merge_ingest<cc_online<F> >
	    (graph_storage, edges_stream);
	  if(graph_storage->stream_empty(updates_stream)) {
	    break;
	  }
	  cc_online_pcpu::bfs_iteration++;
	  graph_storage->rewind_stream(updates_stream);
	  for(unsigned long i=0;i<config->super_partitions;i++) {
	    state_load(do_init, i);
	    do_init = false;
	    cc_online_pcpu::current_step = step_updates_in;
	    x_lib::do_stream< cc_online<F>, 
			      cc_online_update_wrapper, 
			      cc_online_update_wrapper >
	      (graph_storage, i, updates_stream, ULONG_MAX, NULL);
	    graph_storage->reset_stream(updates_stream, i);
	    (void)x_lib::do_cpu<cc_online<F> >(graph_storage, i);
	    state_store(i);
	  }
	  if(heartbeat) {
	    BOOST_LOG_TRIVIAL(info) << clock::timestamp() 
				    << " Heartbeat: " 
				    << cc_online_pcpu::bfs_iteration
				    << " WCC_MERGES: " <<
	      cc_online_pcpu::component_merges;

	  }
	} while(1);
      } while(1);
      setup_time.start();
      graph_storage->terminate();
      setup_time.stop();
      wall_clock.stop();
      BOOST_LOG_TRIVIAL(info) << "ALGORITHM::WCC::COUNT " <<
	init_components - cc_online_pcpu::component_merges;
      setup_time.print("CORE::TIME::SETUP");
      wall_clock.print("CORE::TIME::WALL");
    }

    template<typename F>
    void cc_online<F>::partition_pre_callback(unsigned long superp, 
					     unsigned long partition,
					     per_processor_data *pcpu)
    {
    }

    template<typename F>
    void cc_online<F>::partition_callback (x_lib::stream_callback_state *callback)
    {
      cc_online_pcpu *pcpu = static_cast<cc_online_pcpu *>(callback->cpu_state);
      switch(cc_online_pcpu::current_step) {
      
      case step_updates_in: {
	while(callback->bytes_in) {
	  cc_online_update *m = (cc_online_update *)callback->bufin;
	  cc_online_vertex *v = ((cc_online_vertex *)(callback->state)) +
	    x_lib::configuration::map_offset(m->dst);
	  if(v->component > m->component) {
	    BOOST_ASSERT_MSG(m->component != m->dst,
			     "error in label update !");
	    if(v->component == m->dst) {
	      pcpu->component_merges_local++;
	    }
	    v->component        = m->component;
	    v->update_iteration = cc_online_pcpu::bfs_iteration;
	  }
	  callback->bufin     += sizeof(cc_online_update);
	  callback->bytes_in  -= sizeof(cc_online_update);
	}
	break;
      }

      case step_updates_out: {
	while(callback->bytes_in) {
	  vertex_t src, dst;
	  F::read_edge(callback->bufin, src, dst);
	  cc_online_vertex *v = ((cc_online_vertex *)(callback->state)) +
	    x_lib::configuration::map_offset(src);
	  if(callback->ingest) {
	    v->update_iteration = cc_online_pcpu::bfs_iteration;
	  }
	  if(v->update_iteration == cc_online_pcpu::bfs_iteration) {
	    if((callback->bytes_out + sizeof(cc_online_update)) > 
	       callback->bytes_out_max) {
	      break;
	    }
	    cc_online_update *mout = (cc_online_update *)(callback->bufout);
	    mout->component       = v->component;
	    mout->dst             = dst;
	    callback->bufout     += sizeof(cc_online_update);
	    callback->bytes_out  += sizeof(cc_online_update);
	  }
	  callback->bufin     += F::split_size_bytes();
	  callback->bytes_in  -= F::split_size_bytes();
	}
	break;
      }
      
      default:
	BOOST_LOG_TRIVIAL(fatal) << "Unknown operation in stream callback !";
	exit(-1);
      }
    }
  
    template<typename F>
    void cc_online<F>::partition_post_callback(unsigned long superp, 
					      unsigned long partition,
					      per_processor_data *pcpu)
    {
    }

    template<typename F> cc_online_pcpu ** cc_online<F>::pcpu_array = NULL;
    unsigned long cc_online_pcpu::current_step;
    unsigned long cc_online_pcpu::bfs_iteration = 0;
    unsigned long cc_online_pcpu::component_merges  = 0;
  }
}
#endif
