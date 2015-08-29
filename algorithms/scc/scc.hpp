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

#ifndef _SCC_
#define _SCC_
#include<sys/time.h>
#include<sys/resource.h>
#include "../../core/x-lib.hpp"

// Strongly connected component using forward and backward reachability queries
// do {
//  forward label propagation
//  remove fwd edges with different labels at end points
//  backward label propagation
//  remove back edges with different labels at end points
// } until no edges removed

// Inspired by the similar implementation in Naiad
// http://bigdataatsvc.wordpress.com/2012/10/18/strongly-connected-components-in-naiad/

namespace algorithm {
  namespace scc {
    const static unsigned long step_split_edges         = 0;
    const static unsigned long step_wcc_in              = 1;
    const static unsigned long step_wcc_out             = 2;
    const static unsigned long step_elim_push           = 3;
    const static unsigned long step_elim_write          = 4;
    const static unsigned long step_count_scc           = 5;
    const static unsigned long step_terminate           = 6;

    struct scc_pcpu:public per_processor_data {
      unsigned long processor_id;
      unsigned long scc_local;
      unsigned long elims_local;
      const unsigned long num_processors;
      /* begin work specs. */
      static unsigned long current_step;
      static unsigned long wcc_iteration;
      static unsigned long scc_iteration;
      static unsigned long scc_global;
      static unsigned long elims_global;

      /* end work specs. */
      bool reduce(per_processor_data **per_cpu_array,
		  unsigned long processors)
      {
	if(current_step == step_elim_write) {
	  for(unsigned long i=0;i<num_processors;i++) {
	    scc_pcpu *cpu = static_cast<scc_pcpu *>(per_cpu_array[i]);
	    elims_global     += cpu->elims_local;
	    cpu->elims_local  = 0;
	  }
	}
	else if(current_step == step_terminate) {
	  for(unsigned long i=0;i<num_processors;i++) {
	    scc_pcpu *cpu = static_cast<scc_pcpu *>(per_cpu_array[i]);
	    scc_global       += cpu->scc_local;
	    cpu->scc_local    = 0;
	  }
	}
	return false; 
      }
      scc_pcpu(unsigned long processor_id_in,
	       unsigned long num_processors_in)
	:processor_id(processor_id_in),
	 scc_local(0),
	 elims_local(0),
	 num_processors(num_processors_in)
      {
      }
    } __attribute__((__aligned__(64)));

    struct __attribute__((__packed__)) scc_vertex {
      vertex_t prev_root;
      vertex_t component_root;
      unsigned long update_iteration;
    };

    struct __attribute__((__packed__)) scc_edge {
      vertex_t src;
      vertex_t dst;
    };

    struct __attribute__((__packed__)) scc_message {
      vertex_t src;
      vertex_t dst;
      vertex_t component;
    };

    template<typename F>
    class scc {
      static scc_pcpu ** pcpu_array;
      bool heartbeat;
      x_lib::streamIO<scc> *graph_storage;
      unsigned long vertex_stream;
      unsigned long init_stream;
      unsigned long edges_stream;
      unsigned long messages_stream0;
      unsigned long messages_stream1;
      rtc_clock wall_clock;
      rtc_clock setup_time;

    public:
      scc();
      static void partition_pre_callback(unsigned long super_partition,
					 unsigned long partition,
					 per_processor_data* cpu_state);
      static void partition_callback(x_lib::stream_callback_state *state);
      static void partition_post_callback(unsigned long super_partition,
					  unsigned long partition,
					  per_processor_data *cpu_state);
      
      void scc_do_cc(bool&do_init);
      void scc_do_elim();
      void operator() ();
      
      static unsigned long max_streams()
      {
	return 5; 
      }
      
      static unsigned long max_buffers()
      {
	return 4;
      }

      static unsigned long vertex_state_bytes()
      {
	return sizeof(struct scc_vertex);
      }

      static unsigned long vertex_stream_buffer_bytes()
      {
	return sizeof(scc_vertex) + 
	  MAX(F::split_size_bytes(), sizeof(scc_message));
      }
    
      static void state_iter_callback(unsigned long superp, 
				      unsigned long partition,
				      unsigned long index,
				      unsigned char *vertex,
				      per_processor_data *cpu_state)
      {
	scc_vertex *v = (scc_vertex *)vertex;	
	if(scc_pcpu::current_step == step_count_scc) {
	  scc_pcpu *pcpu = static_cast<scc_pcpu *>(cpu_state);
	  if(v->prev_root == v->component_root &&
	     v->component_root == 
	     x_lib::configuration::map_inverse(superp, partition, index)) {
	    pcpu->scc_local++;
	  }
	}
	else {
	  if(scc_pcpu::scc_iteration == 0) {
	    v->prev_root = (vertex_t)ULONG_MAX;
	    v->component_root = (vertex_t)
	      x_lib::configuration::map_inverse(superp, partition, index);
	    v->update_iteration = 0;
	  } 
	  else if(v->prev_root != v->component_root) {
	    v->prev_root = v->component_root;
	    v->component_root = (vertex_t)
	      x_lib::configuration::map_inverse(superp, partition, index);
	    v->update_iteration = 0;
	  }
	  else { // Already in discovered CC
	    v->update_iteration = ULONG_MAX;
	  }
	}
      }

      static per_processor_data * 
      create_per_processor_data(unsigned long processor_id)
      {
	return pcpu_array[processor_id];
      }
  
      static void do_cpu_callback(per_processor_data *cpu_state)
      {
	// Nothing
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
    scc<F>::scc()
    {
      wall_clock.start();
      setup_time.start();
      heartbeat = (vm.count("heartbeat") > 0);
      unsigned long num_processors = vm["processors"].as<unsigned long>();
      pcpu_array = new scc_pcpu *[num_processors];
      for(unsigned long i=0;i<num_processors;i++) {
	pcpu_array[i] = new scc_pcpu(i, num_processors);
      }
      graph_storage = new x_lib::streamIO<scc>();
      vertex_stream = 
	graph_storage->open_stream("vertices", true, 
				   vm["vertices_disk"].as<unsigned long>(),
				   graph_storage->get_config()->vertex_size);
      std::string efile = pt.get<std::string>("graph.name");
      init_stream = 
	graph_storage->open_stream((const char *)efile.c_str(), false,
				   vm["input_disk"].as<unsigned long>(),
				   F::split_size_bytes(), 1);
      edges_stream = 
	graph_storage->open_stream("edges", true,
				   vm["edges_disk"].as<unsigned long>(),
				   sizeof(scc_edge));
      messages_stream0 = 
	graph_storage->open_stream("messages0", true, 
				   vm["updates0_disk"].as<unsigned long>(),
				   sizeof(scc_message));
      messages_stream1 = 
	graph_storage->open_stream("messages1", true,
				   vm["updates1_disk"].as<unsigned long>(),
				   sizeof(scc_message));
      setup_time.stop();
    }
  
    template<typename F> 
    struct edge_wrapper
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

    struct scc_edge_wrapper
    {
      static unsigned long item_size()
      {
	return sizeof(scc_edge);
      }
    
      static unsigned long key(unsigned char *buffer)
      {
	return ((scc_edge *)buffer)->src;
      }
    };

    struct scc_message_wrapper
    {
      static unsigned long item_size()
      {
	return sizeof(scc_message);
      }
    
      static unsigned long key(unsigned char *buffer)
      {
	return ((scc_message *)buffer)->dst;
      }
    };

    template<typename F>
    void scc<F>::scc_do_cc(bool&do_init)
    {
      const x_lib::configuration *config = graph_storage->get_config();
      unsigned long PHASE = 0;
      unsigned long messages_in_stream;
      unsigned long messages_out_stream;
      scc_pcpu::wcc_iteration = 0;
      do {
	if(PHASE == 0) {
	  messages_in_stream  = messages_stream0;
	  messages_out_stream = messages_stream1;
	}
	else {
	  messages_in_stream  = messages_stream1;
	  messages_out_stream = messages_stream0;
	}
	graph_storage->rewind_stream(messages_in_stream);
	graph_storage->rewind_stream(edges_stream);
	for(unsigned long i=0;i<config->super_partitions;i++) {
	  state_load(do_init, i);
	  do_init = false;
	  scc_pcpu::current_step = step_wcc_in;
	  if(scc_pcpu::wcc_iteration == 0) {
	    x_lib::do_state_iter<scc<F> > (graph_storage, i);
	  }
	  x_lib::do_stream< scc<F>, 
			    scc_message_wrapper, 
			    scc_message_wrapper >
	    (graph_storage, i, messages_in_stream, ULONG_MAX, NULL);
	  graph_storage->reset_stream(messages_in_stream, i);
	  scc_pcpu::current_step = step_wcc_out;
	  x_lib::do_stream< scc<F>, 
			    scc_edge_wrapper, 
			    scc_message_wrapper >
	    (graph_storage, i, edges_stream, messages_out_stream, NULL);
	  state_store(i);
	}
	PHASE = 1 - PHASE;
	scc_pcpu::wcc_iteration++;
      } while(!graph_storage->stream_empty(messages_out_stream));
    }

    template<typename F>
    void scc<F>::scc_do_elim()
    {
      const x_lib::configuration *config = graph_storage->get_config();
      scc_pcpu::current_step = step_elim_push;
      graph_storage->rewind_stream(edges_stream);
      for(unsigned long i=0;i<config->super_partitions;i++) {
	state_load(false, i);
	x_lib::do_stream< scc<F>, 
			  scc_edge_wrapper, 
			  scc_message_wrapper >
	  (graph_storage, i, edges_stream, messages_stream0, NULL);
	graph_storage->reset_stream(edges_stream, i);
	state_store(i);
      }
      graph_storage->rewind_stream(messages_stream0);
      scc_pcpu::current_step = step_elim_write;
      for(unsigned long i=0;i<config->super_partitions;i++) {
	state_load(false, i);
	x_lib::do_stream< scc<F>, 
			  scc_message_wrapper, 
			  scc_edge_wrapper >
	  (graph_storage, i, messages_stream0, edges_stream, NULL);
	graph_storage->reset_stream(messages_stream0, i);
	state_store(i);
      }
    }

    template<typename F>
    void scc<F>::operator() ()
    {
      unsigned long edges = pt.get<unsigned long>("graph.edges");
      scc_pcpu::current_step = step_split_edges;
      x_lib::do_stream< scc<F>, 
      			edge_wrapper<F>, 
			scc_edge_wrapper >
	(graph_storage, 0, init_stream, edges_stream, NULL); 
      graph_storage->close_stream(init_stream);
      // Main loop
      bool do_init = true;
      unsigned long total_elims;
      scc_pcpu::scc_iteration = 0;
      do {
	total_elims = 0;
	scc_do_cc(do_init);
	scc_pcpu::scc_iteration++;
	scc_do_elim();
	total_elims += scc_pcpu::elims_global;
	scc_pcpu::elims_global = 0;
	scc_do_cc(do_init);
	scc_pcpu::scc_iteration++;
	scc_do_elim();
	total_elims += scc_pcpu::elims_global;
	scc_pcpu::elims_global = 0;
	if(heartbeat) {
	  BOOST_LOG_TRIVIAL(info) << clock::timestamp() 
				  << " Heartbeat. "
				  << " edges_eliminated: "
				  << total_elims;
	}
	edges -= total_elims;
      } while(edges > 0);

      scc_pcpu::current_step = step_count_scc;
      for(unsigned long i=0;i<graph_storage->get_config()->super_partitions;i++) {
	state_load(do_init, i);
	do_init = false;
	x_lib::do_state_iter<scc<F> > (graph_storage, i);
      }
      scc_pcpu::current_step = step_terminate;
      x_lib::do_cpu< scc<F> >(graph_storage, ULONG_MAX);
      setup_time.start();
      graph_storage->terminate();
      setup_time.stop();
      wall_clock.stop();
      BOOST_LOG_TRIVIAL(info) << "ALGORITHM::SCC_COUNT " <<
	scc_pcpu::scc_global;
      setup_time.print("CORE::TIME::SETUP");
      wall_clock.print("CORE::TIME::WALL");
    }

    template<typename F>
    void scc<F>::partition_pre_callback(unsigned long superp, 
					 unsigned long partition,
					 per_processor_data *pcpu)
    {
      // Nothing
    }

    template<typename F>
    void scc<F>::partition_callback (x_lib::stream_callback_state *callback)
    {
      scc_pcpu *pcpu = static_cast<scc_pcpu *>(callback->cpu_state);
      switch(scc_pcpu::current_step) {
      
      case step_split_edges: {
	while(callback->bytes_in) {
	  vertex_t src, dst;
	  weight_t weight;
	  F::read_edge(callback->bufin, src, dst, weight);
	  if(callback->bytes_out + sizeof(scc_edge)  >
	     callback->bytes_out_max) {
	    break;
	  }
	  scc_edge *e = (scc_edge *)callback->bufout;
	  e->src    = src;
	  e->dst    = dst;
	  callback->bufout    += sizeof(scc_edge);
	  callback->bytes_out += sizeof(scc_edge);
	  callback->bytes_in -= F::split_size_bytes();
	  callback->bufin    += F::split_size_bytes(); 
	}
	break;
      }
      
      case step_wcc_in: {
	while(callback->bytes_in) {
	  scc_message *m = (scc_message *)callback->bufin;
	  scc_vertex *v = ((scc_vertex *)(callback->state)) +
	    x_lib::configuration::map_offset(m->dst);
	  if(v->component_root > m->component) {
	    v->component_root   = m->component;
	    v->update_iteration = scc_pcpu::wcc_iteration;
	  }
	  callback->bufin     += sizeof(scc_message);
	  callback->bytes_in  -= sizeof(scc_message);
	}
	break;
      }

      case step_wcc_out: {
	while(callback->bytes_in) {
	  scc_edge *e = (scc_edge *)callback->bufin;
	  scc_vertex *v = ((scc_vertex *)(callback->state)) +
	    x_lib::configuration::map_offset(e->src);
	  if(v->update_iteration == scc_pcpu::wcc_iteration) {
	    if((callback->bytes_out + sizeof(scc_message)) > 
	       callback->bytes_out_max) {
	      break;
	    }
	    scc_message *mout = (scc_message *)(callback->bufout);
	    mout->src       = e->src;
	    mout->dst       = e->dst;
	    mout->component = v->component_root; 
	    callback->bufout    += sizeof(scc_message);
	    callback->bytes_out += sizeof(scc_message);
	  }
	  callback->bufin     += sizeof(scc_edge);
	  callback->bytes_in  -= sizeof(scc_edge);
	}
	break;
      }
	
      case step_elim_push: {
	while(callback->bytes_in) {
	  scc_edge *e = (scc_edge *)callback->bufin;
	  scc_vertex *v = ((scc_vertex *)(callback->state)) +
	    x_lib::configuration::map_offset(e->src);
	  if((callback->bytes_out + sizeof(scc_message)) > 
	     callback->bytes_out_max) {
	    break;
	  }
	  scc_message *mout = (scc_message *)(callback->bufout);
	  mout->src       = e->src;
	  mout->dst       = e->dst;
	  mout->component = v->component_root; 
	  callback->bufout    += sizeof(scc_message);
	  callback->bytes_out += sizeof(scc_message);
	  callback->bufin     += sizeof(scc_edge);
	  callback->bytes_in  -= sizeof(scc_edge);
	}
	break;
      }

      case step_elim_write: {
	while(callback->bytes_in) {
	  scc_message *m = (scc_message *)callback->bufin;
	  scc_vertex *v = ((scc_vertex *)(callback->state)) +
	    x_lib::configuration::map_offset(m->dst);
	  if(m->component == v->component_root && 
	     v->component_root != v->prev_root) {
	    if((callback->bytes_out + sizeof(scc_edge)) > 
	       callback->bytes_out_max) {
	      break;
	    }
	    scc_edge *mout = (scc_edge *)(callback->bufout);
	    mout->src       = m->dst;
	    mout->dst       = m->src;
	    callback->bufout    += sizeof(scc_edge);
	    callback->bytes_out += sizeof(scc_edge);
	  }
	  else {
	    pcpu->elims_local++;
	  }
	  callback->bufin     += sizeof(scc_message);
	  callback->bytes_in  -= sizeof(scc_message);
	}
	break;
      }
      default:
	BOOST_LOG_TRIVIAL(fatal) << "Unknown operation in stream callback !";
	exit(-1);
      }
    }
  
    template<typename F>
    void scc<F>::partition_post_callback(unsigned long superp, 
					 unsigned long partition,
					 per_processor_data *pcpu)
    {
      // Nothing
    }

    template<typename F> scc_pcpu ** scc<F>::pcpu_array = NULL;
    unsigned long scc_pcpu::current_step;
    unsigned long scc_pcpu::wcc_iteration = 0;
    unsigned long scc_pcpu::scc_iteration = 0;
    unsigned long scc_pcpu::scc_global = 0;
    unsigned long scc_pcpu::elims_global = 0;
  }
}
#endif
