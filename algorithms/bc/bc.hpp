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

#ifndef _BC_
#define _BC_
#include "../../core/x-lib.hpp"
#include <stdlib.h>  
#include <cmath> 

// Betweeness centrality using the observations of 
// Brandes et. al.


namespace algorithm {
  namespace bc {
    const weight_t fp_precision = 1.0e-12;
    struct bc_pcpu:public per_processor_data {
      unsigned long processor_id;
      static weight_t max_centrality;
      static vertex_t max_centrality_vertex;
      weight_t max_centrality_local;
      vertex_t max_centrality_vertex_local;
      /* begin work specs. */
      static unsigned long current_step;
      /* end work specs. */
      bool reduce(per_processor_data **per_cpu_array,
		  unsigned long processors)
      {
	for(unsigned long i=0;i<processors;i++) {
	  bc_pcpu *pcpu = static_cast<bc_pcpu *>(per_cpu_array[i]);
	  if(pcpu->max_centrality_local > max_centrality) {
	    max_centrality = pcpu->max_centrality_local;
	    max_centrality_vertex = pcpu->max_centrality_vertex_local;
	  }
	}
	return false; 
      }
      bc_pcpu()
	:max_centrality_local(0.0),
	 max_centrality_vertex_local((vertex_t)-1)
      {
      }

    } __attribute__((__aligned__(64)));

    struct __attribute__((__packed__)) bc_vertex {
      union {
	weight_t min_path;
	weight_t bc;
      };
      unsigned long min_paths;
      union {
	weight_t delta_bc;
	unsigned long delta_paths;
      };
      bool active;
    };

    struct __attribute__((__packed__)) bc_fwd_update {
      vertex_t dst;
      weight_t path_length;
      unsigned long delta_paths;
    };
    
    struct __attribute__((__packed__)) bc_rev_update {
      vertex_t dst;
      weight_t delta_bc;
    };

    struct __attribute__((__packed__)) bc_edge {
      vertex_t src;
      vertex_t dst;
      weight_t weight;
    };

    template<typename F>
    class bc {
      const static unsigned long step_init_fwd            = 0;
      const static unsigned long step_send_path           = 1;
      const static unsigned long step_update_path         = 2;
      const static unsigned long step_gen_rev_out         = 3;
      const static unsigned long step_gen_rev_in          = 4;
      const static unsigned long step_init_rev            = 5;
      const static unsigned long step_send_bc             = 6;
      const static unsigned long step_update_bc           = 7;
      const static unsigned long step_calc_max            = 8;

      static bc_pcpu ** pcpu_array;
      static unsigned long source;
      bool heartbeat;
      x_lib::streamIO<bc> *graph_storage;
      unsigned long vertex_stream;
      unsigned long init_stream;
      unsigned long tmp_stream;
      unsigned long edges_stream;
      unsigned long updates_stream;
      unsigned long rev_updates_stream;
      rtc_clock wall_clock;
      rtc_clock setup_time;
    public:
      bc();
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
	return 6;
      }
      static unsigned long max_buffers()
      {
	return 4;
      }

      static unsigned long vertex_state_bytes()
      {
	return sizeof(struct bc_vertex);
      }

      static unsigned long vertex_stream_buffer_bytes()
      {
	return sizeof(bc_vertex) + sizeof(bc_edge);
      }
    
      static void state_iter_callback(unsigned long superp, 
				      unsigned long partition,
				      unsigned long index,
				      unsigned char *vertex,
				      per_processor_data *cpu_state)
      {
	bc_vertex *v = (struct bc_vertex *)vertex;
	bc_pcpu *pcpu = static_cast<bc_pcpu *>(cpu_state);
	if(bc_pcpu::current_step == step_init_fwd) {
	  v->min_path        = (weight_t)std::numeric_limits<double>::max();
	  v->min_paths       = 0;
	  if(x_lib::configuration::map_inverse
	     (superp, partition, index) == source) {
	    v->min_paths       = 1;
	    v->min_path        = 0;
	    v->delta_paths     = 1;
	    v->active          = true;
	  }
	  else {
	    v->active          = false;
	  }
	}
	else if(bc_pcpu::current_step == step_init_rev) {
	  v->bc                = 0;
	  if(v->min_paths > 0) {
	    v->delta_bc        = 1.0/v->min_paths;
	    v->active          = true;
	  }
	  else {
	    v->delta_bc     = 0;
	    v->active       = false;
	  }
	}
	else if(bc_pcpu::current_step == step_update_path) {
	  v->delta_paths = 0;
	  v->active      = false;
	}
	else if(bc_pcpu::current_step == step_update_bc) {
	  v->delta_bc    = 0;
	  v->active      = false; 
	}
	else if(bc_pcpu::current_step == step_calc_max) {
	  unsigned long vindex = x_lib::configuration::map_inverse
	    (superp, partition, index);
	  if((v->bc > pcpu->max_centrality_local) &&
	     vindex != source) {
	    pcpu->max_centrality_local        = v->bc;
	    pcpu->max_centrality_vertex_local = vindex;
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
	
      }
    };
  
    template<typename F>
    bc<F>::bc()
    {
      wall_clock.start();
      setup_time.start();
      heartbeat = (vm.count("heartbeat") > 0);
      source    = vm["bc::source"].as<unsigned long>(); 
      unsigned long num_processors = vm["processors"].as<unsigned long>();
      pcpu_array = new bc_pcpu *[num_processors];
      for(unsigned long i=0;i<num_processors;i++) {
	pcpu_array[i] = new bc_pcpu();
      }
      graph_storage = new x_lib::streamIO<bc>();
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
				   sizeof(struct bc_edge));
      tmp_stream = 
	graph_storage->open_stream("tmp", true,
				   vm["input_disk"].as<unsigned long>(),
				   sizeof(struct bc_edge));

      updates_stream = 
	graph_storage->open_stream("updates", true, 
				   vm["updates0_disk"].as<unsigned long>(),
				   sizeof(struct bc_fwd_update));
      rev_updates_stream =
	graph_storage->open_stream("updates2", true, 
				   vm["updates0_disk"].as<unsigned long>(),
				   sizeof(struct bc_rev_update));
      setup_time.stop();
    }
    
    template<typename F> 
    struct init_edge_wrapper
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

    struct bc_fwd_update_wrapper
    {
      static unsigned long item_size()
      {
	return sizeof(struct bc_fwd_update);
      }
      static unsigned long key(unsigned char *buffer)
      {
	return ((struct bc_fwd_update *)buffer)->dst;
      }
    };
    
    struct bc_rev_update_wrapper
    {
      static unsigned long item_size()
      {
	return sizeof(struct bc_rev_update);
      }
      static unsigned long key(unsigned char *buffer)
      {
	return ((struct bc_rev_update *)buffer)->dst;
      }
    };

    struct bc_edge_wrapper
    {
      static unsigned long item_size()
      {
	return sizeof(struct bc_edge);
      }
      static unsigned long key(unsigned char *buffer)
      {
	return ((struct bc_edge *)buffer)->src;
      }
    };

    template<typename F>
    void bc<F>::operator() ()
    {
      // Prepare for forward pass
      bc_pcpu::current_step = step_init_fwd;
      for(unsigned long i=0;i<graph_storage->get_config()->super_partitions;i++) {
	graph_storage->state_prepare(i);
	x_lib::do_state_iter<bc<F> > (graph_storage, i);
	if(graph_storage->get_config()->super_partitions > 1) {
	  graph_storage->state_store(vertex_stream, i);
	}
      }
      x_lib::do_stream< bc<F>, 
			init_edge_wrapper<F>, 
			bc_edge_wrapper >
	(graph_storage, 0, init_stream, edges_stream, NULL); 
      graph_storage->close_stream(init_stream);

      // Forward pass
      do {
	bc_pcpu::current_step = step_send_path;
	graph_storage->rewind_stream(edges_stream);
	for(unsigned long i=0;i<graph_storage->get_config()->super_partitions;i++) {
	  if(graph_storage->get_config()->super_partitions > 1) {
	    graph_storage->state_load(vertex_stream, i);
	    graph_storage->state_prepare(i);
	  }
	  x_lib::do_stream< bc<F>, 
			    bc_edge_wrapper, 
			    bc_fwd_update_wrapper >
	    (graph_storage, i, edges_stream, updates_stream, NULL); 
	  if(graph_storage->get_config()->super_partitions > 1) {
	    graph_storage->state_store(vertex_stream, i);
	  }
	}
	bc_pcpu::current_step = step_update_path;
	if(graph_storage->stream_empty(updates_stream)) {
	  break;
	}
	graph_storage->rewind_stream(updates_stream);
	for(unsigned long i=0;i<graph_storage->get_config()->super_partitions;i++) {
	  if(graph_storage->get_config()->super_partitions > 1) {
	    graph_storage->state_load(vertex_stream, i);
	    graph_storage->state_prepare(i);
	  }
	  x_lib::do_state_iter<bc<F> > (graph_storage, i);
	  x_lib::do_stream< bc<F>, 
			    bc_fwd_update_wrapper, 
			    bc_fwd_update_wrapper >
	    (graph_storage, i, updates_stream, ULONG_MAX, NULL); 
	  graph_storage->reset_stream(updates_stream, i);
	  if(graph_storage->get_config()->super_partitions > 1) {
	    graph_storage->state_store(vertex_stream, i);
	  }
	}
	if(heartbeat) {
	  BOOST_LOG_TRIVIAL(info) << clock::timestamp() << " Completed one fwd pass ";
	}
      }while(1);

      // Produce reverse shortest path edges
      bc_pcpu::current_step = step_gen_rev_out;
      graph_storage->rewind_stream(edges_stream);
      for(unsigned long i=0;i<graph_storage->get_config()->super_partitions;i++) {
	if(graph_storage->get_config()->super_partitions > 1) {
	  graph_storage->state_load(vertex_stream, i);
	  graph_storage->state_prepare(i);
	}
	x_lib::do_stream< bc<F>, 
			  bc_edge_wrapper, 
			  bc_edge_wrapper >
	  (graph_storage, i, edges_stream, tmp_stream, NULL); 
	graph_storage->reset_stream(edges_stream, i);
	if(graph_storage->get_config()->super_partitions > 1) {
	  graph_storage->state_store(vertex_stream, i);
	}
      }
      
      bc_pcpu::current_step = step_gen_rev_in;
      graph_storage->rewind_stream(tmp_stream);
      for(unsigned long i=0;i<graph_storage->get_config()->super_partitions;i++) {
	if(graph_storage->get_config()->super_partitions > 1) {
	  graph_storage->state_load(vertex_stream, i);
	  graph_storage->state_prepare(i);
	}
	x_lib::do_stream< bc<F>, 
			  bc_edge_wrapper, 
			  bc_edge_wrapper >
	  (graph_storage, i, tmp_stream, edges_stream, NULL); 
	graph_storage->reset_stream(tmp_stream, i);
	if(graph_storage->get_config()->super_partitions > 1) {
	  graph_storage->state_store(vertex_stream, i);
	}
      }


      // Prepare for reverse pass
      bc_pcpu::current_step = step_init_rev;
      for(unsigned long i=0;i<graph_storage->get_config()->super_partitions;i++) {
	if(graph_storage->get_config()->super_partitions > 1) {
	  graph_storage->state_load(vertex_stream, i);
	  graph_storage->state_prepare(i);
	}
	x_lib::do_state_iter<bc<F> > (graph_storage, i);
	if(graph_storage->get_config()->super_partitions > 1) {
	  graph_storage->state_store(vertex_stream, i);
	}
      }
      // Reverse pass
      do {
	bc_pcpu::current_step = step_send_bc;
	graph_storage->rewind_stream(edges_stream);
	for(unsigned long i=0;i<graph_storage->get_config()->super_partitions;i++) {
	  if(graph_storage->get_config()->super_partitions > 1) {
	    graph_storage->state_load(vertex_stream, i);
	    graph_storage->state_prepare(i);
	  }
	  x_lib::do_stream< bc<F>, 
			    bc_edge_wrapper, 
			    bc_rev_update_wrapper >
	    (graph_storage, i, edges_stream, rev_updates_stream, NULL); 
	  if(graph_storage->get_config()->super_partitions > 1) {
	    graph_storage->state_store(vertex_stream, i);
	  }
	}
	bc_pcpu::current_step = step_update_bc;
	if(graph_storage->stream_empty(rev_updates_stream)) {
	  break;
	}
	graph_storage->rewind_stream(rev_updates_stream);
	for(unsigned long i=0;i<graph_storage->get_config()->super_partitions;i++) {
	  if(graph_storage->get_config()->super_partitions > 1) {
	    graph_storage->state_load(vertex_stream, i);
	    graph_storage->state_prepare(i);
	  }
	  x_lib::do_state_iter<bc<F> > (graph_storage, i);
	  x_lib::do_stream< bc<F>, 
			    bc_rev_update_wrapper, 
			    bc_rev_update_wrapper >
	    (graph_storage, i, rev_updates_stream, ULONG_MAX, NULL); 
	  graph_storage->reset_stream(rev_updates_stream, i);
	  if(graph_storage->get_config()->super_partitions > 1) {
	    graph_storage->state_store(vertex_stream, i);
	  }
	}
	if(heartbeat) {
	  BOOST_LOG_TRIVIAL(info) << clock::timestamp() << " Completed one rev pass";
	}
      }while(1);

      bc_pcpu::current_step = step_calc_max;
      for(unsigned long i=0;i<graph_storage->get_config()->super_partitions;i++) {
	if(graph_storage->get_config()->super_partitions > 1) {
	  graph_storage->state_load(vertex_stream, i);
	  graph_storage->state_prepare(i);
	}
	x_lib::do_state_iter<bc<F> > (graph_storage, i);
	if(graph_storage->get_config()->super_partitions > 1) {
	  graph_storage->state_store(vertex_stream, i);
	}
      }
      x_lib::do_cpu< bc<F> >(graph_storage, ULONG_MAX);
      setup_time.start();
      graph_storage->terminate();
      setup_time.stop();
      wall_clock.stop();
      BOOST_LOG_TRIVIAL(info) << "ALGORITHM::BC::MAX_CENTRALITY " 
			      <<  bc_pcpu::max_centrality;
      BOOST_LOG_TRIVIAL(info) << "ALGORITHM::BC::MAX_CENTRALITY_VERTEX " 
			      <<  bc_pcpu::max_centrality_vertex;
      setup_time.print("CORE::TIME::SETUP");
      wall_clock.print("CORE::TIME::WALL");
    }
    
    template<typename F>
    void bc<F>::partition_pre_callback(unsigned long superp, 
				       unsigned long partition,
				       per_processor_data *pcpu)
    {
      // Nothing
    }

    template<typename F> 
    void bc<F>::partition_callback(x_lib::stream_callback_state *callback)
    {
      switch(bc_pcpu::current_step) {
      case step_init_fwd: {
	while(callback->bytes_in) {
	  vertex_t src, dst;
	  weight_t weight;
	  F::read_edge(callback->bufin, src, dst, weight);
	  if(callback->bytes_out + sizeof(bc_edge)  >
	     callback->bytes_out_max) {
	    break;
	  }
	  bc_edge *e = (bc_edge *)callback->bufout;
	  e->src    = src;
	  e->dst    = dst;
	  e->weight = weight;
	  callback->bufout    += sizeof(bc_edge);
	  callback->bytes_out += sizeof(bc_edge);
	  callback->bytes_in  -= F::split_size_bytes();
	  callback->bufin     += F::split_size_bytes(); 
	}	      
	break;
      }
      case step_send_path: {
	while(callback->bytes_in) {
	  bc_edge * e = (bc_edge *)callback->bufin;
	  bc_vertex *v = ((bc_vertex *)(callback->state))+
	    x_lib::configuration::map_offset(e->src);
	  if(v->active) {
	    if((callback->bytes_out + sizeof(struct bc_edge)) > callback->bytes_out_max) {
	      break;
	    }
	    bc_fwd_update *u = (bc_fwd_update *)callback->bufout;
	    u->dst               = e->dst;
	    u->path_length       = v->min_path + e->weight;
	    u->delta_paths       = v->delta_paths;
	    callback->bufout    += sizeof(bc_fwd_update);
	    callback->bytes_out += sizeof(bc_fwd_update);
	  }
	  callback->bytes_in    -= sizeof(bc_edge);
	  callback->bufin       += sizeof(bc_edge);
        }
	break;
      }
      case step_update_path: {
	while(callback->bytes_in) {
	  bc_fwd_update * u = (bc_fwd_update *)callback->bufin;
	  bc_vertex *v = ((bc_vertex *)(callback->state))+
	    x_lib::configuration::map_offset(u->dst);
	  if(u->path_length < v->min_path) {
	    v->min_path     = u->path_length;
	    v->min_paths    = u->delta_paths;
	    v->delta_paths  = u->delta_paths;
	    v->active       = true;
	  }
	  else if((u->path_length - v->min_path) < fp_precision) {
	    v->min_paths   += u->delta_paths;
	    v->delta_paths += u->delta_paths;
	    v->active       = true;
	  }
	  callback->bytes_in -= sizeof(bc_fwd_update);
	  callback->bufin    += sizeof(bc_fwd_update);
	}
	break;
      }	
      case step_gen_rev_out: {
	while(callback->bytes_in) {
	  if((callback->bytes_out + sizeof(bc_edge)) > 
	     callback->bytes_out_max) {
	    break;
	  }
	  bc_edge *ein  = (bc_edge *)callback->bufin;
	  bc_vertex *v = ((bc_vertex *)(callback->state))+
	    x_lib::configuration::map_offset(ein->src);
	  bc_edge *eout = (bc_edge *)callback->bufout;
	  eout->src     = ein->dst;
	  eout->dst     = ein->src;
	  eout->weight  = v->min_path + ein->weight;
	  callback->bufout    += sizeof(bc_edge);
	  callback->bytes_out += sizeof(bc_edge);
	  callback->bytes_in  -= sizeof(bc_edge);
	  callback->bufin     += sizeof(bc_edge);
	}
	break;
      }
      case step_gen_rev_in: {
	while(callback->bytes_in) {
	  bc_edge *ein  = (bc_edge *)callback->bufin;
	  bc_vertex *v = ((bc_vertex *)(callback->state))+
	    x_lib::configuration::map_offset(ein->src);
	  if((ein->weight - v->min_path) < fp_precision) {
	    if((callback->bytes_out + sizeof(bc_edge)) > 
	       callback->bytes_out_max) {
	      break;
	    }
	    bc_edge *eout = (bc_edge *)callback->bufout;
	    eout->src     = ein->src;
	    eout->dst     = ein->dst; // Reverse SSSP-DAG edge
	    eout->weight  = ein->weight;
	    callback->bufout    += sizeof(bc_edge);
	    callback->bytes_out += sizeof(bc_edge);
	  }
	  callback->bytes_in -= sizeof(bc_edge);
	  callback->bufin    += sizeof(bc_edge);
	}
	break;
      }

      case step_send_bc: {
	while(callback->bytes_in) {
	  bc_edge * e = (bc_edge *)callback->bufin;
	  bc_vertex *v = ((bc_vertex *)(callback->state))+
	    x_lib::configuration::map_offset(e->src);
	  if(v->active) {
	    if((callback->bytes_out + sizeof(struct bc_edge)) >
	       callback->bytes_out_max) {
	      break;
	    }
	    bc_rev_update *u = (bc_rev_update *)callback->bufout;
	    u->dst               = e->dst;
	    u->delta_bc          = v->delta_bc;
	    callback->bufout    += sizeof(bc_rev_update);
	    callback->bytes_out += sizeof(bc_rev_update);
	  }
	  callback->bytes_in    -= sizeof(bc_edge);
	  callback->bufin       += sizeof(bc_edge);
        }
	break;
      }
      case step_update_bc: {
	while(callback->bytes_in) {
	  bc_rev_update * u = (bc_rev_update *)callback->bufin;
	  bc_vertex *v = ((bc_vertex *)(callback->state))+
	    x_lib::configuration::map_offset(u->dst);
	  v->delta_bc        += u->delta_bc;
	  v->bc              += v->min_paths*u->delta_bc;
	  v->active           = true;
	  callback->bytes_in -= sizeof(bc_rev_update);
	  callback->bufin    += sizeof(bc_rev_update);
	}
	break;
      }	
      default:
	BOOST_LOG_TRIVIAL(fatal) << "Unknown operation in partition callback";
	break;
      }
    }
    
    template<typename F>
    void bc<F>::partition_post_callback(unsigned long superp, 
					unsigned long partition,
					per_processor_data *pcpu)
    {

    }

    template<typename F>
    bc_pcpu ** bc<F>::pcpu_array = NULL;
    template<typename F>
    unsigned long bc<F>::source; 
    unsigned long bc_pcpu::current_step;
    weight_t bc_pcpu::max_centrality = 0;
    vertex_t bc_pcpu::max_centrality_vertex = (vertex_t)ULONG_MAX;
  }
}
#endif
