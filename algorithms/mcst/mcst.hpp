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

#ifndef _MCST_
#define _MCST_
#include<sys/time.h>
#include<sys/resource.h>
#include "../../core/x-lib.hpp"

// Minimum cost spanning trees using a combination of GHS
// and connected components

namespace algorithm {
  namespace mcst {
    const static unsigned long step_split_edges         = 0;
    const static unsigned long step_compute_min         = 1;
    const static unsigned long step_send_proposal       = 2;
    const static unsigned long step_compute_match       = 3;
    const static unsigned long step_propagate_root      = 4;
    const static unsigned long step_absorb_root         = 5;
    const static unsigned long step_write_tree_edges    = 6;
    const static unsigned long step_rewrite_unknown     = 7;
    const static unsigned long step_terminate           = 8;

    struct mcst_pcpu:public per_processor_data {
      unsigned long processor_id;
      unsigned long tree_edges_local;
      unsigned long unknown_edges_local;
      unsigned long cc_count_local;
      const unsigned long num_processors;
      /* begin work specs. */
      static unsigned long iteration;
      static unsigned long current_step;
      static unsigned long tree_edges_global;
      static unsigned long unknown_edges_global;
      static unsigned long cc_count_global;
      static vertex_t current_ccspread_iteration;
      static x_lib::filter *state_filter;
      /* end work specs. */
      /* stats */
      unsigned long proposals_sent;
      unsigned long proposals_received;
      bool reduce(per_processor_data **per_cpu_array,
		  unsigned long processors)
      {
	if(current_step == step_write_tree_edges) {
	  for(unsigned long i=0;i<num_processors;i++) {
	    mcst_pcpu *cpu = static_cast<mcst_pcpu *>(per_cpu_array[i]);
	    tree_edges_global       += cpu->tree_edges_local;
	    cc_count_global         -= cpu->cc_count_local;
	    unknown_edges_global    -= cpu->unknown_edges_local;
	    cpu->tree_edges_local    = 0;
	    cpu->unknown_edges_local = 0;
	    cpu->cc_count_local = 0;
	  }
	}
	else if(current_step == step_split_edges) {
	  for(unsigned long i=0;i<num_processors;i++) {
	    mcst_pcpu *cpu = static_cast<mcst_pcpu *>(per_cpu_array[i]);
	    unknown_edges_global += cpu->unknown_edges_local;
	    cpu->unknown_edges_local = 0;
	  }
	}
	return false; 
      }
      mcst_pcpu(unsigned long processor_id_in,
		unsigned long num_processors_in)
	:processor_id(processor_id_in),
	 tree_edges_local(0),
	 unknown_edges_local(0),
	 cc_count_local(0),
	 num_processors(num_processors_in),
	 proposals_sent(0),
	 proposals_received(0)
      {
      }
    } __attribute__((__aligned__(64)));

    struct __attribute__((__packed__)) mcst_vertex {
      vertex_t component_root;
      vertex_t proposed_match;
      weight_t proposition_cost;
      vertex_t match_iteration;
    };

    struct __attribute__((__packed__)) mcst_message {
      vertex_t src;
      vertex_t dst;
      weight_t weight;
    };

    template<typename F>
    class mcst {
      static mcst_pcpu ** pcpu_array;
      bool heartbeat;
      x_lib::streamIO<mcst> *graph_storage;
      unsigned long vertex_stream;
      unsigned long init_stream;
      unsigned long unknown_edges_stream;
      unsigned long tree_edges_stream;
      unsigned long messages_stream0;
      unsigned long messages_stream1;
      rtc_clock wall_clock;
      rtc_clock setup_time;

      // Edges are ordered by 
      // 1. weight
      // 2. minumum numbered endpoint
      // 3. maximum numbered endpoint
      // This imposes a total order on the edges in the graph
      static bool next_is_less(vertex_t src, vertex_t prev, vertex_t next,
			       weight_t prev_cost, weight_t next_cost)
      {
	unsigned long prev_min = MIN(src, prev);
	unsigned long prev_max = MAX(src, prev);
	unsigned long next_min = MIN(src, next);
	unsigned long next_max = MAX(src, next);
	if(next_cost < prev_cost) {
	  return true;
	}
	else if(next_cost > prev_cost) {
	  return false;
	}
	else {
	  if(next_min < prev_min) {
	    return true;
	  }
	  else if(next_min == prev_min &&
		  next_max < prev_max) {
	    return true;
	  }
	  else {
	    return false;
	  }
	}
      }

    public:
      mcst();
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
	return 5;
      }

      static unsigned long vertex_state_bytes()
      {
	return sizeof(struct mcst_vertex);
      }

      static unsigned long vertex_stream_buffer_bytes()
      {
	return sizeof(mcst_vertex) + 
	  MAX(F::split_size_bytes(), sizeof(mcst_message));
      }
    
      static void state_iter_callback(unsigned long superp, 
				      unsigned long partition,
				      unsigned long index,
				      unsigned char *vertex,
				      per_processor_data *cpu_state)
      {
	mcst_vertex *v = (mcst_vertex *)vertex;	
	if(mcst_pcpu::current_step == step_compute_min) {
	  v->proposed_match  = (vertex_t)ULONG_MAX;
	  v->match_iteration = (vertex_t)ULONG_MAX;
	  v->component_root  = 
	    x_lib::configuration::map_inverse(superp, partition, index);
	}
      }

      static per_processor_data * 
      create_per_processor_data(unsigned long processor_id)
      {
	return pcpu_array[processor_id];
      }
  
      static void do_cpu_callback(per_processor_data *cpu_state)
      {
	mcst_pcpu *pcpu = static_cast<mcst_pcpu *>(cpu_state);
	if(pcpu->current_step == step_terminate) {
	  BOOST_LOG_TRIVIAL(info) << "ALGORITHM::PROPOSALS_SENT " <<
	    pcpu->proposals_sent;
	  BOOST_LOG_TRIVIAL(info) << "ALGORITHM::PROPOSALS_RECEIVED " <<
	    pcpu->proposals_received;
	}
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
    mcst<F>::mcst()
    {
      wall_clock.start();
      setup_time.start();
      heartbeat = (vm.count("heartbeat") > 0);
      unsigned long num_processors = vm["processors"].as<unsigned long>();
      pcpu_array = new mcst_pcpu *[num_processors];
      for(unsigned long i=0;i<num_processors;i++) {
	pcpu_array[i] = new mcst_pcpu(i, num_processors);
      }
      graph_storage = new x_lib::streamIO<mcst>();
      mcst_pcpu::state_filter = new
	x_lib::filter(graph_storage->get_config()->cached_partitions,
		      num_processors);
      for(unsigned long i=0;i<graph_storage->get_config()->cached_partitions;i++) {
	mcst_pcpu::state_filter->q(i);
      }
      vertex_stream = 
	graph_storage->open_stream("vertices", true, 
				   vm["vertices_disk"].as<unsigned long>(),
				   graph_storage->get_config()->vertex_size);
      std::string efile = pt.get<std::string>("graph.name");
      init_stream = 
	graph_storage->open_stream((const char *)efile.c_str(), false,
				   vm["input_disk"].as<unsigned long>(),
				   F::split_size_bytes(), 1);
      unknown_edges_stream = 
	graph_storage->open_stream("unknown_edges", true,
				   vm["edges_disk"].as<unsigned long>(),
				   sizeof(mcst_message));
      messages_stream0 = 
	graph_storage->open_stream("messages", true, 
				   vm["updates0_disk"].as<unsigned long>(),
				   sizeof(mcst_message));
      messages_stream1 = 
	graph_storage->open_stream("messages", true, 
				   vm["updates1_disk"].as<unsigned long>(),
				   sizeof(mcst_message));
      tree_edges_stream = 
	graph_storage->open_stream("tree_edges", true,
				   vm["output_disk"].as<unsigned long>(),
				   sizeof(mcst_message));
      mcst_pcpu::cc_count_global = pt.get<unsigned long>("graph.vertices");
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

    struct mcst_message_wrapper
    {
      static unsigned long item_size()
      {
	return sizeof(mcst_message);
      }
    
      static unsigned long key(unsigned char *buffer)
      {
	return ((mcst_message *)buffer)->dst;
      }
    };

    template<typename F>
    void mcst<F>::operator() ()
    {
      const x_lib::configuration *config = graph_storage->get_config();
      // Do the initial edge split
      mcst_pcpu::current_step = step_split_edges;
      x_lib::do_stream< mcst<F>, 
			edge_wrapper<F>, 
			mcst_message_wrapper >
	(graph_storage, 0, init_stream, unknown_edges_stream, NULL); 
      graph_storage->close_stream(init_stream);
      graph_storage->rewind_stream(unknown_edges_stream);
      // Main loop
      while(mcst_pcpu::unknown_edges_global > 0) {
	graph_storage->rewind_stream(unknown_edges_stream);
	for(unsigned long i=0;i<config->super_partitions;i++) {
	  state_load(mcst_pcpu::iteration == 0, i);
	  mcst_pcpu::current_step = step_compute_min;
	  x_lib::do_state_iter<mcst<F> > (graph_storage, i);
	  x_lib::do_stream< mcst<F>, 
			    mcst_message_wrapper, 
			    mcst_message_wrapper >
	    (graph_storage, i, unknown_edges_stream,
	     ULONG_MAX, NULL);
	  mcst_pcpu::current_step = step_send_proposal;
	  x_lib::do_stream< mcst<F>, 
			    mcst_message_wrapper, 
			    mcst_message_wrapper >
	    (graph_storage, i, ULONG_MAX,
	     messages_stream0, mcst_pcpu::state_filter);
	  state_store(i);
	}
	
	mcst_pcpu::current_step = step_compute_match;
	graph_storage->rewind_stream(messages_stream0);
	for(unsigned long i=0;i<config->super_partitions;i++) {
	  state_load(false, i);
	  x_lib::do_stream< mcst<F>, 
			    mcst_message_wrapper, 
			    mcst_message_wrapper >
	    (graph_storage, i, messages_stream0, ULONG_MAX, NULL);
	  state_store(i);
	}

	mcst_pcpu::current_step = step_propagate_root;
	mcst_pcpu::current_ccspread_iteration = 0;
	graph_storage->rewind_stream(messages_stream0);
	for(unsigned long i=0;i<config->super_partitions;i++) {
	  state_load(false, i);
	  x_lib::do_stream< mcst<F>, 
			    mcst_message_wrapper, 
			    mcst_message_wrapper >
	    (graph_storage, i, messages_stream0, messages_stream1, NULL);
	  state_store(i);
	}

	mcst_pcpu::current_ccspread_iteration = 1;
	while(!graph_storage->stream_empty(messages_stream1)) {
	  mcst_pcpu::current_step = step_absorb_root;
	  graph_storage->rewind_stream(messages_stream1);
	  for(unsigned long i=0;i<config->super_partitions;i++) {
	    state_load(false, i);
	    x_lib::do_stream< mcst<F>, 
			      mcst_message_wrapper, 
			      mcst_message_wrapper >
	      (graph_storage, i, messages_stream1, ULONG_MAX, NULL);
	    graph_storage->reset_stream(messages_stream1, i);
	    state_store(i);
	  }
	  mcst_pcpu::current_step = step_propagate_root;
	  graph_storage->rewind_stream(messages_stream0);
	  for(unsigned long i=0;i<config->super_partitions;i++) {
	    state_load(false, i);
	    x_lib::do_stream< mcst<F>, 
			      mcst_message_wrapper, 
			      mcst_message_wrapper >
	      (graph_storage, i, messages_stream0, messages_stream1, NULL);
	    state_store(i);
	  }
	  mcst_pcpu::current_ccspread_iteration++;
	}
	
	mcst_pcpu::current_step = step_write_tree_edges;
	graph_storage->rewind_stream(messages_stream0);
	for(unsigned long i=0;i<config->super_partitions;i++) {
	  state_load(false, i);
	  x_lib::do_stream< mcst<F>, 
			    mcst_message_wrapper, 
			    mcst_message_wrapper >
	    (graph_storage, i, messages_stream0, tree_edges_stream, NULL);
	  graph_storage->reset_stream(messages_stream0, i);
	  state_store(i);
	}
	
	mcst_pcpu::current_step = step_rewrite_unknown;
	graph_storage->rewind_stream(unknown_edges_stream);
	for(unsigned long i=0;i<config->super_partitions;i++) {
	  state_load(false, i);
	  x_lib::do_stream< mcst<F>, 
			    mcst_message_wrapper, 
			    mcst_message_wrapper >
	    (graph_storage, i, unknown_edges_stream, messages_stream1, NULL);
	  graph_storage->reset_stream(unknown_edges_stream, i);
	  state_store(i);
	}
	graph_storage->rewind_stream(messages_stream1);
	for(unsigned long i=0;i<config->super_partitions;i++) {
	  state_load(false, i);
	  x_lib::do_stream< mcst<F>, 
			    mcst_message_wrapper, 
			    mcst_message_wrapper >
	    (graph_storage, i, messages_stream1, unknown_edges_stream, NULL);
	  graph_storage->reset_stream(messages_stream1, i);
	  state_store(i);
	}
	if(heartbeat) {
	  BOOST_LOG_TRIVIAL(info) << clock::timestamp() << " Completed phase " 
				  << mcst_pcpu::iteration
				  << " tree edges "
				  << mcst_pcpu::tree_edges_global
				  << " unknown edges "
				  << mcst_pcpu::unknown_edges_global
				  << " cc count "
				  << mcst_pcpu::cc_count_global;
	}
	mcst_pcpu::iteration++;
      }
      mcst_pcpu::current_step = step_terminate;
      x_lib::do_cpu< mcst<F> >(graph_storage, ULONG_MAX);
      setup_time.start();
      graph_storage->rewind_stream(tree_edges_stream);
      graph_storage->terminate();
      setup_time.stop();
      wall_clock.stop();
      BOOST_LOG_TRIVIAL(info) << "ALGORITHM::TREE_EDGES " <<
	mcst_pcpu::tree_edges_global;
      BOOST_LOG_TRIVIAL(info) << "ALGORITHM::CC_COUNT " <<
	mcst_pcpu::cc_count_global;
      BOOST_LOG_TRIVIAL(info) << "CORE::PHASES " << mcst_pcpu::iteration;
      setup_time.print("CORE::TIME::SETUP");
      wall_clock.print("CORE::TIME::WALL");
    }

    template<typename F>
    void mcst<F>::partition_pre_callback(unsigned long superp, 
					 unsigned long partition,
					 per_processor_data *pcpu)
    {
      // Nothing
    }

    template<typename F>
    void mcst<F>::partition_callback (x_lib::stream_callback_state *callback)
    {
      mcst_pcpu *pcpu = static_cast<mcst_pcpu *>(callback->cpu_state);
      switch(mcst_pcpu::current_step) {
      case step_split_edges: {
	// Eliminate self loops
	while(callback->bytes_in) {
	  vertex_t src, dst;
	  weight_t weight;
	  F::read_edge(callback->bufin, src, dst, weight);
	  if(src != dst) {
	    if(callback->bytes_out + sizeof(mcst_message)  >
	       callback->bytes_out_max) {
	      break;
	    }
	    mcst_message *m = (mcst_message *)callback->bufout;
	    m->src    = src;
	    m->dst    = dst;
	    m->weight = weight;
	    callback->bufout    += sizeof(mcst_message);
	    callback->bytes_out += sizeof(mcst_message);
	    pcpu->unknown_edges_local++;
	  }
	  callback->bytes_in -= F::split_size_bytes();
	  callback->bufin    += F::split_size_bytes(); 
	}
	break;
      }
      case step_compute_min: {
	while(callback->bytes_in) {
	  mcst_message *m = (mcst_message *)callback->bufin;
	  mcst_vertex *v = ((mcst_vertex *)(callback->state)) +
	    x_lib::configuration::map_offset(m->dst);
	  if(v->proposed_match == (vertex_t)ULONG_MAX || 
	     next_is_less(m->dst, v->proposed_match, m->src,
			  v->proposition_cost,
			  m->weight)) {
	    v->proposition_cost = m->weight;
	    v->proposed_match   = m->src;
	  }
	  callback->bufin     += sizeof(mcst_message);
	  callback->bytes_in  -= sizeof(mcst_message);
	}
	break;
      }
      case step_send_proposal: {
	while(callback->bytes_in) {
	  mcst_vertex *v = (mcst_vertex *)(callback->bufin);
	  if(v->proposed_match != (vertex_t)ULONG_MAX) {
	    if((callback->bytes_out + sizeof(mcst_message)) > 
	       callback->bytes_out_max) {
	      break;
	    }
	    mcst_message *mout = (mcst_message *)(callback->bufout);
	    mout->src = 
	      x_lib::configuration::map_inverse
	      (callback->superp, callback->partition_id, 
	       v - ((mcst_vertex *)callback->state));
	    mout->dst    = v->proposed_match;
	    mout->weight = v->proposition_cost;
	    pcpu->proposals_sent++;
	    callback->bufout    += sizeof(mcst_message);
	    callback->bytes_out += sizeof(mcst_message);
	  }
	  callback->bufin     += sizeof(mcst_vertex);
	  callback->bytes_in  -= sizeof(mcst_vertex);
	}
	break;
      }
      case step_compute_match: {
	while(callback->bytes_in) {
	  mcst_message *m = (mcst_message *)callback->bufin;
	  mcst_vertex  *v = ((mcst_vertex *)(callback->state))
	    + x_lib::configuration::map_offset(m->dst);
	  if(v->proposed_match == m->src) {
	    // smaller end becomes the root for the component
	    if(m->dst < m->src) {
	      v->match_iteration = 0; // trigger the root
	    }
	  }
	  pcpu->proposals_received++;
	  callback->bufin    += sizeof(struct mcst_message);
	  callback->bytes_in -= sizeof(struct mcst_message);
	}
	break;
      }

      case step_absorb_root: {
	while(callback->bytes_in) {
	  mcst_message *m = (mcst_message *)callback->bufin;
	  mcst_vertex  *v = ((mcst_vertex *)(callback->state))
	    + x_lib::configuration::map_offset(m->dst);
	  v->component_root   = m->src;
	  v->match_iteration  = mcst_pcpu::current_ccspread_iteration;
	  pcpu->cc_count_local++;
	  callback->bytes_in -= sizeof(mcst_message);
	  callback->bufin    += sizeof(mcst_message);
	}
	break;
      }
      
      case step_propagate_root: {
	while(callback->bytes_in) {
	  mcst_message *m = (mcst_message *)callback->bufin;
	  mcst_vertex  *v = ((mcst_vertex *)(callback->state))
	    + x_lib::configuration::map_offset(m->dst);
	  if(v->match_iteration == mcst_pcpu::current_ccspread_iteration
	     && m->src != v->component_root) {
	    if((callback->bytes_out + sizeof(mcst_message)) > 
	       callback->bytes_out_max) {
	      break; // empty buffer and try again
	    }
	    mcst_message *mout   =(mcst_message *)callback->bufout;
	    mout->src            = v->component_root;
	    mout->dst            = m->src;
	    mout->weight         = m->weight;
	    callback->bufout    += sizeof(mcst_message);
	    callback->bytes_out += sizeof(mcst_message);
	  }
	  callback->bytes_in -= sizeof(mcst_message);
	  callback->bufin    += sizeof(mcst_message);
	}
	break;
      }	

      case step_write_tree_edges: {
	while(callback->bytes_in) {
	  mcst_message *m = (mcst_message *)callback->bufin;
	  mcst_vertex  *v = ((mcst_vertex *)(callback->state))
	    + x_lib::configuration::map_offset(m->dst);
	  if(m->src == v->proposed_match) {
	    if((callback->bytes_out + sizeof(mcst_message)) > 
	       callback->bytes_out_max) {
	      break; // empty buffer and try again
	    }
	    mcst_message *mout =(mcst_message *)callback->bufout;
	    mout->src    = m->src;
	    mout->dst    = m->dst;
	    mout->weight = m->weight;
	    callback->bufout    += sizeof(mcst_message);
	    callback->bytes_out += sizeof(mcst_message);
	    if(m->src < m->dst) {
	      pcpu->tree_edges_local++;
	    }
	  }
	  else {
	    if((callback->bytes_out + 2*sizeof(mcst_message)) > 
	       callback->bytes_out_max) {
	      break; // empty buffer and try again
	    }
	    mcst_message *mout =(mcst_message *)callback->bufout;
	    mout->src    = m->src;
	    mout->dst    = m->dst;
	    mout->weight = m->weight;
	    callback->bufout    += sizeof(mcst_message);
	    callback->bytes_out += sizeof(mcst_message);
	    mout =(mcst_message *)callback->bufout;
	    mout->src    = m->dst;
	    mout->dst    = m->src;
	    mout->weight = m->weight;
	    callback->bufout    += sizeof(mcst_message);
	    callback->bytes_out += sizeof(mcst_message);
	    pcpu->tree_edges_local++;
	  }
	  callback->bytes_in -= sizeof(mcst_message);
	  callback->bufin    += sizeof(mcst_message);
	}
	break;
      }	
      
      case step_rewrite_unknown: {
	while(callback->bytes_in) {
	  mcst_message *m = (mcst_message *)callback->bufin;
	  mcst_vertex *v = ((mcst_vertex *)(callback->state)) +
	    x_lib::configuration::map_offset(m->dst);
	  if(m->src != v->component_root) {
	    if((callback->bytes_out + sizeof(mcst_message)) > 
	       callback->bytes_out_max) {
	      break;
	    }
	    mcst_message *mout = (mcst_message *)callback->bufout;
	    mout->src            = v->component_root;
	    mout->dst            = m->src;
	    callback->bufout    += sizeof(mcst_message);
	    callback->bytes_out += sizeof(mcst_message);
	  }
	  else { // drop the edge (has become internal)
	    pcpu->unknown_edges_local++;
	  }
	  callback->bufin    += sizeof(mcst_message);
	  callback->bytes_in -= sizeof(mcst_message);
	}
	break;
      }
      default:
	BOOST_LOG_TRIVIAL(fatal) << "Unknown operation in stream callback !";
	exit(-1);
      }
    }

    template<typename F>
    void mcst<F>::partition_post_callback(unsigned long superp, 
					  unsigned long partition,
					  per_processor_data *pcpu)
    {
      // Nothing
    }

    template<typename F> mcst_pcpu ** mcst<F>::pcpu_array = NULL;
    unsigned long mcst_pcpu::iteration = 0;
    vertex_t mcst_pcpu::current_ccspread_iteration = 0;
    unsigned long mcst_pcpu::current_step;
    unsigned long mcst_pcpu::tree_edges_global = 0;
    unsigned long mcst_pcpu::unknown_edges_global = 0;
    unsigned long mcst_pcpu::cc_count_global = 0;
    x_lib::filter* mcst_pcpu::state_filter = 0;
  }
}
#endif
