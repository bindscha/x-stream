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

#ifndef _BP_
#define _BP_
#include<sys/time.h>
#include<sys/resource.h>
#include "../../core/x-lib.hpp"

// Belief propagation using the technique of 
// Kung et. al. 


//Set what you wish here.
#define BP_STATES 2

namespace algorithm {
  namespace belief_prop {
    namespace standard {
      struct bp_pcpu:public per_processor_data {
	unsigned long processor_id;
	// Stats
	unsigned long update_bytes_out;
	unsigned long update_bytes_in;
	unsigned long edge_bytes_streamed;
	unsigned long partitions_processed;
	// 

	/* begin work specs. */
	static unsigned long bsp_phase;
	static unsigned long current_step;
	/* end work specs. */
	bool reduce(per_processor_data **per_cpu_array,
		    unsigned long processors)
	{
	  return false; 
	}
      } __attribute__((__aligned__(64)));

      struct __attribute__((__packed__)) belief_propagation_vertex {
	weight_t potentials[BP_STATES];
	weight_t product[BP_STATES];
      };

      struct __attribute__((__packed__)) belief_propagation_edge {
	unsigned long src;
	unsigned long dst;
	// Note: Use up when src < dst
	weight_t potential_up[BP_STATES][BP_STATES]; 
	weight_t potential_down[BP_STATES][BP_STATES];
	weight_t belief[BP_STATES];
      };

      template<typename F>
      class belief_propagation {
	const static unsigned long step_gen_edge_potential  = 0;
	const static unsigned long step_absorb              = 1;
	const static unsigned long step_emit                = 2;
	const static unsigned long step_terminate           = 3;
	unsigned long niters;
	static bp_pcpu ** pcpu_array;
	bool heartbeat;
	x_lib::streamIO<belief_propagation> *graph_storage;
	unsigned long vertex_stream;
	unsigned long updates0_stream;
	unsigned long updates1_stream;
	unsigned long init_stream;
	rtc_clock wall_clock;
	rtc_clock setup_time;
	rtc_clock edge_potential_generation_time;

      public:
	belief_propagation();
	static void partition_pre_callback(unsigned long super_partition,
					   unsigned long partition,
					   per_processor_data* cpu_state);
	static void generate_initial_belief
	(unsigned char *edge, 
	 struct belief_propagation_edge* be_fwd,
	 struct belief_propagation_edge* be_rev);
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
	  return sizeof(struct belief_propagation_vertex);
	}

	static unsigned long vertex_stream_buffer_bytes()
	{
	  return sizeof(belief_propagation_vertex) + 
	    sizeof(belief_propagation_edge);
	}
    
	static void state_iter_callback(unsigned long superp, 
					unsigned long partition,
					unsigned long index,
					unsigned char *vertex,
					per_processor_data *cpu_state)
	{
	  belief_propagation_vertex *v = 
	    (struct belief_propagation_vertex *)vertex;
	  for(unsigned long i=0; i< BP_STATES;i++) {
	    v->product[i]  = 1.0;
	  }
	  if(bp_pcpu::bsp_phase == 0) {
	    for(unsigned long i=0; i< BP_STATES;i++) {
	      v->potentials[i] = 0.5;
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
	  bp_pcpu *cpu = static_cast<bp_pcpu *>(cpu_state);
	  if(bp_pcpu::current_step == step_terminate) {
	    BOOST_LOG_TRIVIAL(info)<< "CORE::PARTITIONS_PROCESSED " << cpu->partitions_processed;
	    BOOST_LOG_TRIVIAL(info)<< "CORE::BYTES::EDGES_STREAMED " << cpu->edge_bytes_streamed;
	    BOOST_LOG_TRIVIAL(info)<< "CORE::BYTES::UPDATES_OUT " << cpu->update_bytes_out;
	    BOOST_LOG_TRIVIAL(info)<< "CORE::BYTES::UPDATES_IN " << cpu->update_bytes_in;
	  }
	}
      };
  
      template<typename F>
      belief_propagation<F>::belief_propagation()
      {
	wall_clock.start();
	setup_time.start();
	heartbeat = (vm.count("heartbeat") > 0);
	niters    = vm["belief_propagation::niters"].as<unsigned long>();
	unsigned long num_processors = vm["processors"].as<unsigned long>();
	pcpu_array = new bp_pcpu *[num_processors];
	for(unsigned long i=0;i<num_processors;i++) {
	  pcpu_array[i] = new bp_pcpu();
	  pcpu_array[i]->processor_id = i;
	  pcpu_array[i]->update_bytes_in = 0;
	  pcpu_array[i]->update_bytes_out = 0;
	  pcpu_array[i]->edge_bytes_streamed = 0;
	}
	graph_storage = new x_lib::streamIO<belief_propagation>();
	bp_pcpu::bsp_phase = 0;
	vertex_stream = 
	  graph_storage->open_stream("vertices", true, 
				     vm["vertices_disk"].as<unsigned long>(),
				     graph_storage->get_config()->vertex_size);
	std::string efile = pt.get<std::string>("graph.name");
	init_stream = 
	  graph_storage->open_stream((const char *)efile.c_str(), false,
				     vm["input_disk"].as<unsigned long>(),
				     F::split_size_bytes(), 1);
	updates0_stream = 
	  graph_storage->open_stream("updates0", true, 
				     vm["updates0_disk"].as<unsigned long>(),
				     sizeof(struct belief_propagation_edge));
	updates1_stream = 
	  graph_storage->open_stream("updates1", true, 
				     vm["updates1_disk"].as<unsigned long>(),
				     sizeof(struct belief_propagation_edge));
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

      struct belief_edge_wrapper
      {
	static unsigned long item_size()
	{
	  return sizeof(struct belief_propagation_edge);
	}
	static unsigned long key(unsigned char *buffer)
	{
	  return ((struct belief_propagation_edge *)buffer)->dst;
	}
      };

      template<typename F>
      void belief_propagation<F>::operator() ()
      {
	// Generate edge potentials
	edge_potential_generation_time.start();
	bp_pcpu::current_step = step_gen_edge_potential;
	x_lib::do_stream< belief_propagation<F>, 
			  init_edge_wrapper<F>, 
			  belief_edge_wrapper >
	  (graph_storage, 0, init_stream, updates0_stream, NULL);
	graph_storage->close_stream(init_stream);
	edge_potential_generation_time.stop();
	// Supersteps
	unsigned long PHASE = 0;
	unsigned long iters = 0;
	graph_storage->rewind_stream(updates0_stream);
	while((iters++) < niters) {
	  unsigned long updates_in_stream;
	  unsigned long updates_out_stream; 
	  updates_in_stream  = (PHASE == 0 ? updates1_stream:updates0_stream);
	  updates_out_stream = (PHASE == 0 ? updates0_stream:updates1_stream);
	  for(unsigned long i=0;i<graph_storage->get_config()->super_partitions;i++) {
	    if(graph_storage->get_config()->super_partitions > 1) {
	      if(bp_pcpu::bsp_phase > 0) {
		graph_storage->state_load(vertex_stream, i);
	      }
	      graph_storage->state_prepare(i);
	    }
	    else if(bp_pcpu::bsp_phase == 0) {
	      graph_storage->state_prepare(0);
	    }
	    x_lib::do_state_iter<belief_propagation<F> > (graph_storage, i);
	    bp_pcpu::current_step = step_absorb;
	    x_lib::do_stream<belief_propagation<F>, 
			     belief_edge_wrapper,
			     belief_edge_wrapper >
	      (graph_storage, i, updates_in_stream, ULONG_MAX, NULL);
	    // need to replay the stream to determine updates
	    graph_storage->rewind_stream(updates_in_stream);
	    bp_pcpu::current_step = step_emit;
	    x_lib::do_stream<belief_propagation<F>, 
			     belief_edge_wrapper,
			     belief_edge_wrapper >
	      (graph_storage, i, updates_in_stream, updates_out_stream, NULL);
	    graph_storage->reset_stream(updates_in_stream, i);
	    if(graph_storage->get_config()->super_partitions > 1) {
	      graph_storage->state_store(vertex_stream, i);
	    }
	  }
	  graph_storage->rewind_stream(updates_out_stream);
	  if(graph_storage->get_config()->super_partitions > 1) {
	    graph_storage->rewind_stream(vertex_stream);
	  }
	  PHASE = 1 - PHASE;
	  bp_pcpu::bsp_phase++;
	  if(heartbeat) {
	    BOOST_LOG_TRIVIAL(info) << clock::timestamp() << " Completed phase " <<
	      bp_pcpu::bsp_phase;
	  }
	}
	if(graph_storage->get_config()->super_partitions == 1) {
	  graph_storage->state_store(vertex_stream, 0);
	}
	bp_pcpu::current_step = step_terminate;
	x_lib::do_cpu<belief_propagation<F> >(graph_storage, ULONG_MAX);
	setup_time.start();
	graph_storage->terminate();
	setup_time.stop();
	wall_clock.stop();
	BOOST_LOG_TRIVIAL(info) << "CORE::PHASES " << bp_pcpu::bsp_phase;
	setup_time.print("CORE::TIME::SETUP");
	edge_potential_generation_time.print("CORE::TIME::EDGE_POT_GEN");
	wall_clock.print("CORE::TIME::WALL");
      }

      template<typename F>
      void belief_propagation<F>::partition_pre_callback(unsigned long superp, 
							 unsigned long partition,
							 per_processor_data *pcpu)
      {
	// Nothing
      }

      template<typename F>
      void belief_propagation<F>::generate_initial_belief
      (unsigned char *edge, 
       struct belief_propagation_edge* be_fwd,
       struct belief_propagation_edge *be_rev)
      {
	vertex_t src, dst;
	weight_t weight;
	F::read_edge(edge, src, dst, weight);
	be_fwd->src = src;
	be_fwd->dst = dst;
	be_rev->src = dst;
	be_rev->dst = src;
	for(unsigned long i=0;i<BP_STATES;i++) {
	  for(unsigned long j=0;j<BP_STATES;j++) {
	    be_fwd->potential_up[i][j] = weight;
	    be_fwd->potential_down[i][j] = 1.0 - weight;
	    be_rev->potential_up[i][j] = weight;
	    be_rev->potential_down[i][j] = 1.0 - weight;
	  }
	  be_fwd->belief[i] = weight;
	  be_rev->belief[i] = weight;
	}
      }
  
      template<typename F>
      void belief_propagation<F>::partition_callback
      (x_lib::stream_callback_state *callback)
      {
	bp_pcpu *pcpu = static_cast<bp_pcpu *>(callback->cpu_state);
	switch(bp_pcpu::current_step) {
	case step_gen_edge_potential: {
	  unsigned long tmp = callback->bytes_in;
	  while(callback->bytes_in) {
	    if((callback->bytes_out + 2*sizeof(struct belief_propagation_edge)) >
	       callback->bytes_out_max) {
	      break;
	    }
	    belief_propagation_edge *e_fwd = 
	      (belief_propagation_edge *)
	      (callback->bufout + callback->bytes_out);
	    belief_propagation_edge *e_rev = 
	      (belief_propagation_edge *)
	      (callback->bufout + callback->bytes_out + 
	       sizeof(struct belief_propagation_edge));
	    generate_initial_belief(callback->bufin, e_fwd, e_rev);
	    callback->bytes_out += 2*sizeof(struct belief_propagation_edge);
	    callback->bufin += F::split_size_bytes();
	    callback->bytes_in -= F::split_size_bytes();
	  }
	  pcpu->edge_bytes_streamed += (tmp - callback->bytes_in); 
	  break;
	}
	case step_absorb: {
	  pcpu->update_bytes_in += callback->bytes_in;
	  while(callback->bytes_in) {
	    belief_propagation_edge *u = 
	      (belief_propagation_edge *)(callback->bufin);
	    belief_propagation_vertex *v = 
	      ((belief_propagation_vertex *)(callback->state))
	      + x_lib::configuration::map_offset(u->dst);
	    for(unsigned long i=0;i<BP_STATES;i++) {
	      v->product[i]  *= u->belief[i];
	    }
	    callback->bufin += sizeof(struct belief_propagation_edge);
	    callback->bytes_in -= sizeof(struct belief_propagation_edge);
	  }
	  break;
	}
	case step_emit: {
	  while(callback->bytes_in) {
	    if((callback->bytes_out + sizeof(belief_propagation_edge)) > 
	       callback->bytes_out_max) {
	      break;
	    }
	    BOOST_ASSERT_MSG(callback->bytes_out < callback->bytes_out_max,
			     "Update buffer overflow !!!");
	    belief_propagation_edge *ein = 
	      (belief_propagation_edge *)(callback->bufin);
	    belief_propagation_edge *eout = 
	      (belief_propagation_edge *)(callback->bufout);
	    belief_propagation_vertex *v = 
	      ((belief_propagation_vertex *)(callback->state)) +
	      x_lib::configuration::map_offset(ein->dst);
	    eout->src = ein->dst;
	    eout->dst = ein->src;
	    weight_t sum = 0.0;
	    for(unsigned long i=0;i<BP_STATES;i++) {
	      eout->belief[i] = 0;
	      for(unsigned long j=0;j<BP_STATES;j++) {
		eout->potential_up[i][j]   = ein->potential_up[i][j];
		eout->potential_down[i][j] = ein->potential_down[i][j]; 
		if(eout->src < eout->dst) {
		  eout->belief[i] +=
		    v->potentials[j]*eout->potential_up[i][j]*v->product[j]/ein->belief[j];
		}
		else {
		  eout->belief[i] +=
		    v->potentials[j]*eout->potential_down[i][j]*v->product[j]/ein->belief[j];
		}
	      }
	      sum += eout->belief[i];
	    }
	    if(sum > 0.0) {
	      for(unsigned long i=0;i<BP_STATES;i++) {
		eout->belief[i]/=sum; // Normalize
	      }
	    }
	    callback->bufin     += sizeof(struct belief_propagation_edge);
	    callback->bufout    += sizeof(struct belief_propagation_edge);
	    callback->bytes_in  -= sizeof(struct belief_propagation_edge);
	    callback->bytes_out += sizeof(struct belief_propagation_edge);
	  }
	  pcpu->update_bytes_out    += callback->bytes_out;
	  break;
	}
	default:
	  BOOST_LOG_TRIVIAL(fatal) << "Unknown operation in stream callback !";
	  exit(-1);
	}
      }

      template<typename F>
      void belief_propagation<F>::partition_post_callback(unsigned long superp, 
							  unsigned long partition,
							  per_processor_data *pcpu)
      {
	bp_pcpu *pcpu_actual = static_cast<bp_pcpu *>(pcpu);
	pcpu_actual->partitions_processed++;
      }

      template<typename F>
      bp_pcpu ** belief_propagation<F>::pcpu_array = NULL;
      unsigned long bp_pcpu::bsp_phase = 0;
      unsigned long bp_pcpu::current_step;
    }
  }
}
#endif
