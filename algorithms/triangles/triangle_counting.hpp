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

#ifndef _TRIANGLE_COUNTING_
#define _TRIANGLE_COUNTING_
#include<sys/time.h>
#include<sys/resource.h>
#include "../../core/x-lib.hpp"
#include <cmath>
#include <boost/random.hpp>

// Triangle counting using the semi-streaming approach of 
// Becchetti et. al.

namespace algorithm {
  namespace triangle_counting {
    const unsigned long step_init_hstream = 0;
    const unsigned long step_init_zstream = 1;
    const unsigned long step_compute_hash = 2;
    const unsigned long step_send_hash    = 3;
    const unsigned long step_rcv_hash     = 4;
    const unsigned long step_zout         = 5;
    const unsigned long step_zin          = 6;
    const unsigned long step_rollup       = 7;
    
    struct triangle_counting_per_processor_data:public per_processor_data {
      unsigned long processor_id;
      float total_triangles_local;
      static unsigned long total_bsp_phases;
      static float total;
      /* begin work specs. */
      static unsigned long bsp_phase;
      static unsigned long current_step;
      /* end work specs. */
      boost::mt19937 generator;
      boost::uniform_int<unsigned long> distribution;
      static float total_triangles_global;
      unsigned long total_processors;

      bool reduce(per_processor_data **per_cpu_array,
		  unsigned long processors)
      {
	if(current_step == triangle_counting::step_rollup) {
	  for(unsigned long i=0;i<processors;i++) {
	    triangle_counting_per_processor_data *pcpu =
	      static_cast<triangle_counting_per_processor_data *>(per_cpu_array[i]);
	    total_triangles_global += pcpu->total_triangles_local;
	    pcpu->total_triangles_local   = 0.0f;
	  }
	}
	return false; 
      }
      triangle_counting_per_processor_data(unsigned long processor_id_in,
					   unsigned long total_processors_in)
	:processor_id(processor_id_in),
	 total_triangles_local(0.0),
	 generator(0xdeadbeef), // Deterministic seed
	 distribution(0, ULONG_MAX),
	 total_processors(total_processors_in)
      {
	for(unsigned long i=0;i<processor_id;i++) {
	  (void)distribution(generator); // skip
	}
      }
    } __attribute__((__aligned__(64)));

    struct __attribute__((__packed__)) tc_vertex {
      unsigned long hash_value;
      unsigned long min_adj_hash;
      float incident_triangles;
      unsigned long degree;
    };

    struct __attribute__((__packed__)) tc_hash {
      vertex_t other;
      unsigned long hash_value;
    };

    struct __attribute__((__packed__)) tc_z {
      vertex_t me;
      vertex_t other;
      unsigned long z;
      unsigned long calculated_min; // Or degree of other end
    };

    template<typename F>
    class triangle_counting {
      static unsigned long niters;
      static triangle_counting_per_processor_data ** pcpu_array;
      bool heartbeat; 
      x_lib::streamIO<triangle_counting> *graph_storage;
      unsigned long vertex_stream;
      unsigned long init_stream;
      unsigned long edge_stream;
      unsigned long hash_stream;
      unsigned long z0_stream;
      unsigned long z1_stream;
      rtc_clock wall_clock;
      rtc_clock setup_time;
    public:
      triangle_counting();
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
	return 6; // vertices, init, edges, z0, z1, hash
      }
      static unsigned long max_buffers()
      {
	return 5;
      }
      static unsigned long vertex_state_bytes()
      {
	return sizeof(struct tc_vertex);
      }
      static unsigned long vertex_stream_buffer_bytes()
      {
	return sizeof(tc_vertex)+ 
	  MAX(F::split_size_bytes(),
	      MAX(sizeof(tc_z),
		  sizeof(tc_hash)));
      }
      static void state_iter_callback(unsigned long superp, 
				      unsigned long partition,
				      unsigned long index,
				      unsigned char *vertex,
				      per_processor_data *cpu_state)
      {
	
	tc_vertex* v =  (struct tc_vertex*)vertex;
	triangle_counting_per_processor_data *pcpu = 
	  static_cast<triangle_counting_per_processor_data *>(cpu_state);
	if(triangle_counting_per_processor_data::current_step == step_compute_hash) {
	  if(triangle_counting_per_processor_data::bsp_phase == 0) {
	    v->degree = 0;
	    v->incident_triangles = 0.0f;
	  }
	  v->hash_value = pcpu->distribution(pcpu->generator);
	  unsigned long skip = pcpu->total_processors - 1;
	  while(skip--) {
	    (void)pcpu->distribution(pcpu->generator);
	  }
	  v->min_adj_hash = ULONG_MAX;
	}
	else if(triangle_counting_per_processor_data::current_step == step_rollup) {
	  v->incident_triangles /= 2.0f;
	  pcpu->total_triangles_local += v->incident_triangles;
	}
	else {
	  BOOST_LOG_TRIVIAL(fatal) << "Unexpected state iter callback !";
	  exit(-1);
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
  
    template<typename F>triangle_counting<F>::triangle_counting()
    {
      wall_clock.start();
      setup_time.start();
      heartbeat = (vm.count("heartbeat") > 0);
      unsigned long num_processors = vm["processors"].as<unsigned long>();
      niters =  (vm["triangle_counting::niters"].as<unsigned long>());
      pcpu_array = new triangle_counting_per_processor_data *[num_processors];
      for(unsigned long i=0;i<num_processors;i++) {
	pcpu_array[i] = new 
	  triangle_counting_per_processor_data(i, num_processors);
      }
      graph_storage = new x_lib::streamIO<triangle_counting>();
      triangle_counting_per_processor_data::bsp_phase = 0;
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
      hash_stream = 
	graph_storage->open_stream("hash", true, 
				   vm["updates0_disk"].as<unsigned long>(),
				   sizeof(struct tc_hash));
      z0_stream = 
	graph_storage->open_stream("z0", true, 
				   vm["updates0_disk"].as<unsigned long>(),
				   sizeof(struct tc_z));
      z1_stream = 
	graph_storage->open_stream("z1", true, 
				   vm["updates1_disk"].as<unsigned long>(),
				   sizeof(struct tc_z));
      setup_time.stop();

    }
    
    template<typename F> struct init_edge_wrapper
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

    struct tc_hash_wrapper
    {
      static unsigned long item_size()
      {
	return sizeof(struct tc_hash);
      }
      static unsigned long key(unsigned char *buffer)
      {
	return ((struct tc_hash *)buffer)->other;
      }
    };
    struct tc_z_wrapper
    {
      static unsigned long item_size()
      {
	return sizeof(struct tc_z);
      }
      static unsigned long key(unsigned char *buffer)
      {
	return ((struct tc_z *)buffer)->me;
      }
    };

#define SP_PRE(_i)							\
    do {								\
      if(graph_storage->get_config()->super_partitions > 1) {		\
	if(triangle_counting_per_processor_data::bsp_phase > 0) {	\
	  graph_storage->state_load(vertex_stream, _i);			\
	}								\
	graph_storage->state_prepare(_i);				\
      }									\
      else if(triangle_counting_per_processor_data::bsp_phase == 0) {	\
	graph_storage->state_prepare(_i);				\
      }									\
    } while(0);

#define SP_POST(_i)						\
    do {							\
      if(graph_storage->get_config()->super_partitions > 1) {	\
	graph_storage->state_store(vertex_stream, (_i));	\
      }								\
    } while(0)						       

#define SWAP_STREAMS(_i, _j)			\
    do {					\
      unsigned long _tmp;			\
      _tmp = _i;				\
      _i = _j;					\
      _j = _tmp;				\
    }while(0) 

    template<typename F>void triangle_counting<F>::operator() ()
    {
      // Setup
      triangle_counting_per_processor_data::current_step = step_init_hstream;	
      if(graph_storage->get_config()->super_partitions > 1) {
	x_lib::do_stream<triangle_counting<F>, 
			 init_edge_wrapper<F>,
			 init_edge_wrapper<F> >
	  (graph_storage, 0, init_stream, edge_stream, NULL);
	graph_storage->close_stream(init_stream);
      }
      graph_storage->rewind_stream(edge_stream);
      triangle_counting_per_processor_data::current_step = step_init_zstream;	
      x_lib::do_stream<triangle_counting<F>, 
		       init_edge_wrapper<F>,
		       tc_z_wrapper >
	(graph_storage, 0, edge_stream, z0_stream, NULL);
      // TC loop
      for(unsigned long iter=0;iter < niters;iter++) {
	graph_storage->rewind_stream(edge_stream);
	for(unsigned long i=0;i<graph_storage->get_config()->super_partitions;i++) {
	  SP_PRE(i);
	  triangle_counting_per_processor_data::current_step = step_compute_hash;	
	  x_lib::do_state_iter<triangle_counting<F> > (graph_storage, i);
	  triangle_counting_per_processor_data::current_step = step_send_hash;	
	  x_lib::do_stream<triangle_counting<F>, 
			   init_edge_wrapper<F>,
			   tc_hash_wrapper >
	    (graph_storage, i, edge_stream, hash_stream, NULL, true);
	  SP_POST(i);
	}
	graph_storage->rewind_stream(hash_stream);
	graph_storage->rewind_stream(z0_stream);
	for(unsigned long i=0;i<graph_storage->get_config()->super_partitions;i++) {
	  SP_PRE(i);
	  triangle_counting_per_processor_data::current_step = step_rcv_hash;	
	  x_lib::do_stream<triangle_counting<F>, 
			   tc_hash_wrapper,
			   tc_hash_wrapper >
	    (graph_storage, i, hash_stream, ULONG_MAX, NULL);
	  graph_storage->reset_stream(hash_stream, i);
	  triangle_counting_per_processor_data::current_step = step_zout;	
	  x_lib::do_stream<triangle_counting<F>, 
			   tc_z_wrapper,
			   tc_z_wrapper >
	    (graph_storage, i, z0_stream, z1_stream, NULL);
	  graph_storage->reset_stream(z0_stream, i);
	  SP_POST(i);
	}
	graph_storage->rewind_stream(z1_stream);
	for(unsigned long i=0;i<graph_storage->get_config()->super_partitions;i++) {
	  SP_PRE(i);
	  triangle_counting_per_processor_data::current_step = step_zin;	
	  x_lib::do_stream<triangle_counting<F>, 
			   tc_z_wrapper,
			   tc_z_wrapper >
	    (graph_storage, i, z1_stream, z0_stream, NULL);
	  graph_storage->reset_stream(z1_stream, i);
	  SP_POST(i);
	}
	if(heartbeat) {
	  BOOST_LOG_TRIVIAL(info) << clock::timestamp() << " Completed phase " <<triangle_counting_per_processor_data::bsp_phase;
	}
	triangle_counting_per_processor_data::bsp_phase++;
      }
      graph_storage->rewind_stream(z0_stream);
      triangle_counting_per_processor_data::current_step = step_rollup;	
      for(unsigned long i=0;i<graph_storage->get_config()->super_partitions;i++) {
	SP_PRE(i);
	x_lib::do_stream<triangle_counting<F>, 
			 tc_z_wrapper,
			 tc_z_wrapper >
	  (graph_storage, i, z0_stream, ULONG_MAX, NULL);
	graph_storage->reset_stream(z0_stream, i);
	x_lib::do_state_iter<triangle_counting<F> > (graph_storage, i);
	x_lib::do_cpu<triangle_counting>(graph_storage, i);
	SP_POST(i);
      }
      if(graph_storage->get_config()->super_partitions == 1) {
	graph_storage->state_store(vertex_stream, 0);
      }
      setup_time.start();
      graph_storage->terminate();
      setup_time.stop();
      wall_clock.stop();
      // Note div by 3.0 as each triangle is counted once at each incident
      // vertex
      BOOST_LOG_TRIVIAL(info) << "ALGORITHM::TRIANGLE_COUNTING::TRIANGLES "
			      <<
	triangle_counting_per_processor_data::total_triangles_global/3.0f;
      setup_time.print("CORE::TIME::SETUP");
      wall_clock.print("CORE::TIME::WALL");
    }

#undef SP_PRE
#undef SP_POST
#undef SWAP_STREAMS

    template<typename F>
    void triangle_counting<F>::partition_pre_callback(unsigned long superp, 
						      unsigned long partition,
						      per_processor_data *pcpu)
    {
      
    }

    template<typename F>
    void triangle_counting<F>::partition_callback(x_lib::stream_callback_state *callback)
    {
      if(callback->loopback) {
	while(callback->bytes_in) {
	  tc_hash *uin  = (tc_hash *)(callback->bufin);
	  tc_vertex* v  = ((tc_vertex *)callback->state) +
	    x_lib::configuration::map_offset(uin->other);
	  if(uin->hash_value < v->min_adj_hash) {
	    v->min_adj_hash = uin->hash_value;
	  }
	  callback->bufin     += sizeof(struct tc_hash);
	  callback->bytes_in  -= sizeof(struct tc_hash);
	}
	return;
      }
      switch(triangle_counting_per_processor_data::current_step) {
      case step_init_hstream:
	{
	  while(callback->bytes_in) {
	    if((callback->bytes_out + sizeof(F::split_size_bytes()))
	       >callback->bytes_out_max) {
	      break;
	    }
	    memcpy(callback->bufout, callback->bufin, F::split_size_bytes());
	    callback->bytes_out += F::split_size_bytes();
	    callback->bufout += F::split_size_bytes();
	    callback->bufin +=F::split_size_bytes();
	    callback->bytes_in -=F::split_size_bytes();
	  }
	  break;
	}
	
      case step_init_zstream:
	{
	  while(callback->bytes_in) {
	    if((callback->bytes_out + sizeof(tc_z)) >callback->bytes_out_max) {
	      break;
	    }
	    tc_z *z = (tc_z *)(callback->bufout);
	    vertex_t src, dst;
	    F::read_edge(callback->bufin, src, dst);
	    z->me    = src;
	    z->other = dst; 
	    z->z = 0;
	    z->calculated_min = 0xdeadbeefUL;
	    callback->bytes_out += sizeof(tc_z);
	    callback->bufout += sizeof(tc_z);
	    callback->bufin +=F::split_size_bytes();
	    callback->bytes_in -=F::split_size_bytes();
	  }
	  break;
	}

      case step_send_hash: 
	{
	  while(callback->bytes_in) {
	    if((callback->bytes_out + sizeof(tc_hash)) >callback->bytes_out_max) {
	      break;
	    }
	    vertex_t src, dst;
	    F::read_edge(callback->bufin, src, dst);
	    tc_hash *uout = (tc_hash *)(callback->bufout);
	    tc_vertex* v  = ((tc_vertex *)callback->state) +
	      x_lib::configuration::map_offset(src);
	    uout->hash_value = v->hash_value;
	    uout->other      = dst;
	    if(triangle_counting_per_processor_data::bsp_phase == 0){
	      v->degree++;
	    }
	    callback->bufin     += F::split_size_bytes();
	    callback->bytes_in  -= F::split_size_bytes();
	    callback->bufout    += sizeof(struct tc_hash);
	    callback->bytes_out += sizeof(struct tc_hash);
	  }
	  break;
	}
	
      case step_rcv_hash:
	{
	  while(callback->bytes_in) {
	    tc_hash *uin  = (tc_hash *)(callback->bufin);
	    tc_vertex* v  = ((tc_vertex *)callback->state) +
	      x_lib::configuration::map_offset(uin->other);
	    if(uin->hash_value < v->min_adj_hash) {
	      v->min_adj_hash = uin->hash_value;
	    }
	    callback->bufin     += sizeof(struct tc_hash);
	    callback->bytes_in  -= sizeof(struct tc_hash);
	  }
	  break;
	}
      case step_zout:
	{
	  while(callback->bytes_in) {
	    if((callback->bytes_out + sizeof(tc_z)) > callback->bytes_out_max) {
	      break;
	    }
	    tc_z* zin = (tc_z *)(callback->bufin);
	    tc_z* zout = (tc_z *)(callback->bufout);
	    tc_vertex* v =  ((tc_vertex *)(callback->state)) +
	      x_lib::configuration::map_offset(zin->me);
	    zout->me             = zin->other;
	    zout->other          = zin->me;
	    zout->z              = zin->z;
	    zout->calculated_min = v->min_adj_hash; 
	    callback->bufin     += sizeof(struct tc_z);
	    callback->bufout    += sizeof(struct tc_z);
	    callback->bytes_in  -= sizeof(struct tc_z);
	    callback->bytes_out += sizeof(struct tc_z);
	  }
	  break;
	}
	
      case step_zin:
	{
	  while(callback->bytes_in) {
	    if((callback->bytes_out + sizeof(tc_z)) > callback->bytes_out_max) {
	      break;
	    }
	    tc_z* zin  = (tc_z *)(callback->bufin);
	    tc_z* zout = (tc_z *)(callback->bufout);
	    tc_vertex* v    =  ((tc_vertex *)(callback->state)) +
	      x_lib::configuration::map_offset(zin->me);
	    zout->me             = zin->other;
	    zout->other          = zin->me;
	    if(zin->calculated_min == v->min_adj_hash) {
	      zout->z = zin->z + 1;
	    }
	    else {
	      zout->z = zin->z;
	    }
	    zout->calculated_min = v->degree;
	    callback->bufin     += sizeof(struct tc_z);
	    callback->bufout    += sizeof(struct tc_z);
	    callback->bytes_in  -= sizeof(struct tc_z);
	    callback->bytes_out += sizeof(struct tc_z);
	  }
	  break;
	}

      case step_rollup:
	{
	  while(callback->bytes_in) {
	    tc_z* zin  = (tc_z *)(callback->bufin);
	    tc_vertex* v    =  ((tc_vertex *)(callback->state)) +
	      x_lib::configuration::map_offset(zin->me);
	    v->incident_triangles = v->incident_triangles +
	      ((zin->z/(zin->z + (float)niters))*
	       (zin->calculated_min + v->degree));
	    callback->bufin     += sizeof(struct tc_z);
	    callback->bytes_in  -= sizeof(struct tc_z);
	  }
	  break;
	}
      default:
	exit(-1);
      }
    }

    template<typename F>
    void triangle_counting<F>::partition_post_callback
    (unsigned long superp, 
     unsigned long partition,
     per_processor_data *pcpu)
    {
    }

    template<typename F> triangle_counting_per_processor_data **
    triangle_counting<F>::pcpu_array = NULL;
    template<typename F> unsigned long triangle_counting<F>::niters = 0;
    unsigned long triangle_counting_per_processor_data::bsp_phase = 0;
    unsigned long triangle_counting_per_processor_data::current_step;
    float triangle_counting_per_processor_data::total_triangles_global = 0.0;
  }
}
#endif

