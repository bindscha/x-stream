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

#ifndef _REVERSE_
#define _REVERSE_
#include "../../core/x-lib.hpp"

// Note: Don't forget to truncate the output graph to (edges * F::split_size_bytes())

namespace algorithm {
  namespace reverse {

    template <typename F>
    class reverse {
      x_lib::streamIO<reverse>* graph_storage;
      unsigned long in_stream;
      unsigned long out_stream;

      rtc_clock wall_clock;
      rtc_clock setup_time;

    public:
      reverse();
      static void partition_pre_callback(unsigned long super_partition,
					 unsigned long partition,
					 per_processor_data* cpu_state)
      {}
      static void partition_callback(x_lib::stream_callback_state *state);
      static void partition_post_callback(unsigned long super_partition,
					  unsigned long partition,
					  per_processor_data *cpu_state)
      {}
      static void do_cpu_callback(per_processor_data *cpu_state)
      {}
      static void state_iter_callback(unsigned long superp, 
				      unsigned long partition,
				      unsigned long index,
				      unsigned char *vertex,
				      per_processor_data *cpu_state)
      {}
      void operator() ();

      static unsigned long max_streams()
      {
	return 2; 
      }
      static unsigned long max_buffers()
      {
	return 4;
      }

      static unsigned long vertex_state_bytes()
      {
	return 0;
      }
      static unsigned long vertex_stream_buffer_bytes()
      {
	return F::split_size_bytes();
      }

      static per_processor_data* create_per_processor_data(unsigned long processor_id)
      {
	return NULL;
      }
    };

    template <typename F>
    reverse<F>::reverse()
    {
      wall_clock.start();
      setup_time.start();
      graph_storage = new x_lib::streamIO<reverse>();
      if (graph_storage->get_config()->super_partitions > 1) {
	BOOST_LOG_TRIVIAL(fatal) << "ALGORITHM::REVERSE::WORKS_ONLY_WITH_1_SUPER_PARTITION";
        exit(-1);
      }
      std::string infile = pt.get<std::string>("graph.name");
      std::string outfile = infile + "-rev";
      in_stream  = graph_storage->open_stream((const char*)infile.c_str(), false,
					      vm["input_disk"].as<unsigned long>(),
					      F::split_size_bytes(), 1);
      out_stream = graph_storage->open_stream((const char*)outfile.c_str(), true,
					      vm["output_disk"].as<unsigned long>(),
					      F::split_size_bytes());
      setup_time.stop();
    }
  
    template <typename F> 
    struct edge_type_wrapper
    {
      static unsigned long item_size()
      {
	return F::split_size_bytes();
      }
      static unsigned long key(unsigned char* buffer)
      {
	return F::split_key(buffer, 0);
      }
    };

    template <typename F>
    void reverse<F>::operator()()
    {
      x_lib::do_stream<reverse<F>,
		       edge_type_wrapper<F>,
		       edge_type_wrapper<F> >
	(graph_storage, 0, in_stream, out_stream, NULL);
      graph_storage->rewind_stream(in_stream);
      graph_storage->rewind_stream(out_stream);

      setup_time.start();
      graph_storage->terminate();
      setup_time.stop();
      wall_clock.stop();
      
      setup_time.print("CORE::TIME::SETUP");
      wall_clock.print("CORE::TIME::WALL");
    }

    template <typename F>
    void reverse<F>::partition_callback(x_lib::stream_callback_state* callback)
    {
      while (callback->bytes_in) {
	if (callback->bytes_out + F::split_size_bytes() > callback->bytes_out_max) {
	  break;
	}
	vertex_t src, dst;
	weight_t value;
	F::read_edge(callback->bufin, src, dst, value);
	callback->bufin     += F::split_size_bytes();
	callback->bytes_in  -= F::split_size_bytes();
	F::write_edge(callback->bufout, dst, src, value);
	callback->bufout    += F::split_size_bytes();
	callback->bytes_out += F::split_size_bytes();
      }
    }
  }
}

#endif
