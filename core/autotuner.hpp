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

#ifndef _AUTOTUNER_
#define _AUTOTUNER_
#include "../utils/boost_log_wrapper.h"
#include "../utils/memory_utils.h"
#include "../utils/options_utils.h"
#include "../utils/desc_utils.h"
#define OPTIMUM_STREAM_UNIT (16*1024*1024)
#define RAM_ADJUST (0.9)
#define DISK_PAGE_SIZE (4*1024)
#define MIN(a, b) ((b) < (a) ? (b):(a))
#define MAX(a, b) ((a) < (b) ? (b):(a))

namespace x_lib {
  struct configuration
  {
    /* Independent variables */
    unsigned long processors;
    unsigned long vertices;
    unsigned long edges;
    unsigned long vertex_size;
    unsigned long vertex_footprint;
    unsigned long llc_size;
    unsigned long llc_line_size;
    unsigned long available_ram;
    unsigned long stream_unit; // Set by constant
    unsigned long max_streams;
    unsigned long max_buffers;
    unsigned long num_ioqs;
    unsigned long memory_buffer_object_size;
    unsigned long disk_stream_object_size;
    unsigned long ioq_object_size;

    /* Dependent variables */
    static unsigned long cached_partitions;
    unsigned long fanout;
    static unsigned long super_partitions;
    unsigned long buffer_size;
    unsigned long vertex_state_buffer_size;

    /* Mapping */
    static unsigned long partition_shift;
    static unsigned long super_partition_shift;
    void setup_mapping()
    {
      partition_shift = 0;
      unsigned long temp = cached_partitions - 1;
      while(temp) {
	partition_shift++;
	temp = temp >> 1;
      }
      super_partition_shift = 0;
      temp = super_partitions - 1;
      while(temp) {
	super_partition_shift++;
	temp = temp >> 1;
      }
    }

    unsigned long state_bufsize(unsigned long superp)
    {
      return vertex_state_buffer_size;
    }

    unsigned long max_state_bufsize()
    {
      return vertex_state_buffer_size;
    }

    unsigned long state_count(unsigned long superp, 
			      unsigned long partition)
    {
      unsigned long base = (partition << super_partition_shift) | superp;
      unsigned long count = (vertices - 1) & 
	~((1UL << (partition_shift + super_partition_shift)) - 1);
      BOOST_ASSERT_MSG((base & count) == 0, "Error in state count calculation");
      if((count | base) < vertices) {
	count = count >> (partition_shift + super_partition_shift);
      }
      else {
	count = (count >> (partition_shift + super_partition_shift)) - 1;
      }
      count++;
      return count;
    }

    unsigned long calculate_ram_budget()
    {
      unsigned long ram_budget = 0;
      // Loaded vertices
      vertex_state_buffer_size = (vertices/super_partitions)*vertex_size;
      // Align to DISK PAGE size
      vertex_state_buffer_size = vertex_state_buffer_size + DISK_PAGE_SIZE - 1;
      vertex_state_buffer_size =
	(vertex_state_buffer_size/DISK_PAGE_SIZE)*DISK_PAGE_SIZE;
      ram_budget += vertex_state_buffer_size;
      // Indexes [+1 for the aux]
      unsigned long aux_cnt = ((vm.count("qsort") > 0) ? 0:1);
      ram_budget += (max_buffers + aux_cnt)*cached_partitions*sizeof(unsigned long);
      ram_budget += (max_buffers + aux_cnt)*processors*sizeof(unsigned long);
      // Filters
      ram_budget += max_buffers*cached_partitions*sizeof(unsigned long);
      ram_budget += max_buffers*processors*sizeof(unsigned long);
      // Bounce buffers for the io threads
      ram_budget += num_ioqs*stream_unit;
      // IOQ object size
      ram_budget += num_ioqs*ioq_object_size;
      // Streams
      ram_budget += max_streams*disk_stream_object_size;
      // Buffers occupy the rest
      ram_budget += max_buffers*memory_buffer_object_size;
      buffer_size = (available_ram - ram_budget)/(max_buffers + 1); 
      buffer_size -= DISK_PAGE_SIZE;
      return ram_budget;
    }

  public:
    void dump_config()
    {
      BOOST_LOG_TRIVIAL(info) << "CORE::CONFIG::PROCESSORS " << processors;
      BOOST_LOG_TRIVIAL(info) << "CORE::CONFIG::PHYSICAL_MEMORY " <<
	available_ram;
      BOOST_LOG_TRIVIAL(info) << "CORE::CONFIG::VERTICES " << vertices;
      BOOST_LOG_TRIVIAL(info) << "CORE::CONFIG::VERTEX_SIZE " << vertex_size;
      BOOST_LOG_TRIVIAL(info) << "CORE::CONFIG::VERTEX_BUFFER " <<
	vertex_state_buffer_size;
      BOOST_LOG_TRIVIAL(info) << "CORE::CONFIG::EDGES " << edges;
      BOOST_LOG_TRIVIAL(info) << "CORE::CONFIG::PARTITIONS " <<
	cached_partitions;
      BOOST_LOG_TRIVIAL(info) << "CORE::CONFIG::FANOUT " << fanout;
      BOOST_LOG_TRIVIAL(info) << "CORE::CONFIG::SUPER_PARTITIONS " << 
	super_partitions;
      BOOST_LOG_TRIVIAL(info) << "CORE::CONFIG::STREAM_UNIT " << 
	stream_unit;
      BOOST_LOG_TRIVIAL(info) << "CORE::CONFIG::BUFFER_SIZE " <<
       buffer_size;
      BOOST_LOG_TRIVIAL(info) << "CORE::CONFIG::IO_NUMIOQS " <<
	num_ioqs;
      BOOST_LOG_TRIVIAL(info) << "CORE::CONFIG::MAX_BUFFERS " <<
	max_buffers;
      BOOST_LOG_TRIVIAL(info) << "CORE::CONFIG::MAX_STREAMS " <<
	max_streams;
    }

    static unsigned long map_offset(unsigned long key)
    {
      return key >> (super_partition_shift + partition_shift);
    }
    
    static unsigned long map_cached_partition(unsigned long key)
    {
      return (key >> super_partition_shift) & (cached_partitions - 1);
    }
    
    static unsigned long map_super_partition(unsigned long key)
    {
      return key & (super_partitions - 1);
    }
    
    static unsigned long map_inverse(unsigned long super_partition,
				     unsigned long partition,
				     unsigned long offset)
    {
      return 
	(((offset << partition_shift) | partition) << super_partition_shift)
	| super_partition;
    }

    void set_cache(unsigned long partitions)
    {
      cached_partitions = partitions/super_partitions;
      fanout = cached_partitions;
      unsigned long cache_lines = llc_size/llc_line_size;
      while(fanout > cache_lines) {
	fanout = fanout/2;
      }
    }
    
    // Returns true iff autotuning successful
    bool autotune()
    {
      // Cache <-> MM tuning 
      unsigned long partitions;
      unsigned long vertices_per_partition = llc_size/vertex_footprint;
      partitions = 1;
      while((vertices_per_partition*partitions) < vertices) {
	partitions = partitions*2;
      }
      if(partitions < processors) {
	partitions = processors;
      }
      // MM <-> Disk tuning
      unsigned long ram_budget;
      stream_unit = OPTIMUM_STREAM_UNIT;
      while(stream_unit > DISK_PAGE_SIZE) {
	super_partitions = 1;
	while(super_partitions <= partitions) {
	  set_cache(partitions);
	  ram_budget = calculate_ram_budget();
	  if(ram_budget <= RAM_ADJUST*available_ram &&
	     buffer_size >= stream_unit*super_partitions) {
	    setup_mapping();
	    return true;
	  }
	  super_partitions = super_partitions*2;
	}
	stream_unit = stream_unit/2;
      }
      return false;
    }
    
    void manual()
    {
      unsigned long total_partitions = vm["partitions"].as<unsigned long>();
      super_partitions = vm["super_partitions"].as<unsigned long>();
      cached_partitions = total_partitions/super_partitions;
      fanout = vm["fanout"].as<unsigned long>();
      stream_unit = OPTIMUM_STREAM_UNIT;
      unsigned long ram_budget = calculate_ram_budget();
      if(ram_budget > RAM_ADJUST*available_ram) {
	BOOST_LOG_TRIVIAL(fatal) << "Too little physical memory, try autotune.";
	exit(-1);
      }
      setup_mapping();
    }

    void init()
    {
      processors = vm["processors"].as<unsigned long>();
      vertices   = pt.get<unsigned long>("graph.vertices");
      edges      = pt.get<unsigned long>("graph.edges");
      llc_size   = vm["cpu_cache_size"].as<unsigned long>();
      llc_line_size = vm["cpu_line_size"].as<unsigned long>();
      available_ram = vm["physical_memory"].as<unsigned long>();
      try {
	num_ioqs = pt.get<unsigned long>("disk.count"); 
      }
      catch (...) {
	BOOST_LOG_TRIVIAL(warn) << 
	  "Disk count not specified, assuming it to be 1";
	num_ioqs = 1; 
      }
      if(vm["force_buffers"].as<unsigned long>() > 0) {
	max_buffers = vm["force_buffers"].as<unsigned long>();
      }
    }
  };

  class map_cached_partition_wrap
  {
  public:
    static unsigned long map(unsigned long key) 
    {
      return (key >> configuration::super_partition_shift) & 
	(configuration::cached_partitions - 1);
    }
  };

  class map_super_partition_wrap
  {
  public:
    static unsigned long map(unsigned long key)
    {
      return key & (configuration::super_partitions - 1);
    }
  };
}
#endif
