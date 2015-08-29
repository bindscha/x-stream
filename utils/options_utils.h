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

//! Utilities for options
#ifndef _OPTIONS_UTILS_
#define _OPTIONS_UTILS_

#include<boost/program_options.hpp>
#include "boost_log_wrapper.h"
#include<iostream>

extern boost::program_options::options_description desc;
extern boost::program_options::variables_map vm;

static void setup_options(int argc, const char* argv[])
{
  desc.add_options()
    ( "help,h", "Produce help message")
    ( "processors,p",  boost::program_options::value<unsigned long>()->default_value(1), 
      "Number of processors")
    ("benchmark,b",
     boost::program_options::value<std::string>()->default_value("bfs"),
     "bfs, bfs_async, bfs_forest, degree_cnt, conductance, range_check, reverse, \
     spmv, sssp, sssp_forest, cc, cc_online, als, als_async, checkbip, mis, \
     pagerank, pagerank_ddf, belief_propagation, belief_propagation_graphchi, \
     mcst, hyperanf, scc, triangle_counting, bc")
    ("graph,g", boost::program_options::value<std::string>()->required(), 
     "Name of the graph/matrix")
    ("autotune,a", "Autotune partition count")
    ("cpu_cache_size", 
     boost::program_options::value<unsigned long>()->default_value(2097152),
     "Per-processor cache size")
    ("cpu_line_size", 
     boost::program_options::value<unsigned long>()->default_value(64),
     "Per-processor cacheline size")
    ("physical_memory",
     boost::program_options::value<unsigned long>()->default_value(1073741824),
     "Physical memory available")
    ("partitions,k", boost::program_options::value<unsigned long>()->default_value(32),
     "Number of partitions")
    ("super_partitions", boost::program_options::value<unsigned long>()->default_value(1),
     "Number of partitions")
    ("fanout,f", boost::program_options::value<unsigned long>()->default_value(32),  
     "Fanout for the tree splitter (power of two)")
    ("qsort", "Use quicksort for partitioning")
    ("heartbeat", "Emit a heartbeat message at the end of every phase")
    ("destroy_init", "Trim the initial edge list file to release space")
    ("measure_scatter_gather", "Measure sub-steps of scatter gather")
    ("compressed_io", "Do compressed io")
    ("blocked_memory",
     boost::program_options::value<unsigned long>()->default_value(0),
     "Physical memory to block before starting the test");
    
  // Scatter Gather driver options
  desc.add_options()
    ("vertices_disk",
     boost::program_options::value<unsigned long>()->default_value(0),
     "Disk containing vertex data files")
    ("input_disk",
     boost::program_options::value<unsigned long>()->default_value(0),
     "Disk containing input edge list")
    ("edges_disk",
     boost::program_options::value<unsigned long>()->default_value(0),
     "Disk containing edge stream")
    ("updates0_disk",
     boost::program_options::value<unsigned long>()->default_value(0),
     "Disk containing first update stream")
    ("updates1_disk",
     boost::program_options::value<unsigned long>()->default_value(0),
     "Disk containing second update stream")
    ("output_disk",
     boost::program_options::value<unsigned long>()->default_value(0),
     "Disk containing output stream");
  // Autotuner force options
  desc.add_options()
    ("force_buffers",
     boost::program_options::value<unsigned long>()->default_value(0),
     "force buffer count")
    ("no_dio", "Don't do direct I/O");
;
  // Add algorithm specific options here !!!!!!!!!!!!!!!
  // bfs
  desc.add_options()
    ("bfs::root", 
     boost::program_options::value<unsigned long>()->default_value(0),
     "root vertex id for bfs and bfs_async");
  // sssp
  desc.add_options()
    ("sssp::source",
     boost::program_options::value<unsigned long>()->default_value(0),
     "source vertex id for sssp");
  // cc online
  desc.add_options()
    ("cc_online::shm_key",
     boost::program_options::value<unsigned long>()->default_value(0xf22UL),
     "shm key for ingest buffer segment");
  // als
  desc.add_options()
    ("als::niters",
     boost::program_options::value<unsigned long>()->default_value(5),
     "number of iterations for als and als_async");
  // pagerank
  desc.add_options()
    ("pagerank::niters",
     boost::program_options::value<unsigned long>()->default_value(5),
     "number of iterations for pagerank and pagerank_ddf");
  // belief propagation
  desc.add_options()
    ("belief_propagation::niters",
     boost::program_options::value<unsigned long>()->default_value(5),
     "number of iterations for bp and bp_graphchi");
  // Hyperanf
  desc.add_options()
    ("hyperanf::maxiters",
     boost::program_options::value<unsigned long>()->default_value(100),
     "maximum number of iterations to run for")
    ("hyperanf::epsilon",
     boost::program_options::value<float>()->default_value(0.001f),
     "relative change in neighbourhood function to stop at")
    ("hyperanf::rsd",
     boost::program_options::value<float>()->default_value(0.10f),
     "tolerated relative standard deviation in hyperanf result")
    ("hyperanf::run_to_completion", "run hyperanf to completion");
  // Triangle counting
  desc.add_options()
    ("triangle_counting::niters",
     boost::program_options::value<unsigned long>()->default_value(100),
     "number of iterations for triangle counting");
  // bc
  desc.add_options()
    ("bc::source", 
     boost::program_options::value<unsigned long>()->default_value(0),
     "source vertex for bc");

  try {
    boost::program_options::store(boost::program_options::parse_command_line(argc,
									     argv,
									     desc),
				  vm);
    boost::program_options::notify(vm);
  }
  catch (boost::program_options::error &e) {
    if(vm.count("help") || argc ==1) {
      std::cerr << desc << "\n";
    }
    std::cerr << "Error:" << e.what() << std::endl;
    std::cerr << "Try: " << argv[0] << " --help" << std::endl;
    exit(-1);
  }
}

static void check_pow_2(unsigned long value, const char * fail_string)
{
  if((value & (value - 1)) != 0) {
    BOOST_LOG_TRIVIAL(fatal) << fail_string;
    exit(-1);
  }
}

#endif
