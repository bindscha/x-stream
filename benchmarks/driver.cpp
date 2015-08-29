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

//! Main driver for benchmark programs
#include "../utils/boost_log_wrapper.h"
// Core
#ifdef PYTHON_SUPPORT
  #include "../core/sg_driver_python.hpp"
#else
  #include "../core/sg_driver.hpp"
  #include "../core/sg_driver_async.hpp"
#endif
// Types
#include "../format/type_1.hpp"
#include "../format/type_2.hpp"
// X-Stream algorithms
#include "../algorithms/bfs/bfs.hpp"
#include "../algorithms/bfs/bfs_async.hpp"
#include "../algorithms/bfs/bfs_forest.hpp"
#include "../algorithms/scan/degree_cnt.hpp"
#include "../algorithms/scan/conductance.hpp"
#include "../algorithms/scan/range_check.hpp"
#include "../algorithms/scan/reverse.hpp"
#include "../algorithms/spmv/spmv.hpp"
#include "../algorithms/sssp/sssp.hpp"
#include "../algorithms/sssp/sssp_forest.hpp"
#include "../algorithms/cc/cc.hpp"
#include "../algorithms/cc/cc_online.hpp"
// Uncomment for ALS
//#include "../algorithms/als/als.hpp"
//#include "../algorithms/als/als_async.hpp"
//
#include "../algorithms/als/checkbip.hpp"
#include "../algorithms/mis/mis.hpp"
#include "../algorithms/pagerank/pagerank.hpp"
#include "../algorithms/pagerank/pagerank_ddf.hpp"
#include "../algorithms/belief_propagation/bp.hpp"
#include "../algorithms/belief_propagation/bp_graphchi.hpp"
#include "../algorithms/mcst/mcst.hpp"
#include "../algorithms/hyper-anf/hyper-anf.hpp"
#include "../algorithms/scc/scc.hpp"
#include "../algorithms/triangles/triangle_counting.hpp"
#include "../algorithms/bc/bc.hpp"

// Macros to keep things clean
#define ADD_ALGORITHM(_cmd, _class, _type, _xscale)			\
  else if((vm["benchmark"].as<std::string>() == #_cmd) &&		\
	  (vm.count("x_scale") > 0 ? (_xscale):!(_xscale)) &&		\
	  (pt.get<unsigned long>("graph.type") == (_type)))		\
    (*(new _class<format::type##_type::format_utils>()))()		\

#define ADD_SG_ALGORITHM(_cmd, _class, _type, _xscale)			\
  else if((vm["benchmark"].as<std::string>() == #_cmd) &&		\
	  (vm.count("x_scale") > 0 ? (_xscale):!(_xscale)) &&		\
	  (pt.get<unsigned long>("graph.type") == (_type)))		\
    (*(new								\
       algorithm::scatter_gather<_class<format::type##_type::format_utils> , \
       format::type##_type::format_utils>()))()

#define ADD_SGASYNC_ALGORITHM(_cmd, _class, _type, _xscale)		\
  else if((vm["benchmark"].as<std::string>() == #_cmd) &&		\
    	  (vm.count("x_scale") > 0 ? (_xscale):!(_xscale)) &&		\
	  (pt.get<unsigned long>("graph.type") == (_type)))		\
    (*(new								\
       algorithm::async_scatter_gather<_class<format::type##_type::format_utils> , \
       format::type##_type::format_utils>()))()

int main(int argc, const char* argv[])
{
  /* Parse the cmd line */
  setup_options(argc, argv);

  BOOST_LOG_TRIVIAL(info) << "STARTUP";
  BOOST_LOG_TRIVIAL(info) << "CORE::GRAPH " << vm["graph"].as<std::string>();
  BOOST_LOG_TRIVIAL(info) << "CORE::ALGORITHM " << vm["benchmark"].as<std::string>();

  /* Read in key graph information */
  init_graph_desc(vm["graph"].as<std::string>());
  
  unsigned long blocked_memory = vm["blocked_memory"].as<unsigned long>();

  if(blocked_memory > 0) {
    BOOST_LOG_TRIVIAL(info) << clock::timestamp() 
			    << " Blocking memory amount " 
			    << blocked_memory;
    (void)map_anon_memory(blocked_memory, true, "Blockmem");
    BOOST_LOG_TRIVIAL(info) << clock::timestamp() 
			    << " Done blocking memory";
  }

  /* Only certain combinations of algorithms and formats
   * are supported. This is encoded below.
   * Algorithms that require the edge value cannot use format type2
   */
  if(false);
  ADD_SG_ALGORITHM(bfs, algorithm::sg_simple::bfs, 1, false);
  ADD_SG_ALGORITHM(bfs, algorithm::sg_simple::bfs, 2, false);
  ADD_SGASYNC_ALGORITHM(bfs_async, algorithm::sg_simple::bfs_async, 1, false);
  ADD_SGASYNC_ALGORITHM(bfs_async, algorithm::sg_simple::bfs_async, 2, false);
  ADD_SG_ALGORITHM(bfs_forest, algorithm::sg_simple::bfs_forest, 1, false);
  ADD_SG_ALGORITHM(bfs_forest, algorithm::sg_simple::bfs_forest, 2, false);
  ADD_SG_ALGORITHM(degree_cnt, algorithm::sg_simple::degree_cnt, 1, false);
  ADD_SG_ALGORITHM(degree_cnt, algorithm::sg_simple::degree_cnt, 2, false);
  ADD_SG_ALGORITHM(conductance, algorithm::sg_simple::conductance, 1, false);
  ADD_SG_ALGORITHM(conductance, algorithm::sg_simple::conductance, 2, false);
  ADD_SG_ALGORITHM(range_check, algorithm::sg_simple::range_check, 1, false);
  ADD_SG_ALGORITHM(range_check, algorithm::sg_simple::range_check, 2, false);
  ADD_ALGORITHM(reverse, algorithm::reverse::reverse, 1, false);
  ADD_ALGORITHM(reverse, algorithm::reverse::reverse, 2, false);
  ADD_SG_ALGORITHM(spmv, algorithm::sg_simple::spmv, 1, false);
  ADD_SG_ALGORITHM(sssp, algorithm::sg_simple::sssp, 1, false);
  ADD_SG_ALGORITHM(sssp_forest, algorithm::sg_simple::sssp_forest, 1, false);
  ADD_SG_ALGORITHM(cc, algorithm::sg_simple::cc, 1, false);
  ADD_SG_ALGORITHM(cc, algorithm::sg_simple::cc, 2, false);
  ADD_ALGORITHM(cc_online, algorithm::cc_online_ns::cc_online, 1, false);
  ADD_ALGORITHM(cc_online, algorithm::cc_online_ns::cc_online, 2, false);
  // Uncomment for ALS
  //ADD_SG_ALGORITHM(als, algorithm::sg_simple::als_factorization, 1, false);
  //ADD_SGASYNC_ALGORITHM(als_async, algorithm::sg_simple::als_async_factorization, 1, false);
  //
  ADD_SG_ALGORITHM(checkbip, algorithm::sg_simple::check_if_bipartite, 1, false);
  ADD_SG_ALGORITHM(mis, algorithm::sg_simple::mis, 1, false);
  ADD_SG_ALGORITHM(mis, algorithm::sg_simple::mis, 2, false);
  ADD_SG_ALGORITHM(pagerank, algorithm::sg_simple::pagerank, 1, false);
  ADD_SG_ALGORITHM(pagerank, algorithm::sg_simple::pagerank, 2, false);
  ADD_SG_ALGORITHM(pagerank_ddf, algorithm::sg_simple::pagerank_ddf, 1, false);
  ADD_SG_ALGORITHM(pagerank_ddf, algorithm::sg_simple::pagerank_ddf, 2, false);
  ADD_ALGORITHM(belief_propagation, algorithm::belief_prop::standard::belief_propagation, 1, false);
  ADD_ALGORITHM(belief_propagation_graphchi, algorithm::belief_prop::graphchi::belief_propagation_graphchi, 1, false);
  ADD_ALGORITHM(belief_propagation_graphchi, algorithm::belief_prop::graphchi::belief_propagation_graphchi, 2, false);
  ADD_ALGORITHM(mcst, algorithm::mcst::mcst, 1, false);
  ADD_SG_ALGORITHM(hyperanf, algorithm::sg_simple::hyperanf, 1, false);
  ADD_SG_ALGORITHM(hyperanf, algorithm::sg_simple::hyperanf, 2, false);
  ADD_ALGORITHM(scc, algorithm::scc::scc, 1, false);
  ADD_ALGORITHM(scc, algorithm::scc::scc, 2, false);
  ADD_ALGORITHM(triangle_counting, algorithm::triangle_counting::triangle_counting, 1, false);
  ADD_ALGORITHM(triangle_counting, algorithm::triangle_counting::triangle_counting, 2, false);
  ADD_ALGORITHM(bc, algorithm::bc::bc, 1, false);

  else {
    BOOST_LOG_TRIVIAL(fatal) << "Don't know how to run " <<
      vm["benchmark"].as<std::string>() << 
      " on " << vm["graph"].as<std::string>();
    exit(-1);
  }
  BOOST_LOG_TRIVIAL(info) << "SHUTDOWN";
  return 0;
} 

