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

#include <iostream>
#include <boost/program_options.hpp>
#include "options.h"

namespace po = boost::program_options;

int process_options(int argc, char** argv, bool rmat_generator, struct options* options)
{
  // Options common for both generators
  po::options_description common("Common options");
  common.add_options()
    ("help", "print this message")    
    ("name", po::value<std::string>(&options->global.graphname), "graph name")
    ("threads", po::value<int>(&options->global.nthreads)->default_value(DEFAULT_NTHREADS), "number of threads")
    ("buffer-size", po::value<size_t>(&options->global.buffer_size), "buffer size in bytes")
    ("bpt", po::value<int>(&options->global.buffers_per_thread)->default_value(DEFAULT_BUFFERS_PER_THREAD), "number of buffers per thread")
    ("symmetric", "emit also a reverse edge for each generated edge")
    ("seed1", po::value<uint64_t>(&options->rng.userseed1)->default_value(DEFAULT_RNG_USERSEED1), "first 64b of seed for rng")
    ("seed2", po::value<uint64_t>(&options->rng.userseed2)->default_value(DEFAULT_RNG_USERSEED2), "second 64b of seed for rng")
  ;

  // Options specific to ER
  po::options_description er("Erdos-Renyi");
  er.add_options()
    ("vertices", po::value<vertex_t>(&options->erdos_renyi.vertices)->default_value(DEFAULT_ER_VERTICES), "number of vertices")
    ("edges", po::value<edge_t>(&options->erdos_renyi.edges)->default_value(DEFAULT_ER_EDGES), "number of edges")
    ("self-loops", "allow self loops")
    ("bipartite", po::value<vertex_t>(&options->erdos_renyi.bipartite), "argument should specify the number of vertices on the left side")
  ;

  // Options specific to RMAT
  po::options_description rmat("R-MAT");
  rmat.add_options()
    ("scale", po::value<int>(&options->rmat.scale)->default_value(DEFAULT_RMAT_SCALE), "log2 of the number of vertices")
    ("edges", po::value<edge_t>(&options->rmat.edges)->default_value(DEFAULT_RMAT_EDGES), "number of edges")
    ("a", po::value<double>(&options->rmat.a)->default_value(DEFAULT_RMAT_A, "0.57"), "a, b, c, d are RMAT probabilities (usually a = 3b = 3c > d)")
    ("b", po::value<double>(&options->rmat.b)->default_value(DEFAULT_RMAT_B, "0.19"), "")
    ("c", po::value<double>(&options->rmat.c)->default_value(DEFAULT_RMAT_C, "0.19"), "")
  ;

  po::options_description cmdline_options;
  if (rmat_generator)
    cmdline_options.add(common).add(rmat);
  else
    cmdline_options.add(common).add(er);

  // Store options
  bool err = false;
  po::variables_map vm;
  try {
    po::store(po::parse_command_line(argc, argv, cmdline_options), vm);
    po::notify(vm);
  } catch (boost::program_options::error) {
    err = true;
  }

  // Help
  if (vm.count("help") || !vm.count("name") || err)
  {
    std::cout << "Usage: " << (rmat_generator ? "rmat" : "erdor-renyi") << " --name graphname [options]\n";
    std::cout << "       " << (rmat_generator ? "rmat" : "erdor-renyi") << " --help\n";
    std::cout << cmdline_options << "\n";
    return 1;
  }

  // Process options (where needed)
  if (vm.count("buffer-size")) {
    options->global.buffer_size = vm["buffer-size"].as<size_t>();
  } else {
    options->global.buffer_size = 0;
  }
  if (vm.count("symmetric")) {
    options->global.symmetric = true;
  } else {
    options->global.symmetric = false;
  }
  if (vm.count("self-loops")) {
    options->erdos_renyi.self_loops = true;
  } else {
    options->erdos_renyi.self_loops = false;
  }
  if (!vm.count("bipartite")) {
    options->erdos_renyi.bipartite = 0;
  }

  return 0;  
}
