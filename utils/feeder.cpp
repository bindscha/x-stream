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

#include "memory_utils.h"
#include "clock_utils.h"
#include "desc_utils.h"
#include "options_utils.h"
#include "boost_log_wrapper.h"
#include "../core/x-ingest.hpp"
#include "../format/type_1.hpp"
#include "../format/type_2.hpp"
#include<iostream>
#include<string>
#include<stdlib.h>

#define MIN(_a, _b) ((_a) < (_b) ? (_a) : (_b))

void feed(int fd,
	  unsigned long align,
	  unsigned long bytes)
{
  rtc_clock wall_clock;
  rtc_clock feed_clock;
  ingest_t * delta;
  delta = attach_ingest(0xf22);
  wall_clock.start();
  feed_clock.start();
  while(bytes) {
    unsigned long bytes_to_read = MIN(delta->max, bytes);
    bytes_to_read = (bytes_to_read/align)*align;
    read_from_file(fd, &delta->buffer[0], bytes_to_read);
    bytes -= bytes_to_read;
    delta->avail = bytes_to_read;
    __sync_synchronize();
    while(delta->avail);
    feed_clock.stop();
    wall_clock.stop();
    wall_clock.start();
    double wall_time_sec = 
      ((double)wall_clock.elapsed_time())/1000000;
    double feed_time_sec = 
      ((double)feed_clock.elapsed_time())/1000000;
    BOOST_LOG_TRIVIAL(info) << "[" << clock::timestamp() << "] "
                            << "WALL_TIME:INGEST_INTERVAL  "
			    << wall_time_sec << " " 
			    << feed_time_sec;
    feed_clock.reset();
    feed_clock.start();
  }
  delta->eof = true;
}

int main(int argc, char *argv[])
{
  desc.add_options()
    ("help,h", "Produce help message")
    ("source,i", boost::program_options::value<std::string>()->required(), 
     "Name of the input to feed (need input.ini)");
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
  init_graph_desc(vm["source"].as<std::string>());
  unsigned long edges, record_size;
  std::stringstream fname;
  fname << pt.get<std::string>("graph.name");
  edges = pt.get<unsigned long>("graph.edges");
  int fd_edges = open (fname.str().c_str(), O_RDONLY|O_LARGEFILE);
  if(fd_edges == -1) {
    BOOST_LOG_TRIVIAL(fatal) << "Unable to open edges file " << 
      fname.str() << ":" << strerror(errno);
    exit(-1);
  }
  if(pt.get<unsigned long>("graph.type") == 1) {
    record_size = format::type1::format_utils::split_size_bytes();
  }
  else if(pt.get<unsigned long>("graph.type") == 2) {
    record_size = format::type2::format_utils::split_size_bytes();
  } 
  else {
    BOOST_LOG_TRIVIAL(fatal) << "Unsupported file type";
    exit(-1);
  }
  feed(fd_edges, record_size, record_size*edges);
  return 0;
}
