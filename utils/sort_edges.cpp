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
#include "../format/type_1.hpp"
#include<iostream>
#include<string>
#include<stdlib.h>

template<typename FormatUtil>
int compare(const void *a, const void *b)
{
  unsigned long key_a, key_b;
  key_a = FormatUtil::split_key((const unsigned char *)a, 0);
  key_b = FormatUtil::split_key((const unsigned char *)b, 0);
  if(key_a < key_b) {
    return -1;
  }
  else if(key_a > key_b) {
    return 1;
  }
  else {
    return 0;
  }
}

template<typename F>
void counting_sort(unsigned char * buf_in, 
		   unsigned char *buf_out,
		   unsigned long items,
		   unsigned long *index,
		   unsigned long keys)
{
  // Init
  for(unsigned long i=0;i<keys;i++) {
    index[i] = 0;
  }
  // Count
  for(unsigned long i=0;i<items;i++) {
    unsigned long key = 
      F::split_key(buf_in + i*F::split_size_bytes(), 0);
    index[key]++;
  }
  // Prefix sum
  unsigned long prev_keys = 0;
  for(unsigned long i=0;i<keys;i++) {
    unsigned long tmp = index[i];
    index[i] = prev_keys;
    prev_keys += tmp;
  }
  // Sanity check
  if(prev_keys != items) {
    BOOST_LOG_TRIVIAL(fatal) << "Counting sort failed !";
    exit(-1);
  }
  // Copy
  for(unsigned long i=0;i<items;i++) {
    unsigned char * src = buf_in + i*F::split_size_bytes();
    unsigned long key = F::split_key(src, 0);
    unsigned char *dst = buf_out + index[key]*F::split_size_bytes();
    index[key]++;
    memcpy(dst, src, F::split_size_bytes());
  }
}

int main(int argc, char *argv[])
{
  rtc_clock wall_clock;
  desc.add_options()
    ("help,h", "Produce help message")
    ("algorithm,a", 
     boost::program_options::value<std::string>()->default_value("quick_sort"),
     "Sort algorithm: quick_sort (default), counting_sort") 
    ("source,i", boost::program_options::value<std::string>()->required(), 
     "Name of the input to sort (need input.ini)");
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
  unsigned char * stream;
  unsigned long items, record_size;
  std::stringstream fname;
  fname << pt.get<std::string>("graph.name");
  items = pt.get<unsigned long>("graph.edges");
  int fd_edges = open (fname.str().c_str(), O_RDONLY|O_LARGEFILE);
  if(fd_edges == -1) {
    BOOST_LOG_TRIVIAL(fatal) << "Unable to open edges file " << 
      fname.str() << ":" << strerror(errno);
    exit(-1);
  }
  if(pt.get<unsigned long>("graph.type") == 1) {
    record_size = format::type1::format_utils::split_size_bytes();
    stream = (unsigned char *)
      map_anon_memory(items*record_size,
		      true, "Sort buffer");
    read_from_file(fd_edges, stream, items*record_size); 
    close(fd_edges);
  }
  else {
    BOOST_LOG_TRIVIAL(fatal) << "Unknown file type";
    exit(-1);
  }
  if(vm["algorithm"].as<std::string>() == "quick_sort") { 
    wall_clock.start();
    qsort(stream, items, 
	  record_size, compare<format::type1::format_utils>);
    wall_clock.stop();
  }
  else if(vm["algorithm"].as<std::string>() == "counting_sort") { 
    wall_clock.start();
    unsigned long keys = pt.get<unsigned long>("graph.vertices");
    counting_sort<format::type1::format_utils>
      (stream, 
       (unsigned char *)map_anon_memory(items*record_size,
					true, "Output buffer"),
       items,
       (unsigned long *)map_anon_memory(keys*sizeof(unsigned long),
					true, "Index"),
       keys);
    wall_clock.stop();
  }
  else {
    BOOST_LOG_TRIVIAL(fatal) << "Don't know how to do " <<  
      vm["algorithm"].as<std::string>();
    exit(-1);
  }
  wall_clock.print("TIME WALL ");
  return 0;
}
