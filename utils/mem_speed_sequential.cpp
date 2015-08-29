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
#include "boost_log_wrapper.h"
#include <string.h>
#include<boost/thread.hpp>
#include<boost/thread/mutex.hpp>
#include<boost/assert.hpp>
#include<boost/thread/barrier.hpp>

#define BUFFER_SIZE (256*1024*1024 + 4096)


class mem_speed {
  unsigned char *buffer1;
  unsigned char *buffer2;
  boost::barrier *thread_sync;
  double convert(double mbps)
  {
    double factor = (1 << 20)/((double)1000000.0);
    return mbps*factor;
  }

  unsigned long chunk_size;
public:
  mem_speed(unsigned long chunk_size_in,
	    boost::barrier * thread_sync_in, //! Barrier for threads
	    unsigned char *buffer1_in,
	    unsigned char *buffer2_in) 
    :buffer1(buffer1_in),
     buffer2(buffer2_in),
     thread_sync(thread_sync_in),
     chunk_size(chunk_size_in)
  {
  }
  void operator() ()
  {
    rtc_clock timer;
    unsigned char tmp[chunk_size];
    thread_sync->wait();
    timer.reset();
    timer.start();
    for(unsigned long i=0;i<BUFFER_SIZE;i+=chunk_size) {
      memcpy(tmp, buffer1 + i, chunk_size);
    }
    timer.stop();
    double mbps = ((double)BUFFER_SIZE)/timer.elapsed_time();
    BOOST_LOG_TRIVIAL(info) << "MEMREAD_SPEED "<< convert(mbps) << "MB/s";
    timer.reset();
    timer.start();
    for(unsigned long i=0;i<BUFFER_SIZE;i+=chunk_size) {
      memcpy(buffer1 + i, tmp, chunk_size);
    }
    timer.stop();
    mbps = ((double)BUFFER_SIZE)/timer.elapsed_time();
    BOOST_LOG_TRIVIAL(info) << "MEMWRITE_SPEED "<< convert(mbps) << "MB/s";
    timer.reset();
    timer.start();
    for(unsigned long i=0;i<BUFFER_SIZE;i+=chunk_size) {
      memcpy(buffer2 + i, buffer1 + i, chunk_size);
    }
    timer.stop();
    mbps = ((double)BUFFER_SIZE)/timer.elapsed_time();
    BOOST_LOG_TRIVIAL(info) << "MEMCPY_SPEED "<< convert(mbps) << "MB/s";
  }

};

int main(int argc, char *argv[])
{
  unsigned long threads, chunk_size;
  if(argc < 3) {
    std::cerr << "Usage " << argv[0] << " chunk_size threads" << std::endl;
    exit(-1);
  }
  else {
    chunk_size = atol(argv[1]);
    threads = atol(argv[2]);
  }
  
  boost::thread ** thread_array = new boost::thread * [threads];
  mem_speed ** obj = new mem_speed *[threads];
  boost::barrier * thread_sync = new boost::barrier(threads);
  unsigned long alloc_bytes = 
      ((BUFFER_SIZE + chunk_size - 1) /chunk_size)*chunk_size;
  for(unsigned long i=0;i<threads;i++) {
    unsigned char *buffer1 = (unsigned char *)
      map_anon_memory(alloc_bytes, true, "buffer");
    unsigned char *buffer2 = (unsigned char *)
      map_anon_memory(alloc_bytes, true, "buffer");
    obj[i] = new mem_speed(chunk_size, thread_sync, buffer1, buffer2);
    thread_array[i] = new boost::thread
      (boost::ref(*obj[i]));
  }
  
  for(unsigned long i=0;i<threads;i++) {
    thread_array[i]->join();
  }
  return 0;
}
