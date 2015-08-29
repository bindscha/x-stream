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

#include "x-lib.hpp"
#include "sg_driver.hpp"
#include "sg_driver_async.hpp"
namespace x_lib {
  unsigned long configuration::cached_partitions = 0;
  unsigned long configuration::partition_shift = 0;
  unsigned long configuration::super_partitions = 0;
  unsigned long configuration::super_partition_shift = 0;
  
  x_barrier* x_thread::sync;
  volatile bool x_thread::terminate = false;
  struct work_base * volatile x_thread::work_to_do = NULL;

  ioq** disk_ioq_array;
  static boost::thread **io_threads;
  static unsigned long io_threads_cnt;
  void startup_ioqs(const configuration *config)
  {
    io_threads_cnt = 0;
    io_threads = new boost::thread *[config->num_ioqs];
    disk_ioq_array = new ioq *[config->num_ioqs];
    struct sched_param param;
    struct sched_param new_param;
    int policy;
    int retcode;
    pthread_t native_handle = pthread_self();
    if((retcode = pthread_getschedparam(native_handle, &policy, &param)) 
       != 0) {
      errno = retcode;
      BOOST_LOG_TRIVIAL(warn) << "pthread_getschedparam failed:" <<
	strerror(errno);
    }
    new_param = param;
    new_param.sched_priority = 1;
    // Update to FIFO to spawn IO threads (Linux specific !)
    if((retcode = pthread_setschedparam(native_handle, SCHED_FIFO, &new_param)) 
       != 0) {
      errno = retcode;
      BOOST_LOG_TRIVIAL(warn) << "pthread_getschedparam failed:" <<
	strerror(errno);
    }
    for(unsigned long i=0;i<config->num_ioqs;i++) {
      std::stringstream disk_mnt_pt;
      disk_mnt_pt << "disk.mnt" << i;
      const char *mtpt;
      try {
	mtpt = pt.get<std::string>(disk_mnt_pt.str().c_str()).c_str();
      }
      catch (...) {
	BOOST_LOG_TRIVIAL(warn) << "No mount point specified for disk" << i
				<< ", falling back to current directory";
	mtpt = get_current_wd(); // Note memory will leak here but bounded by
	                         // number of disks in use * wd string size
      }
      disk_ioq_array[i] = new ioq(mtpt);
      disk_io *new_disk = new disk_io(config->stream_unit, disk_ioq_array[i]); 
      io_threads[io_threads_cnt++] = new boost::thread(boost::ref(*new_disk));
    }
    // Switch back to default settings
    if((retcode = pthread_setschedparam(native_handle, policy, &param)) 
       != 0) {
      errno = retcode;
      BOOST_LOG_TRIVIAL(warn) << "pthread_getschedparam failed:" <<
	strerror(errno);
    }
  }
  

  void shutdown_ioqs()
  {
    for(unsigned long i=0;i<io_threads_cnt;i++) {
      disk_ioq_array[i]->notify_terminate();
      io_threads[i]->join();
    }
  }
  void ioq::add_work(disk_stream *work)
  {
    BOOST_ASSERT_MSG(terminate == false, 
		     "Adding work to terminated ioq !");
    boost::unique_lock<boost::mutex> lock(cond_lock);
    work->workq_next = queue;
    queue = work;
    cond_var.notify_one();
  }
    
  disk_stream* ioq::get_work()
  {
    boost::unique_lock<boost::mutex> lock(cond_lock);
    do {
      if(queue == NULL && !terminate) {
	cond_var.wait(lock);
      }
      else {
	break;
      }
    } while(1);
    disk_stream *work = queue;
    if(work != NULL) {
      queue = work->workq_next;
      work->workq_next = NULL;
    }
    else {
      BOOST_ASSERT_MSG(terminate, "Spurious ioq wakeup !");
    }
    return work;
  }
  unsigned long ** memory_buffer::aux_index = NULL;
  unsigned char * memory_buffer::aux_buffer = NULL;
#ifdef PYTHON_SUPPORT
  PyObject * memory_buffer::auxpBuffer = NULL;
#endif
  bool memory_buffer::use_qsort = false;
  unsigned long qsort_keys = 0;
}

namespace algorithm {
  unsigned long sg_pcpu::bsp_phase;
  unsigned long sg_pcpu::current_step;
  rtc_clock sg_pcpu::pc_clock;
  sg_pcpu ** sg_pcpu::per_cpu_array;
  x_barrier *sg_pcpu::sync;
  x_lib::filter * sg_pcpu::scatter_filter;
  per_processor_data ** sg_pcpu::algo_pcpu_array;
  bool sg_pcpu::do_algo_reduce;
  unsigned long sg_async_pcpu::bsp_phase;
  unsigned long sg_async_pcpu::current_step;
  sg_async_pcpu ** sg_async_pcpu::per_cpu_array;
  x_barrier *sg_async_pcpu::sync;
  per_processor_data ** sg_async_pcpu::algo_pcpu_array;
  bool sg_async_pcpu::do_algo_reduce;
}
