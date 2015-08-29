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

#ifndef _X_LIB_
#define _X_LIB_
#include "disk_io.hpp"
#include "../utils/clock_utils.h"
#include "../utils/memory_utils.h"
#include "../utils/barrier.h"
#include<boost/thread.hpp>
#include "../utils/barrier.h"
#include "../utils/per_cpu_data.hpp"
#include "split_stream.hpp"
#include "x-ingest.hpp"
#include<sys/resource.h>
#define OUTBUF_SIZE 8192
namespace x_lib {
  struct stream_callback_state {
    bool loopback;
    bool ingest;
    unsigned char *state;
    unsigned long superp;
    unsigned long partition_id; 
    unsigned char *bufin;    // callee can change
    unsigned long bytes_in;  // callee must set
    unsigned char *bufout;   // callee can change
    unsigned long bytes_out; // callee must set
    unsigned long bytes_out_max; 
#ifdef PYTHON_SUPPORT
    PyObject *pbufin;
    unsigned long pbufin_offset;
    PyObject *pbufout;
    PyObject *pstate;
    unsigned long pstate_offset;
#endif
    algorithm::per_processor_data *cpu_state;
  };
  
  struct work_base {
    virtual void operator() 
    (unsigned long processor_id,
     configuration *config,
     x_barrier *sync, 
     algorithm::per_processor_data * cpu_state,
     unsigned char *outbuf) = 0;
  };

  template<typename A>
  struct state_iter_work:public work_base {
    memory_buffer *state_buffer;
    unsigned long superp;
    void operator() (unsigned long processor_id,
		     configuration *config,
		     x_barrier *sync,
		     algorithm::per_processor_data *cpu_state,
		     unsigned char *outbuf)
    {
      unsigned long partitions_per_cpu =
	config->cached_partitions/config->processors;
      unsigned long remainder = config->cached_partitions - 
	partitions_per_cpu*config->processors;
      unsigned long start = partitions_per_cpu*processor_id;
      unsigned long count = partitions_per_cpu;
      if(processor_id == (config->processors - 1)) {
	count += remainder;
      }
      unsigned long i,j,k;
      for(i=start,j=0;j<count;i++,j++) {
	unsigned long vertices = config->state_count(superp, i);
	for(k=0;k<vertices;k++) {
	  unsigned char * vertex =
	    &state_buffer->buffer[state_buffer->index[0][i] + k*config->vertex_size];
	  A::state_iter_callback(superp, i, k, vertex, cpu_state);   
	}
      }
    }
  } __attribute__((__aligned__(64)));

  template<typename A, typename IN, typename OUT>
  struct work:public work_base {
    unsigned long superp;
    memory_buffer *state;
    memory_buffer *stream_in;
    memory_buffer *ingest;
    memory_buffer *stream_out;
    filter *input_filter;
    disk_stream *disk_stream_out;
    rtc_clock *io_clock;
    buffer_manager *bufm;
    bool loopback;

    bool flush_buffer(unsigned long processor_id, 
		      configuration *config,
		      x_barrier *sync,
		      algorithm::per_processor_data *cpu_state)
    {
      memory_buffer *loopback_buffer = stream_out;
      bool empty = false;
      sync->wait();
      if(stream_out->bufsize > 0) {
	make_index<OUT, map_super_partition_wrap>(stream_out, processor_id,
						  config->super_partitions,
						  sync);
	if(processor_id == 0) {
	  if(loopback) {
	    // IO only if more than one superpartition
	    if(config->super_partitions > 1) {
	      // Hold on for looping back on current superpartition
	      loopback_buffer->skip_superp = superp;
	      loopback_buffer->set_refcount(loopback_buffer->get_refcount() + 1);
	      io_clock->start();
	      disk_stream_out->append(stream_out);
	      io_clock->stop();
	    }
	  }
	  else {
	    io_clock->start();
	    disk_stream_out->append(stream_out);
	    io_clock->stop();
	  }
	  stream_out = NULL;
	}
	if(loopback) {
	  if(processor_id == 0) {
	    stream_out = bufm->get_buffer();
	  }
	  sync->wait();
	  unsigned long loopback_bytes;
	  unsigned char *loopback_src   = 
	    loopback_buffer->get_substream(processor_id, superp, &loopback_bytes);
	  unsigned long offset_start;
	  unsigned long offset_stop;
	  do {
	    offset_start = stream_out->bufsize;
	    offset_stop = offset_start + loopback_bytes;
	  } while (!__sync_bool_compare_and_swap(&stream_out->bufsize, 
						 offset_start,
						 offset_stop));
	  memcpy(stream_out->buffer + offset_start, loopback_src,
		 loopback_bytes);
	  sync->wait();
	  make_index<OUT, map_cached_partition_wrap >
	    (stream_out, processor_id, config->cached_partitions, sync);
	  stream_callback_state callback_state;
	  callback_state.superp = superp;
	  callback_state.bytes_out_max = 0;
	  callback_state.cpu_state = cpu_state;
	  callback_state.loopback  = true;
	  sync->wait();
	  stream_out->work_queues->prep_dq(processor_id);
	  sync->wait();
	  unsigned long partition_id;
	  while((partition_id = stream_out->work_queues->dq(processor_id)) != ULONG_MAX) {
	    callback_state.state = &state->buffer[state->index[0][partition_id]];
	    callback_state.partition_id = partition_id;
	    A::partition_pre_callback(superp, partition_id, cpu_state);
	    for(unsigned long i=0;i<config->processors;i++) {
	      callback_state.bufin = 
		stream_out->get_substream(i, partition_id,
					  &callback_state.bytes_in);
	      callback_state.bufout = NULL; // No output allowed in loopback
#ifdef PYTHON_SUPPORT
	      callback_state.pbufin        = stream_out->pBuffer;
	      callback_state.pbufin_offset = callback_state.bufin - stream_out->buffer;
	      callback_state.pstate        = state->pBuffer;
	      callback_state.pstate_offset = callback_state.state - state->buffer;
#endif
	      A::partition_callback(&callback_state);
	    }
	    A::partition_post_callback(superp, partition_id, cpu_state);
	  }
	  sync->wait();
	  if(processor_id == 0) {
	    loopback_buffer->set_refcount(loopback_buffer->get_refcount() - 1);
	    stream_out->set_refcount(stream_out->get_refcount() - 1);
	    stream_out = NULL;
	  }
	}
	if(processor_id == 0) {
	  if(vm["force_buffers"].as<unsigned long>() == 0) {
	    stream_out = bufm->get_buffer();
	  }
	  else {
	    stream_out = NULL;
	  }
	}
      }
      else {
	empty = true;
      }
      sync->wait();
      return empty;
    }

    void final_flush(unsigned long processor_id, 
		     configuration *config,
		     x_barrier *sync, 
		     algorithm::per_processor_data *cpu_state)
    {
      bool empty;
      if(stream_out != NULL) {
	do {
	  empty = flush_buffer(processor_id, config, sync, cpu_state);
	} while(!empty && stream_out != NULL);
      }
      sync->wait();
      if(processor_id == 0 && stream_out != NULL) {
	stream_out->set_refcount(stream_out->get_refcount() - 1);
      }
    }

    void append_buffer(unsigned char *buffer, unsigned long bytes,
		       unsigned long processor_id, configuration *config,
		       x_barrier *sync, algorithm::per_processor_data *cpu_state)
    {
      while(bytes) {
	unsigned char *base  = stream_out->buffer;
	unsigned long offset_start = stream_out->bufsize;
	unsigned long space = 	  
	  (MIN(stream_out->bufbytes - offset_start, bytes)
	   /OUT::item_size())*OUT::item_size();
	unsigned long offset_stop = offset_start + space;
	if(__sync_bool_compare_and_swap(&stream_out->bufsize, 
					offset_start,
					offset_stop)) {
	  memcpy(base + offset_start, buffer, space);
	  if(space != bytes) { // Need flush
	    (void)flush_buffer(processor_id, config, sync, cpu_state);
	  }
	  bytes  -= space;
	  buffer += space;
	}
      }
    }

    void execute_callback(stream_callback_state *callback,
			  unsigned long processor_id,
			  configuration *config,
			  x_barrier *sync,
			  algorithm::per_processor_data *cpu_state,
			  unsigned char *outbuf)
    {
      callback->bufout = outbuf;
      callback->bytes_out = 0;
#ifdef PYTHON_SUPPORT
      callback->pbufin        = stream_in->pBuffer;
      callback->pbufin_offset = callback->bufin - stream_in->buffer;
      npy_intp dim[1]={OUTBUF_SIZE};
      callback->pbufout       = PyArray_SimpleNewFromData(1, dim, NPY_UINT8, (void *)outbuf);
      callback->pstate        = state->pBuffer;
      callback->pstate_offset = callback->state - state->buffer;
#endif
      while(callback->bytes_in) {
	A::partition_callback(callback);
	if(stream_out != NULL && callback->bytes_out > 0) {
	  append_buffer(outbuf, callback->bytes_out,
			processor_id, config, sync,
			cpu_state);
	  callback->bufout = outbuf;
	  callback->bytes_out = 0;
	}
      }
#ifdef PYTHON_SUPPORT
      Py_DECREF(callback->pbufout);
#endif
    }

    void operator() (unsigned long processor_id,
		     configuration *config,
		     x_barrier *sync, 
		     algorithm::per_processor_data *cpu_state,
		     unsigned char *outbuf)
    {
      if(stream_in == NULL && stream_out == NULL) {
	A::do_cpu_callback(cpu_state);
	return;
      }
      stream_callback_state callback_state;
      callback_state.superp = superp;
      callback_state.bytes_out_max =
	(OUTBUF_SIZE/OUT::item_size())*OUT::item_size();
      callback_state.cpu_state = cpu_state;
      callback_state.loopback  = false;
      // Must play ingest first
      if(ingest != NULL) {
	sync->wait();
	callback_state.ingest = true;
	make_index<IN, map_super_partition_wrap>(ingest, processor_id,
						 config->super_partitions,
						 sync);
	unsigned long ingest_bytes;
	unsigned char *ingest_src   = 
	  ingest->get_substream(processor_id, superp, &ingest_bytes);
	unsigned long offset_start;
	unsigned long offset_stop;
	do {
	  offset_start = stream_in->bufsize;
	  offset_stop = offset_start + ingest_bytes;
	} while (!__sync_bool_compare_and_swap(&stream_in->bufsize, 
					       offset_start,
					       offset_stop));
	memcpy(stream_in->buffer + offset_start, 
	       ingest_src,
	       ingest_bytes);
	sync->wait();
	if(processor_id == 0) {
	  BOOST_ASSERT_MSG(stream_in->bufsize == ingest->bufsize,
			   "Error in partitioning ingest !");
	  ingest = NULL;
	}
      }
      else {
	callback_state.ingest = false;
      }
      if(stream_in != NULL) {
	make_index<IN, map_cached_partition_wrap >
	  (stream_in, processor_id, config->cached_partitions, sync);
      }
      sync->wait();
      input_filter->prep_dq(processor_id);
      sync->wait();
      unsigned long partition_id;
      while((partition_id = input_filter->dq(processor_id)) != ULONG_MAX) {
	callback_state.state = &state->buffer[state->index[0][partition_id]];
	callback_state.partition_id = partition_id;
	A::partition_pre_callback(superp, partition_id, cpu_state);
	if(stream_in != NULL) {
	  for(unsigned long i=0;i<config->processors;i++) {
	    callback_state.bufin = 
	      stream_in->get_substream(i, partition_id,
				       &callback_state.bytes_in);
	    execute_callback(&callback_state, 
			     processor_id, 
			     config, 
			     sync, 
			     cpu_state,
			     outbuf);
	  }
	}
	else {
	  callback_state.bufin    = callback_state.state;
	  callback_state.bytes_in = 
	    config->vertex_size*config->state_count(superp, partition_id);
	  execute_callback(&callback_state, processor_id, config, sync,
			   cpu_state, outbuf);
	}
	A::partition_post_callback(superp, partition_id, cpu_state);
      }
      if(stream_out != NULL) {
	final_flush(processor_id, config, sync, cpu_state);
      }
    }
  } __attribute__((aligned(64)));


  class x_thread {
  public:
    static x_barrier *sync;
    struct configuration *config;
    algorithm::per_processor_data * const cpu_state;
    const unsigned long processor_id; 
    static volatile bool terminate;
    static struct work_base *volatile work_to_do;
    unsigned char *outbuf;

    x_thread(struct configuration* config_in,
	     unsigned long processor_id_in,
	     algorithm::per_processor_data *cpu_state_in)
      :config(config_in),
       cpu_state(cpu_state_in),
       processor_id(processor_id_in)
    {
      if(sync == NULL) { // First object
	sync = new x_barrier(config->processors);
      }
      outbuf = (unsigned char *)map_anon_memory(OUTBUF_SIZE, true, "thread outbuf");
    }
    
    void operator() ()
    {
      do {
	sync->wait();
	if(terminate) {
	  break;
	}
	else {
	  (*work_to_do)(processor_id, config, sync, cpu_state, outbuf);
	  sync->wait(); // Must synchronize before p0 exits (object is on stack)
	}
      }while(processor_id != 0);
    }
    friend class stream_IO;
  } __attribute__((__aligned__(64)));

  template<typename A>
  class streamIO {
    struct configuration *config;
    disk_stream **streams;
    memory_buffer **ingest_buffers;
    buffer_manager *buffers;
    memory_buffer *state_buffer;
    x_thread **workers;
    boost::thread ** thread_array;
    algorithm::per_processor_data ** cpu_state_array;

    /* Clock */
    rtc_clock io_wait_time;

#ifdef PYTHON_SUPPORT
     //M: has to be called before creating python arrays! It is a macro, it has to be inside an int-returning function
    int import_numpy_array(){
      import_array();
      return 0;
    }
#endif
    
    void make_config()
    {
#ifdef PYTHON_SUPPORT
      Py_Initialize();
      import_numpy_array();
#endif
      config->memory_buffer_object_size = sizeof(memory_buffer);
      config->disk_stream_object_size = sizeof(disk_stream);
      config->ioq_object_size = sizeof(ioq);
      config->vertex_size = A::vertex_state_bytes();
      config->vertex_footprint = config->vertex_size + 
		A::vertex_stream_buffer_bytes();
      config->max_streams = A::max_streams();
      config->max_buffers = A::max_buffers();
      config->init();
      if(vm.count("autotune") > 0) {
	bool success = config->autotune();
	if(!success) {
	  BOOST_LOG_TRIVIAL(fatal) << "Auto-tuning failed !";
	  config->dump_config();
	  exit(-1);
	}
      }
      else {
	config->manual();
      }
      config->dump_config();
      /* Sanity checks */
      check_pow_2(config->super_partitions, "Super partitions must be power of two");
      check_pow_2(config->cached_partitions, "Cached partitions must be power of two");
      check_pow_2(config->processors, "Processors must be power of two");
    }


  public:
    streamIO()
    {
      config = new struct configuration();
      make_config();
      buffers = new buffer_manager(config, 
				   config->buffer_size,
				   config->max_buffers);
      state_buffer = new memory_buffer(config,
				       config->max_state_bufsize());
      startup_ioqs(config);
      streams        = new disk_stream *[config->max_streams];
      ingest_buffers = new memory_buffer *[config->max_streams]; 
      for(unsigned long i=0;i<config->max_streams;i++) {
	streams[i] = NULL;
	ingest_buffers[i] = 0;
      }      
      workers = new x_thread* [config->processors];
      thread_array = new boost::thread* [config->processors];
      cpu_state_array = new algorithm::per_processor_data* [config->processors];
      for(unsigned long i=0;i<config->processors;i++) {
	cpu_state_array[i] = A::create_per_processor_data(i);
	workers[i] = new x_thread(config, i, cpu_state_array[i]);
	if(i > 0) {
	  thread_array[i] = new boost::thread(boost::ref(*workers[i]));
	}
      }
    }
    
    const configuration *get_config()
    {
      return config;
    }

    unsigned long open_stream(const char *stream_name,
			      bool new_file,
			      unsigned long io_queue_number,
			      unsigned long stream_unit,
			      unsigned long override_superp_cnt = ULONG_MAX)
    {
      unsigned long stream_id;
      for(stream_id=0;stream_id<config->max_streams;stream_id++){
	if(streams[stream_id] == NULL) {
	  break;
	}
      }
      if(stream_id == config->max_streams) {
	BOOST_LOG_TRIVIAL(fatal) << "Exceeded max streams !";
	exit(-1);
      }
      if(io_queue_number >= config->num_ioqs) {
	BOOST_LOG_TRIVIAL(fatal) << "Trying to access non-existent disk!";
	exit(-1);
      }
      streams[stream_id] = 
	new disk_stream(stream_name, new_file, 
			vm.count("compressed_io") > 0,
			disk_ioq_array[io_queue_number],
			buffers, 
			override_superp_cnt == ULONG_MAX ?
			config->super_partitions:override_superp_cnt,
			stream_unit,
			config->stream_unit);
      return stream_id;
    }
    
    void close_stream(unsigned long stream)
    {
      streams[stream]->rewind(true);
      // TBD -- cleanup resources
    }
    
    void state_load(int stream, unsigned long superp)
    {
      io_wait_time.start();
      state_buffer->wait_uptodate(); // Previous op complete ?
      state_buffer->bufsize = config->state_bufsize(superp);
      streams[stream]->read_one_shot(state_buffer, superp);
      state_buffer->wait_uptodate();
      io_wait_time.stop();
    }

    void state_prepare(unsigned long superp)
    {
      // Make sure state is uptodate
      state_buffer->wait_uptodate();
      /* Setup pmap */
      unsigned long v_so_far = 0;
      for(unsigned long i=0;i<config->cached_partitions;i++) {
	//Count entries in the partition
	state_buffer->index[0][i] = v_so_far;
	v_so_far += config->vertex_size*config->state_count(superp, i);
      }
    }

    void state_store(int stream, unsigned long superp)
    {
      state_buffer->wait_uptodate(); // Previous op complete ?
      state_buffer->bufsize = config->state_bufsize(superp);
      io_wait_time.start();
      streams[stream]->write_one_shot(state_buffer, superp);
      io_wait_time.stop();
    }

    bool stream_empty(int stream)
    {
      return streams[stream]->is_empty();
    }
  
    void terminate()
    {
      workers[0]->terminate = true;
      (*workers[0])();
    
      for(unsigned long i=1;i<config->processors;i++) {
	thread_array[i]->join();
      }
      shutdown_ioqs();
      struct rusage ru;
      (void)getrusage(RUSAGE_SELF, &ru);
      BOOST_LOG_TRIVIAL(info) << "CORE::RUSAGE::MAX_RSS_KB " << ru.ru_maxrss;
      BOOST_LOG_TRIVIAL(info) << "CORE::RUSAGE::MINFLT " << ru.ru_minflt;
      BOOST_LOG_TRIVIAL(info) << "CORE::RUSAGE::MAJFLT " << ru.ru_majflt;
      BOOST_LOG_TRIVIAL(info) << "CORE::RUSAGE::MAJFLT " << ru.ru_majflt;
      BOOST_LOG_TRIVIAL(info) << "CORE::RUSAGE::INBLK "  << ru.ru_inblock;
      BOOST_LOG_TRIVIAL(info) << "CORE::RUSAGE::OUBLK " << ru.ru_oublock;
      BOOST_LOG_TRIVIAL(info) << "CORE::UTILS::BYTES_READ " << stat_bytes_read;
      BOOST_LOG_TRIVIAL(info) << "CORE::UTILS::BYTES_WRITTEN " <<
	stat_bytes_written;
      io_wait_time.print("CORE::TIME::IO_WAIT");
    }

    void rewind_stream(unsigned long stream)
    {
      streams[stream]->rewind();
    }

    void reset_stream(unsigned long stream, unsigned long super_partition)
    {
      streams[stream]->reset(super_partition);
    }

    template<typename B, typename IN, typename OUT> friend 
    bool do_stream(streamIO<B> *sio,
		   unsigned long superp,
		   unsigned long stream_in,
		   unsigned long stream_out,
		   filter *override_input_filter,
		   bool loopback);
    template<typename B> friend
    bool do_cpu(streamIO<B> *sio, unsigned long superp);
    template<typename B> friend
    void do_state_iter(streamIO<B> *sio, unsigned long superp);
    template<typename B> friend
    bool ingest(streamIO<B> *sio, 
		unsigned long stream_in, 
		ingest_t *ingest_segment);
    template<typename B> friend
    void merge_ingest(streamIO<B> *sio, unsigned long stream);
  };
  
#define SETUP_STREAMOUT()					\
  do {								\
    work_item.bufm = sio->buffers;				\
    if(stream_out != ULONG_MAX) {				\
      work_item.stream_out = sio->buffers->get_buffer();	\
      work_item.disk_stream_out = sio->streams[stream_out];	\
      work_item.io_clock = &sio->io_wait_time;			\
    }								\
    else {							\
      work_item.stream_out = NULL;				\
      work_item.disk_stream_out = NULL;				\
      work_item.io_clock = NULL;				\
    }								\
    sio->workers[0]->work_to_do = &work_item;			\
  }while(0)


  template<typename A, typename IN, typename OUT>
  static bool do_stream(streamIO<A> *sio,
			unsigned long superp,
			unsigned long stream_in,
			unsigned long stream_out,
			filter *override_input_filter,
			bool loopback = false)
  {
    struct work<A, IN, OUT> work_item;
    bool reduce_result;
    work_item.superp   = superp;
    work_item.state    = sio->state_buffer;
    work_item.loopback = loopback;
    work_item.ingest   = NULL;
    if(stream_in != ULONG_MAX) {
      if(sio->ingest_buffers[stream_in] != NULL) {
	work_item.ingest = sio->ingest_buffers[stream_in];
	work_item.stream_in = sio->buffers->get_buffer();
	work_item.input_filter = work_item.stream_in->work_queues;
	SETUP_STREAMOUT();
	(*sio->workers[0])();
	work_item.stream_in->set_refcount(work_item.stream_in->get_refcount()-1);
      }
      while(!sio->streams[stream_in]->stream_eof(superp)) {
	sio->io_wait_time.start();
	work_item.stream_in = 
	  sio->streams[stream_in]->read(superp);
	sio->io_wait_time.stop();
	if(override_input_filter != NULL) {
	  work_item.input_filter = override_input_filter;
	}
	else {
	  work_item.input_filter = work_item.stream_in->work_queues;
	}
	SETUP_STREAMOUT();
	(*sio->workers[0])();
	work_item.stream_in->set_refcount(work_item.stream_in->get_refcount()-1);
      }
    }
    else {
      BOOST_ASSERT_MSG(override_input_filter != NULL,
		       "Must have input stream or input filter !");
      work_item.input_filter = override_input_filter;
      work_item.stream_in = NULL;
      work_item.bufm = sio->buffers;
      BOOST_ASSERT_MSG(stream_out != ULONG_MAX,
		       "Must have input stream or output stream !");
      work_item.stream_out = sio->buffers->get_buffer();
      work_item.disk_stream_out = sio->streams[stream_out];
      work_item.io_clock = &sio->io_wait_time;
      sio->workers[0]->work_to_do = &work_item;
      (*sio->workers[0])();
    }
    reduce_result =
      sio->cpu_state_array[0]->reduce(sio->cpu_state_array,
				      sio->config->processors);
    return reduce_result;
  }

  class DUMMY_IN
  {
  public:
    static unsigned long item_size()
    {
      BOOST_ASSERT_MSG(false, "Should not be called !");
      return 0;
    }
    static unsigned long key(unsigned char *buffer)
    {
      BOOST_ASSERT_MSG(false, "Should not be called !");
      return 0;
    }
  };

  class DUMMY_OUT
  {
  public:
    static unsigned long item_size()
    {
      BOOST_ASSERT_MSG(false, "Should not be called !");
      return 0;
    }
    static unsigned long key(unsigned char *buffer)
    {
      BOOST_ASSERT_MSG(false, "Should not be called !");
      return 0;
    }
  };

  template<typename A>
  static bool do_cpu(streamIO<A> *sio, unsigned long superp)
  {
    struct work<A, DUMMY_IN, DUMMY_OUT> work_item;
    work_item.state = NULL;
    work_item.stream_in  = NULL;
    work_item.ingest     = NULL;
    work_item.stream_out = NULL;
    sio->workers[0]->work_to_do = &work_item;
    (*sio->workers[0])();
    bool reduce_result =
      sio->cpu_state_array[0]->reduce(sio->cpu_state_array,
				      sio->config->processors);
    return reduce_result;
  }
  
  template<typename A>
  static void do_state_iter(streamIO<A> *sio, unsigned long superp) 
  {
    state_iter_work<A> work;
    work.state_buffer = sio->state_buffer;
    work.superp = superp;
    sio->workers[0]->work_to_do = &work;
    (*sio->workers[0])();
  }
  
  // Returns true on eof
  template<typename A>
  static bool ingest(streamIO<A> *sio, 
		     unsigned long stream, 
		     ingest_t *ingest_segment)
  {
    while(ingest_segment->avail == 0 && !ingest_segment->eof);
    if(ingest_segment->eof) {
      return true;
    }
    memory_buffer *ingest_buffer = sio->buffers->get_buffer();
    memcpy(ingest_buffer->buffer, ingest_segment->buffer, ingest_segment->avail);
    ingest_buffer->bufsize = ingest_segment->avail;
    ingest_segment->avail = 0; // Signal that the ingest segment is free
    sio->ingest_buffers[stream] = ingest_buffer;
    return false;
  }
  
  template<typename A>
  static void merge_ingest(streamIO<A> *sio, unsigned long stream)
  {
    if(sio->ingest_buffers[stream] != NULL) {
      sio->streams[stream]->ingest(sio->ingest_buffers[stream]);
      sio->ingest_buffers[stream] = NULL;
    }
  }
}
#endif
