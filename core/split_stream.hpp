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

#ifndef _SPLIT_STREAM_
#define _SPLIT_STREAM_
#include "autotuner.hpp"
#include "x-splitter.hpp"
#include "../utils/memory_utils.h"
#include "../utils/barrier.h"
#include<zlib.h>

#ifdef PYTHON_SUPPORT
#include</usr/include/python3.2/Python.h>
#include</usr/lib/python3/dist-packages/numpy/core/include/numpy/arrayobject.h>
#endif

namespace x_lib {
  class disk_stream;
  class ioq {
    boost::mutex cond_lock;
    boost::condition_variable cond_var;
    disk_stream * queue;
    volatile bool terminate;
    const char* mnt_pt;
  public:
    ioq(const char *mnt_pt_in)
      :mnt_pt(mnt_pt_in)
    {
      queue = NULL;
      terminate = false;
    }
    
    void add_work(disk_stream *work);
    void notify_terminate()
    {
      boost::unique_lock<boost::mutex> lock(cond_lock);
      terminate = true;
      cond_var.notify_all();
    }
    disk_stream* get_work();
    const char* get_mnt_pt()
    {
      return mnt_pt;
    }
  };

  extern ioq** disk_ioq_array; 
  extern void startup_ioqs(const configuration *config);
  extern void shutdown_ioqs();

  class memory_buffer {
    unsigned long *per_cpu_sizes;
    static bool use_qsort;
    static unsigned long **aux_index;
    static unsigned char *aux_buffer;
#ifdef PYTHON_SUPPORT
    static PyObject *auxpBuffer;
#endif
    unsigned long refcount;
    volatile unsigned long index_entries; // How many partitions ?
    unsigned long max_partitions;

    void switch_with_aux()
    {
      unsigned long ** tmp_index;
      tmp_index  = aux_index;
      aux_index  = index;
      index      = tmp_index;
      unsigned char * tmp_buffer;
      tmp_buffer = aux_buffer;
      aux_buffer = buffer;
      buffer     = tmp_buffer;
#ifdef PYTHON_SUPPORT
      PyObject * tmp_pbuffer;
      tmp_pbuffer = auxpBuffer;
      auxpBuffer = pBuffer;
      pBuffer = tmp_pbuffer;
#endif
    }
    
  public:    
    filter *work_queues;
    unsigned long **index;//[cpu][partition]
    const unsigned long bufbytes;
    volatile bool indexed;
    // Accessed by IO
    unsigned char *buffer;
    unsigned long bufsize;
    volatile bool uptodate;   // I/O outstanding 
    bool dirty; // Debug aid
    volatile unsigned long skip_superp;
    configuration *config;
#ifdef PYTHON_SUPPORT
    PyObject *pBuffer;
#endif
    memory_buffer(configuration *details,
		  unsigned long bufbytes_in)
      :refcount(0),
       bufbytes(bufbytes_in),
       indexed(false),
       bufsize(0),
       uptodate(true),
       dirty(false),
       skip_superp(-1UL),
       config(details)
    {
      max_partitions = (config->cached_partitions > config->super_partitions) ?
	config->cached_partitions:config->super_partitions;
      work_queues = new filter(max_partitions, config->processors);
      index = new unsigned long*[config->processors];
      per_cpu_sizes = new unsigned long[config->processors];
      for(unsigned long i=0;i<config->processors;i++) {
	index[i] = new unsigned long[max_partitions];
	per_cpu_sizes[i] = 0;
	for(unsigned long j=0;j<max_partitions;j++) {
	  index[i][j] = 0;
	}
      }
      if(aux_index == NULL) {
	use_qsort = (vm.count("qsort") > 0);
	if(!use_qsort) {
	  aux_index = new unsigned long*[config->processors];
	  for(unsigned long i=0;i<config->processors;i++) {
	    aux_index[i] = new unsigned long[max_partitions];
	    for(unsigned long j=0;j<max_partitions;j++) {
	      aux_index[i][j] = 0;
	    }
	  }
	}
      }
      buffer = (unsigned char *)
	map_anon_memory(bufbytes, true, "memory buffer");
#ifdef PYTHON_SUPPORT
      npy_intp dim[1]={bufbytes};
      pBuffer = PyArray_SimpleNewFromData(1, dim, NPY_UINT8, (void *)buffer);
#endif
      if(aux_buffer == NULL && !use_qsort) {
	aux_buffer = (unsigned char *)
	  map_anon_memory(bufbytes, true, "memory buffer");
#ifdef PYTHON_SUPPORT
	auxpBuffer = PyArray_SimpleNewFromData(1, dim, NPY_UINT8, (void *)aux_buffer);
#endif
      }
    }

    // Set the bufsize for IO
    void set_bufsize(unsigned long bytes_available,
		     unsigned long stream_unit_bytes)
    {
      bufsize = MIN(bytes_available, bufbytes);
      unsigned long stream_units =
	bufsize/stream_unit_bytes;
      bufsize = stream_units*stream_unit_bytes;
    }

    // Reset the index
    void reset_index(unsigned long processor_id)
    {
      per_cpu_sizes[processor_id] = 0;
      memset(index[processor_id], 0, 
	     max_partitions*sizeof(unsigned long));
      work_queues->done(processor_id);
      skip_superp = -1UL;
    }
    
    void set_flat_index()
    {
      per_cpu_sizes[0] = bufsize;
      index[0][0]      = 0;
      index_entries    = 1;
      indexed          = true;
    }

    unsigned long get_refcount()
    {
      return refcount;
    }
    
    void set_refcount(unsigned long new_refcount)
    {
      refcount = new_refcount;
      if(refcount == 0) {
	BOOST_ASSERT_MSG(uptodate == true, "Trying to free a busy buffer !");
	BOOST_ASSERT_MSG(dirty == false, "Trying to free a dirty buffer !");
	if(indexed) {
	  for(unsigned long i=0;i<config->processors;i++) {
	    reset_index(i);
	  }
	  indexed  = false;
	}
      }
    }
  
    unsigned char *get_substream(unsigned long processor,
				 unsigned long partition, 
				 unsigned long *size)
    {
      BOOST_ASSERT_MSG(indexed, "Cannot extract substream without index");
      BOOST_ASSERT_MSG(partition < index_entries,
		       "Too few partitions in index !");
      if(partition < ( index_entries - 1)) {
	*size = (index[processor][partition + 1] -
		 index[processor][partition]);
      }
      else {
	*size = (per_cpu_sizes[processor] + index[processor][0]) -
	  index[processor][partition];
      }
      BOOST_ASSERT_MSG((*size) <= per_cpu_sizes[processor],
		       "Substream size out of range");
      return buffer + index[processor][partition]; 
    }

    void wait_uptodate()
    {
      while(!uptodate);
      __sync_synchronize();
    }

    template<typename T, typename M> friend void 
    make_index(memory_buffer *stream,
	       unsigned long processor_id,
	       unsigned long partitions,
	       x_barrier* thread_sync);
  };

  template<typename T, typename M>
  static void make_index(memory_buffer *stream,
			 unsigned long processor_id,
			 unsigned long partitions,
			 x_barrier* thread_sync)
  {
    if(stream->indexed) {
      thread_sync->wait();
      return;
    }
    unsigned long items = stream->bufsize/T::item_size();
    BOOST_ASSERT_MSG(stream->bufsize % T::item_size() == 0,
		     "Buffer does not contain an integral number of items !");
    unsigned long items_per_cpu = items/stream->config->processors;
    unsigned long remainder = items - items_per_cpu*stream->config->processors;
    stream->reset_index(processor_id);
    stream->per_cpu_sizes[processor_id] = items_per_cpu*T::item_size();
    if(processor_id == (stream->config->processors - 1)) {
      stream->per_cpu_sizes[processor_id] += remainder*T::item_size();
    }
    stream->index[processor_id][0] =
      items_per_cpu*processor_id*T::item_size();
    thread_sync->wait();
    bool twizzle;
    if(!stream->use_qsort) {
      twizzle = x_split<T, M>(stream->index[processor_id], 
			      stream->buffer,
			      stream->aux_index[processor_id], 
			      stream->aux_buffer,
			      stream->per_cpu_sizes[processor_id],
			      stream->config->fanout,
			      partitions,
			      stream->work_queues);
    }
    else {
      if(processor_id == 0) {
	qsort_keys = partitions;
      }
      thread_sync->wait();
      x_split_qsort<T, M>(stream->index[processor_id], 
			  stream->buffer,
			  stream->per_cpu_sizes[processor_id],
			  stream->work_queues);
      twizzle = false;
    }
    thread_sync->wait();
    if(processor_id == 0) {
      if(twizzle) {
	stream->switch_with_aux();
      }
      stream->index_entries = partitions;
      stream->indexed = true;
    }
    // Stream in a steady state here
    thread_sync->wait();
  }

  class buffer_manager {
    memory_buffer **buffers;
    unsigned long num_buffers;
  public:
    buffer_manager(configuration *config,
		   unsigned long bufbytes,
		   unsigned long bufcount)
      :num_buffers(bufcount)
    {
      buffers = new memory_buffer *[num_buffers];
      for(unsigned long i=0;i<num_buffers;i++) {
	buffers[i] = new memory_buffer(config, bufbytes);
      }
    }
    
    memory_buffer* get_buffer()
    {
      for(unsigned long i=0;i<num_buffers;i++) {
	if(buffers[i]->get_refcount() == 0) {
	  buffers[i]->set_refcount(1);
	  buffers[i]->bufsize = 0;
	  return buffers[i];
	}
      }
      BOOST_LOG_TRIVIAL(fatal) <<  "Out of memory buffers !";
      exit(-1);
    }
    
    unsigned long available_buffers()
    {
      unsigned long avail = 0;
      for(unsigned long i=0;i<num_buffers;i++) {
	if(buffers[i]->get_refcount() == 0) {
	  avail++;
	}
      }
      return avail;
    }
  };

  class disk_stream {
    const unsigned long superp_cnt;
    const unsigned long stream_unit_bytes;
    int *superp_fd;
    unsigned long *disk_pos;
    bool eof_flag;
    unsigned long *disk_bytes;
    unsigned long request_superp;
    unsigned long request_offset;
    buffer_manager *bufm;
    unsigned char **disk_pages;
    disk_stream *workq_next;
    ioq* io_queue;
    bool write_op;
    volatile bool flush_complete;
    bool one_shot;
    bool compressed;
    memory_buffer *request;
    memory_buffer *cache;
    flip_buffer **flip_buffers;
    z_stream **zlib_streams;
    bool *zlib_eof;
    bool z_inflate_mode;
    unsigned long max_io_unit;

    bool eof(unsigned long superp)
    {
      return (disk_pos[superp] == disk_bytes[superp]); 
    }

    void set_zlib_inflate(bool init = false)
    {
      if(!z_inflate_mode) {
	for(unsigned long i=0;i<superp_cnt;i++) {
	  z_stream* strm = zlib_streams[i];
	  if(!init) 
	    deflateEnd(strm);
	  strm->zalloc = Z_NULL;
	  strm->zfree = Z_NULL;
	  strm->opaque = Z_NULL;
	  strm->avail_in = 0;
	  strm->next_in = Z_NULL;
	  int ret = inflateInit(strm);
	  if (ret != Z_OK) {
	    BOOST_LOG_TRIVIAL(fatal) << "Unable to initialize zlib stream:"
				     <<"(" << ret << ")" << zerr(ret);
	    exit(-1);
	  }
	  zlib_eof[i] = false;
	  init_flip_buffer(flip_buffers[i], superp_fd[i]);
	}
      }
      z_inflate_mode = true;
    }
    
    void set_zlib_deflate(bool init = false)
    {
      if(z_inflate_mode) {
	for(unsigned long i=0;i<superp_cnt;i++) {
	  z_stream* strm = zlib_streams[i];
	  init_flip_buffer(flip_buffers[i], superp_fd[i]);
	  if(!init)
	    inflateEnd(strm);
	  strm->zalloc = Z_NULL;
	  strm->zfree = Z_NULL;
	  strm->opaque = Z_NULL;
	  strm->avail_in = 0;
	  strm->next_in = Z_NULL;
	  strm->avail_out = max_io_unit;
	  strm->next_out  = flip_buffers[i]->ready;
	  int ret = deflateInit(strm, ZLIB_COMPRESSION_LEVEL);
	  if (ret != Z_OK) {
	    BOOST_LOG_TRIVIAL(fatal) << "Unable to initialize zlib stream" <<
	      "(" << ret << ")" << zerr(ret);
	    exit(-1);
	  }
	}
      }
      z_inflate_mode = false;
    }

  public:
    bool stream_eof(unsigned long superp)
    {
      if(cache != NULL) {
	return eof_flag;
      }
      else if(request != NULL && request_superp == superp) {
	return false;
      }
      else {
	return eof(superp) && (!compressed || zlib_eof[superp]);
      }
    }

    bool disk_empty()
    {
      for(unsigned long i=0;i<superp_cnt;i++) {
	if(disk_bytes[i] > 0) {
	  return false;
	}
      }
      return true;
    }

    bool is_empty()
    {
      return (cache == NULL) && (request == NULL) && disk_empty();
    }
    
    disk_stream(const char * fname,
		bool new_file,
		bool is_compressed,
		ioq *io_queue_in,
		buffer_manager *bufm_in,
		unsigned long superp_cnt_in,
		unsigned long stream_unit_bytes_in,
		unsigned long max_io_unit_in)
      :superp_cnt(superp_cnt_in),
       stream_unit_bytes(stream_unit_bytes_in),
       eof_flag(false),
       request_superp(-1),
       bufm(bufm_in),
       workq_next(NULL),
       io_queue(io_queue_in),
       write_op(false),
       flush_complete(true),
       one_shot(false),
       compressed(is_compressed),
       request(NULL),
       cache(NULL),
       max_io_unit(max_io_unit_in)
    {
      superp_fd    = new int[superp_cnt];
      disk_pos     = new unsigned long[superp_cnt];
      disk_bytes   = new unsigned long[superp_cnt];
      disk_pages   = new unsigned char *[superp_cnt];
      flip_buffers = new flip_buffer *[superp_cnt];
      zlib_streams = new z_stream *[superp_cnt];
      zlib_eof     = new bool[superp_cnt];
      for(unsigned long i=0;i<superp_cnt;i++) {
	disk_pages[i] = (unsigned char *)
	  map_anon_memory(DISK_PAGE_SIZE, true, "Disk page");
	// open the fds
	std::stringstream full_name;
	full_name << io_queue->get_mnt_pt() << "/"; 
	if(i==0) {
	  full_name << fname;
	}
	else {
	  full_name << fname << "." << i;
	}
	if(vm.count("no_dio") > 0) {
	  superp_fd[i] = open(full_name.str().c_str(),
			      O_RDWR|O_LARGEFILE|
			      (new_file ? (O_CREAT|O_TRUNC): 0),
			      S_IRWXU);
	}
	else {
	  superp_fd[i] = open(full_name.str().c_str(),
			      O_RDWR|O_LARGEFILE|O_DIRECT|
			      (new_file ? (O_CREAT|O_TRUNC): 0),
			      S_IRWXU);
	}
	if(superp_fd[i] == -1) {
	  BOOST_LOG_TRIVIAL(fatal) << "Unable to open stream file:" <<
	    strerror(errno);
	  exit(-1);
	}
	disk_pos[i]   = 0;
	if(new_file) {
	  disk_bytes[i] = 0;
	}
	else {
	  disk_bytes[i] = get_file_size(superp_fd[i]);
	}
	if(compressed) {
	  flip_buffers[i] = new flip_buffer();
	  memset(&flip_buffers[i]->ctx, 0, sizeof(aiocb));
	  flip_buffers[i]->ready = (unsigned char *)
	    map_anon_memory(max_io_unit, true, "zlib buffer");
	  flip_buffers[i]->pending = (unsigned char *)
	    map_anon_memory(max_io_unit, true, "zlib buffer");
	  zlib_streams[i] = new z_stream();
	}
	else {
	  flip_buffers[i] = NULL;
	  zlib_streams[i] = NULL;
	  zlib_eof[i]     = false;
	}
      }
      if(compressed) {
	if(new_file) {
	  z_inflate_mode = true;
	  set_zlib_deflate(true);
	}
	else {
	  z_inflate_mode = false;
	  set_zlib_inflate(true);
	}
      }
    }
    
    memory_buffer* read(unsigned long superp)
    {
      memory_buffer* result;
      if(cache != NULL) {
	BOOST_ASSERT_MSG(request_superp != -1UL, "Invalid request_superp");
	eof_flag = true;
	cache->set_refcount(cache->get_refcount() + 1);
	return cache;
      }
      // Allocate if need be
      unsigned long current_pos = disk_pos[superp];
      if(request == NULL || request_superp != superp) {
	if(request) {
	  request->wait_uptodate();
	  request->set_refcount(request->get_refcount() - 1);
	}
	request = bufm->get_buffer();
	request_superp = superp;
	request->uptodate = false;
	write_op = false;
	io_queue->add_work(this);
      }
      result = request;
      result->wait_uptodate();
      // Issue readahead and return result
      if(!eof(superp)) {
	request = bufm->get_buffer();
	request->uptodate = false;
	write_op = false;
	io_queue->add_work(this);
      }
      else if(superp_cnt == 1 && current_pos == 0) { // Add to cache
	BOOST_ASSERT_MSG(cache == NULL, "Cache not empty !");
	cache = request;
	eof_flag = true;
	cache->set_refcount(cache->get_refcount() + 1);
	request = NULL;
      }
      else {
	request = NULL;
      }
      return result;
    }
    
    void read_one_shot(memory_buffer *buffer,
		       unsigned long superp)
    {
      one_shot = true;
      request = buffer;
      request_superp = superp;
      request->uptodate = false;
      write_op = false;
      io_queue->add_work(this);
      // Note: asynchronous read
    }
    
    void append(memory_buffer *buffer)
    {
      if(buffer->bufsize == 0) {
	// Nothing to do
	buffer->set_refcount(buffer->get_refcount() - 1);
	return;
      }
      buffer->dirty = true;
      if(request != NULL) {
	request->wait_uptodate();
      }
      // Special treatment for in-memory streams
      if(disk_empty() && cache == NULL && superp_cnt == 1) {
	cache = buffer;
	BOOST_ASSERT_MSG(request == NULL, "BUG: empty file after append");
      }
      else {
	if(cache != NULL) {
	  request = cache;
	  request->uptodate = false;
	  write_op = true;
	  io_queue->add_work(this);
	  request->wait_uptodate();
	  request->set_refcount(request->get_refcount() - 1);
	  request = NULL;
	  cache = NULL;
	}
	else if(request != NULL) {
	  request->wait_uptodate();
	  request->set_refcount(request->get_refcount() - 1);
	}
	request = buffer;
	request->uptodate = false;
	write_op = true;
	io_queue->add_work(this);
      }
      BOOST_ASSERT_MSG(cache == NULL || request == NULL,
		       "Cached block while request pending !");
    }

    void write_one_shot(memory_buffer *buffer,
			unsigned long superp)
    {
      one_shot = true;
      request = buffer;
      request_superp = superp;
      request->uptodate = false;
      request->dirty = true;
      write_op = true;
      io_queue->add_work(this);
      //Note: asynchronous write
    }

    void ingest(memory_buffer *ingest_buffer)
    {
      request_superp = -1;
      if(is_empty()){
	append(ingest_buffer);
	return;
      }
      unsigned long bytes = ingest_buffer->bufsize;
      if(cache != NULL) {
	for(unsigned long i=0;i<cache->config->processors;i++) {
	  cache->reset_index(i);
	  ingest_buffer->reset_index(i);
	}
	unsigned long space = 
	  MIN(cache->bufbytes - cache->bufsize, bytes);
	space = (space/stream_unit_bytes)*stream_unit_bytes;
	if(space > 0) {
	  BOOST_ASSERT_MSG(ingest_buffer->bufsize % stream_unit_bytes == 0,
			   "Ingest bufsize not a multiple of object size");
	  // Copy from end
	  memcpy(cache->buffer + cache->bufsize, 
		 ingest_buffer->buffer + (ingest_buffer->bufsize - space), 
		 space);
	  ingest_buffer->bufsize -= space;
	  bytes -= space;
	}
	ingest_buffer->set_flat_index();
	if(bytes > 0) {
	  cache->set_flat_index();
	  request = cache;
	  request->uptodate = false;
	  write_op = true;
	  io_queue->add_work(this);
	  request->wait_uptodate();
	  request->set_refcount(request->get_refcount() - 1);
	  request = NULL;
	  cache = NULL;
	}
	else {
	  ingest_buffer->set_refcount(ingest_buffer->get_refcount() - 1);
	  return;
	}
      }
      BOOST_ASSERT_MSG(request == NULL, "Stream not at end when ingesting");
      for(unsigned long i=0;i<superp_cnt;i++) {
	disk_pos[i] = disk_bytes[i];
	set_filepos(superp_fd[i], (disk_pos[i]/DISK_PAGE_SIZE)*DISK_PAGE_SIZE);
      }
      append(ingest_buffer);
    }

    void rewind(bool drop_cache = false)
    {
      // Note: request_superp == -1 means we were in append mode
      eof_flag = false;
      if(cache == NULL) {
	if(request != NULL) {
	  request->wait_uptodate();
	  request->set_refcount(request->get_refcount() - 1);
	  request = NULL;
	}
	for(unsigned long i=0;i<superp_cnt;i++) {
	  // Note: must always flush on compressed to write out checksum
	  if(request_superp == -1UL && 
	     ((compressed && zlib_streams[i]->total_in > 0) || 
	      (disk_pos[i] > 0 && disk_bytes[i] > 0))) {
	    flush_complete = false;
	    write_op = true;
	    io_queue->add_work(this);
	    while(!flush_complete);
	  }
	  rewind_file(superp_fd[i]);
	  disk_pos[i] = 0;
	}
	request_superp = -1;
      }
      else {
	BOOST_ASSERT_MSG(request == NULL,
			 "Request pending while block cached !");
	if(request_superp == -1UL) {
	  for(unsigned long i=0;i<cache->config->processors;i++) {
	    cache->reset_index(i);
	  }
	  cache->indexed = false;
	  request_superp = 0; // valid cache
	}
	if(drop_cache) {
	  cache->set_refcount(cache->get_refcount() - 1);
	  cache = NULL;
	  request_superp = -1;
	}
      }
    }

    void reset(unsigned long superp)
    {
      request_superp = -1UL;
      eof_flag = false;
      if(cache != NULL) {
	cache->dirty = false;
	cache->set_refcount(cache->get_refcount() - 1);
	cache = NULL;
	BOOST_ASSERT_MSG(request == NULL,
			 "Request pending while block cached !");
      }
      else if(request != NULL) {
	request->wait_uptodate();
	request->set_refcount(request->get_refcount() - 1);
	request = NULL;
      }
      
      disk_pos[superp] = 0;
      if(disk_bytes[superp] > 0) {
	rewind_file(superp_fd[superp]);
	truncate_file(superp_fd[superp]);
	disk_bytes[superp] = 0;
      }
    }
    friend class ioq;
    friend class disk_io;
  };

}
#endif
