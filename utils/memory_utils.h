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

#ifndef _MEMORY_UTILS_
#define _MEMORY_UTILS_
#include<sys/types.h>
#include<unistd.h>
#include<sys/stat.h>
#include<fcntl.h>
#include<sys/mman.h>
#include<stdio.h>
#include<errno.h>
#include<string.h>
#include<stdlib.h>
#include<zlib.h>
#include<aio.h>
#include "boost_log_wrapper.h"

extern unsigned long stat_bytes_read;
extern unsigned long stat_bytes_written;

static void memory_utils_err(const char *op, const char *subop)
{
  BOOST_LOG_TRIVIAL(error) << op << "::" << subop << " failed:" <<
    strerror(errno);
}

static char* get_current_wd()
{
  return getcwd(NULL, 0);
}

//! Used to allocate *seriously large* amounts of 
//! zeroed out memory
static void *map_anon_memory(unsigned long size, 
			     bool mlocked,
			     const char *operation,
			     bool zero = false)
{
  void *space = mmap(NULL, size > 0 ? size:4096, 
		     PROT_READ|PROT_WRITE,
		     MAP_ANONYMOUS|MAP_SHARED, -1, 0);
  if(space == MAP_FAILED) {
    memory_utils_err(operation, "mmap");
    exit(-1);
  }
  if(mlocked) {
    if(mlock(space, size) < 0) {
      memory_utils_err(operation, "mlock");
    }
  }
  if(zero) {
    memset(space, 0, size);
  }
  return space;
}

/* 
 * maps fsize bytes from file
 */
static void* map_file_memory(int fd,
			     const char *operation,
			     unsigned long fsize,
			     bool mlocked,
			     bool read_only)
{
  void *mapped_file;
  mapped_file = mmap(0, fsize, read_only ? (PROT_READ):(PROT_READ|PROT_WRITE),
		     MAP_FILE|MAP_SHARED, fd, 0);
  if(mapped_file == MAP_FAILED) {
    memory_utils_err(operation, "mmap");
    exit(-1);
  }
  if(mlocked) {
    if(mlock(mapped_file, fsize) < 0) {
      memory_utils_err(operation, "mlock");
    }
  }
  return mapped_file;
}

static void truncate_file(int fd)
{
  if(ftruncate(fd, 0)) {
    BOOST_LOG_TRIVIAL(fatal) << "file truncate failed:" << strerror(errno);
    exit(-1);
  }
}

static void check_lseek_result(off_t result)
{
  if(result == (off_t)-1) {
    BOOST_LOG_TRIVIAL(fatal) << "lseek failed:" << strerror(errno);
    exit(-1);
  }
}

static unsigned long get_file_size(int fd)
{
  off_t fsize, cpos;
  cpos  = lseek(fd, 0, SEEK_CUR);
  check_lseek_result(cpos);
  fsize = lseek(fd, 0, SEEK_END);
  check_lseek_result(fsize);
  cpos  = lseek(fd, cpos, SEEK_SET);
  check_lseek_result(cpos);
  return fsize;
}

static void rewind_file(int fd)
{
  unsigned long cpos  = lseek(fd, 0, SEEK_SET);
  check_lseek_result(cpos);
}

static void set_filepos(int fd, unsigned long pos)
{
  unsigned long cpos  = lseek(fd, pos, SEEK_SET);
  check_lseek_result(cpos);
}

static void write_to_file(int fd, 
			  unsigned char *output,
			  unsigned long bytes_to_write) 
{
  __sync_fetch_and_add(&stat_bytes_written, bytes_to_write);
  while(bytes_to_write) {
    unsigned long bytes_written = write(fd, output, bytes_to_write);
    if(bytes_written == -1UL) {
      if(errno != EAGAIN) {
	BOOST_LOG_TRIVIAL(fatal) << 
	  "Stream writeout unsuccessful:" << strerror(errno);
	exit(-1);
      }
    }
    else {
      output += bytes_written;
      bytes_to_write -= bytes_written;
    }
  }
}

static void read_from_file(int fd, 
			   unsigned char *input,
			   unsigned long bytes_to_read) 
{
  __sync_fetch_and_add(&stat_bytes_read, bytes_to_read);
  while(bytes_to_read) {
    unsigned long bytes_read = read(fd, input, bytes_to_read);
    if(bytes_read == -1UL) {
      if(errno != EAGAIN) {
	BOOST_LOG_TRIVIAL(fatal) << 
	  "Stream readin unsuccessful:" << strerror(errno);
	exit(-1);
      }
    }
    else if(bytes_read == 0) {
      // Note: silently return if EOF
      break;
    }
    else {
      input += bytes_read;
      bytes_to_read -= bytes_read;
    }
  }
}

// Assume we can read in one shot
static void read_from_file_atomic(int fd, 
				  unsigned char *input,
				  unsigned long bytes_to_read) 
{
  __sync_fetch_and_add(&stat_bytes_read, bytes_to_read);
  while(bytes_to_read) {
    unsigned long bytes_read = read(fd, input, bytes_to_read);
    if(bytes_read == -1UL) {
      if(errno != EAGAIN) {
	BOOST_LOG_TRIVIAL(fatal) << 
	  "Stream readin unsuccessful:" << strerror(errno);
	exit(-1);
      }
    }
    else {
      // Note: assume read is successful or EOF
      bytes_to_read = 0; // break
    }
  }
}

/* report a zlib error */
static const char* zerr(int ret)
{
    fputs("zpipe: ", stderr);
    switch (ret) {
    case Z_STREAM_ERROR:
        return "invalid compression level";
    case Z_DATA_ERROR:
         return "invalid or incomplete deflate data";
    case Z_MEM_ERROR:
        return "out of memory";
    case Z_VERSION_ERROR:
        return "zlib version mismatch";
    default:
        return "unknown zline error ";
    }
}

struct flip_buffer {
  unsigned char *ready;
  unsigned char *pending;
  struct aiocb ctx;
  unsigned long ready_bytes;
  unsigned long pending_bytes;
};


static void do_flip_IO(flip_buffer *buf, 
		       unsigned long offset,
		       unsigned long bytes,
		       bool read)
{
  int ret;
  if(buf->pending_bytes != 0) {
    while((ret = aio_error(&buf->ctx)) == EINPROGRESS);
    buf->pending_bytes = aio_return( &buf->ctx );
    if(ret > 0) {
      BOOST_LOG_TRIVIAL(warn) << "Asynchronous I/O failed !";
      errno = aio_error(&buf->ctx);
      memory_utils_err("aio", "complete");
      exit(-1);
    }
  }
  unsigned char *tmp;
  tmp = buf->ready;
  buf->ready = buf->pending;
  buf->ready_bytes = buf->pending_bytes;
  buf->pending = tmp;
  buf->pending_bytes = bytes;
  // Setup and issue aio
  if(bytes > 0) {
    buf->ctx.aio_nbytes = bytes;
    buf->ctx.aio_offset = offset;
    buf->ctx.aio_buf    = buf->pending;
    if(read) {
      ret = aio_read(&buf->ctx);
    }
    else {
      ret = aio_write(&buf->ctx);
    }
    if(ret < 0) {
      BOOST_LOG_TRIVIAL(warn) << "Asynchronous I/O failed !";
      errno = aio_error(&buf->ctx);
      memory_utils_err("async", "issue");
      exit(-1);
    }
  }
}
		  
static void init_flip_buffer(flip_buffer *buf, int fd)
{
  int ret;
  while((ret = aio_error(&buf->ctx)) == EINPROGRESS);
  if(ret > 0) {
    BOOST_LOG_TRIVIAL(warn) << "Asynchronous I/O failed !";
    errno = aio_error(&buf->ctx);
    memory_utils_err("aio", "complete");
    exit(-1);
  }
  buf->ready_bytes     = 0;
  buf->pending_bytes   = 0;
  buf->ctx.aio_fildes = fd;
}



#endif
