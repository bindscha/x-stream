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

#ifndef _DISK_IO_
#define _DISK_IO_
#include "split_stream.hpp"
#include<boost/thread.hpp>
#include<boost/thread/locks.hpp>
#include<boost/thread/condition_variable.hpp>

#include <stdio.h>
#include <string.h>
#include <assert.h>

namespace x_lib {
  extern ioq io_requests;

  static bool is_aligned(unsigned char *buffer)
  {
    return (((unsigned long)buffer) & (DISK_PAGE_SIZE - 1)) == 0;
  }
  
  class disk_io {
    unsigned long stream_unit;
    unsigned char *bounce_buffer;
    ioq *wq;

    void reset_inflate_state(z_stream *strm)
    {
      inflateEnd(strm);
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
    }

    void reset_deflate_state(z_stream *strm, unsigned char *buffer)
    {
      deflateEnd(strm);
      strm->zalloc = Z_NULL;
      strm->zfree = Z_NULL;
      strm->opaque = Z_NULL;
      strm->avail_in = 0;
      strm->next_in = Z_NULL;
      strm->avail_out = stream_unit;
      strm->next_out  = buffer;
      int ret = deflateInit(strm, ZLIB_COMPRESSION_LEVEL);
      if (ret != Z_OK) {
	BOOST_LOG_TRIVIAL(fatal) << "Unable to initialize zlib stream" <<
	      "(" << ret << ")" << zerr(ret);
	exit(-1);
      }
    }

    void do_read_IO(int fd,
		    unsigned char *buffer,
		    unsigned char *disk_page,
		    unsigned long bytes,
		    unsigned long disk_buffer_pos)
    {
      unsigned long disk_page_offset;
      unsigned long disk_page_bytes;
      disk_page_offset = disk_buffer_pos % DISK_PAGE_SIZE;
      if(disk_page_offset > 0) {
	disk_page_bytes = DISK_PAGE_SIZE - disk_page_offset;
      }
      else {
	disk_page_bytes = 0;
      }
      unsigned long buffer_offset = 0;
      // Read from disk page
      if(disk_page_bytes > 0) {
	unsigned long copy_bytes = MIN(bytes, disk_page_bytes);
	memcpy(buffer + buffer_offset, disk_page + disk_page_offset,
	       copy_bytes);
	bytes -= copy_bytes; 
	buffer_offset += copy_bytes;
	if(bytes == 0) {
	  return;
	}
      }
      // Read from disk
      unsigned long io_size = stream_unit;
      while(io_size >= DISK_PAGE_SIZE) {
	if(io_size > bytes) {
	  io_size = io_size/2;
	  continue;
	}
	if(is_aligned(buffer + buffer_offset)) {
	  read_from_file(fd, buffer + buffer_offset, io_size);
	}
	else {
	  read_from_file(fd, bounce_buffer, io_size);
	  memcpy(buffer + buffer_offset, bounce_buffer, io_size);
	}
	buffer_offset += io_size;
	bytes -= io_size;
      }
      if(bytes) {
	read_from_file_atomic(fd, disk_page, DISK_PAGE_SIZE);
	memcpy(buffer + buffer_offset, disk_page, bytes);
      }
    }

    void do_write_IO(int fd, 
		     unsigned char *buffer, 
		     unsigned long buffer_bytes,
		     unsigned long disk_buffer_pos,
		     unsigned char *disk_page)
    {
      unsigned long disk_page_offset = disk_buffer_pos % DISK_PAGE_SIZE;
      unsigned long page_bytes_left;
      if(disk_page_offset > 0) {
	page_bytes_left = DISK_PAGE_SIZE - disk_page_offset;
      }
      else {
	page_bytes_left = 0;
      }
      unsigned long bytes_to_copy;
      if(page_bytes_left > 0) {
	bytes_to_copy = MIN(page_bytes_left, buffer_bytes);
	memcpy(disk_page + disk_page_offset,
	       buffer, bytes_to_copy);
	buffer += bytes_to_copy;
	buffer_bytes -= bytes_to_copy;
	page_bytes_left -= bytes_to_copy;
	if(page_bytes_left == 0) {
	  write_to_file(fd, disk_page, DISK_PAGE_SIZE);
	}
	if(buffer_bytes == 0) {
	  return;
	}
      }
      unsigned long io_size = stream_unit;
      while(io_size >= DISK_PAGE_SIZE) {
	if(io_size > buffer_bytes) {
	  io_size  = io_size/2;
	  continue;
	}
	if(is_aligned(buffer)) {
	  write_to_file(fd, buffer, io_size);
	}
	else {
	  memcpy(bounce_buffer, buffer, io_size);
	  write_to_file(fd, bounce_buffer, io_size);
	}
	buffer_bytes -= io_size;
	buffer += io_size;
      }
      if(buffer_bytes > 0) {
	memcpy(disk_page, buffer, buffer_bytes);
      }
    }

    void read_operator(disk_stream *inflight)
    {
      if(inflight->one_shot) {
	rewind_file(inflight->superp_fd[inflight->request_superp]);
	read_from_file(inflight->superp_fd[inflight->request_superp],
		       inflight->request->buffer,
		       inflight->request->bufsize);
	inflight->one_shot = false;
	inflight->request->uptodate = true;
	return;
      }
      unsigned long disk_buffer_pos;
      unsigned long bytes;
      disk_buffer_pos = inflight->disk_pos[inflight->request_superp];
      inflight->request->set_bufsize
	(inflight->disk_bytes[inflight->request_superp] -
	 inflight->disk_pos[inflight->request_superp],
	 inflight->stream_unit_bytes);
      bytes = inflight->request->bufsize;
      inflight->disk_pos[inflight->request_superp] += bytes;
      BOOST_ASSERT_MSG(inflight->disk_pos[inflight->request_superp] 
		       <= inflight->disk_bytes[inflight->request_superp],
		       "Trying to read past end of stream !");
      do_read_IO(inflight->superp_fd[inflight->request_superp],
		 inflight->request->buffer,
		 inflight->disk_pages[inflight->request_superp],
		 bytes,
		 disk_buffer_pos);
      inflight->request->uptodate = true;
    }

    void read_and_decompress_operator(disk_stream *inflight)
    {
      z_stream *zlib_stream = inflight->zlib_streams[inflight->request_superp];
      int ret;
      inflight->set_zlib_inflate();
      if(inflight->one_shot) {
	rewind_file(inflight->superp_fd[inflight->request_superp]);
      }
      else {
	inflight->request->set_bufsize
	  (ULONG_MAX, inflight->stream_unit_bytes);
      }
      zlib_stream->next_out  = inflight->request->buffer;
      zlib_stream->avail_out = inflight->request->bufsize;
      while(zlib_stream->avail_out) {
	if(zlib_stream->avail_in == 0) {
	  unsigned long bytes;
	  flip_buffer * fb = inflight->flip_buffers[inflight->request_superp];
	  do {
	    unsigned long disk_buffer_pos =
	      inflight->disk_pos[inflight->request_superp];
	    unsigned long avail = 
	      inflight->disk_bytes[inflight->request_superp] -
	      inflight->disk_pos[inflight->request_superp];
	    if(avail == 0) {
	      bytes = 0;
	    }
	    else {
	      bytes = stream_unit;
	      while(bytes > avail) {
		if(bytes == DISK_PAGE_SIZE) {
		  break;
		}
		bytes = bytes/2;
	      }
	      if(bytes <= avail) {
		inflight->disk_pos[inflight->request_superp] += bytes;
	      }
	      else {
		inflight->disk_pos[inflight->request_superp] += avail;
	      }
	    }
	    do_flip_IO(fb, disk_buffer_pos, bytes, true); 
	  } while(fb->ready_bytes == 0 && bytes != 0);
	  zlib_stream->avail_in = fb->ready_bytes;
	  zlib_stream->next_in = fb->ready;
	}
	ret = inflate(zlib_stream, Z_NO_FLUSH);
	if(ret == Z_STREAM_END) {
	  inflight->zlib_eof[inflight->request_superp] = true;
	  init_flip_buffer(inflight->flip_buffers[inflight->request_superp],
			   inflight->superp_fd[inflight->request_superp]);
	  reset_inflate_state(zlib_stream);
	  break;
	}
	else if(ret == Z_BUF_ERROR) {
	  BOOST_LOG_TRIVIAL(fatal) 
	    << "Trying to read past end of compressed stream !"
	    << " disk bytes " 
	    << inflight->disk_bytes[inflight->request_superp]
	    << " disk pos  " 
	    << inflight->disk_pos[inflight->request_superp]
	    << " buffer bytes "
	    << zlib_stream->avail_in
	    << " output bytes "
	    << zlib_stream->avail_out;
	  exit(-1);
	}
	else if(ret != Z_OK) {
	  BOOST_LOG_TRIVIAL(fatal) << "Decompression error:(" << ret << ")" <<
	    zerr(ret);
	  exit(-1);
	}
      }
      inflight->request->bufsize -= zlib_stream->avail_out;
      if(inflight->one_shot) {
	inflight->one_shot = false;
      }
      inflight->request->uptodate = true;
      return;
    }

    void squeeze(disk_stream *inflight, unsigned long superp)
    {
      z_stream *zlib_stream =  inflight->zlib_streams[superp];
      int ret;
      unsigned long disk_buffer_pos;
      while(zlib_stream->avail_in) {
	ret = deflate(zlib_stream, Z_NO_FLUSH);
	if(ret != Z_OK) {
	  BOOST_LOG_TRIVIAL(fatal) << "Compression failure:("
				   << ret << ")"
				   << zerr(ret);
	  exit(-1);
	}
	if(zlib_stream->avail_out == 0) {
	  disk_buffer_pos = inflight->disk_pos[superp];
	  inflight->disk_pos[superp]   += stream_unit;
	  inflight->disk_bytes[superp] += stream_unit;
	  do_flip_IO(inflight->flip_buffers[superp],
		     disk_buffer_pos,
		     stream_unit,
		     false);
	  zlib_stream->avail_out = stream_unit;
	  zlib_stream->next_out = inflight->flip_buffers[superp]->ready;
	}
      }
    }

    void zlib_stream_flush(disk_stream *inflight,
			   unsigned long superp)
    {
      z_stream *zlib_stream =  inflight->zlib_streams[superp];
      int ret;
      unsigned long disk_buffer_pos;
      zlib_stream->avail_in  = 0;
      zlib_stream->next_in   = NULL; 
      do {
	ret = deflate(zlib_stream, Z_FINISH);
	if(ret != Z_STREAM_END && ret != Z_OK) {
	  BOOST_LOG_TRIVIAL(fatal) << "Compression flush failure:(" 
				   << ret << ")"
				   << zerr(ret) 
				   << " processed so far "
				   << zlib_stream->total_in;
	  exit(-1);
	}
	unsigned long bytes = stream_unit - zlib_stream->avail_out;
	disk_buffer_pos = inflight->disk_pos[superp];
	inflight->disk_pos[superp]   += bytes;	
	inflight->disk_bytes[superp] += bytes;
	do_flip_IO(inflight->flip_buffers[superp],
		   disk_buffer_pos,
		   ((bytes + DISK_PAGE_SIZE - 1)/DISK_PAGE_SIZE)*DISK_PAGE_SIZE,
		   false);
	zlib_stream->next_out  = inflight->flip_buffers[superp]->ready;
	zlib_stream->avail_out = stream_unit;
	if(ret == Z_STREAM_END) {
	  break;
	}
      } while(1);
      init_flip_buffer(inflight->flip_buffers[superp],
		       inflight->superp_fd[superp]);
      reset_deflate_state(zlib_stream, inflight->flip_buffers[superp]->ready);
    }

    void compress_and_write_internal(disk_stream *inflight)
    {
      z_stream *zlib_stream;
      BOOST_ASSERT_MSG(inflight->request->dirty,
		       "Trying to write out clean buffer !");
      inflight->set_zlib_deflate();
      if(inflight->one_shot) {
	rewind_file(inflight->superp_fd[inflight->request_superp]);
	zlib_stream = inflight->zlib_streams[inflight->request_superp];
	zlib_stream->next_in  = inflight->request->buffer;
	zlib_stream->avail_in = inflight->request->bufsize; 
	squeeze(inflight, inflight->request_superp);
	flush_one(inflight, inflight->request_superp);
      }
      else {
	for(unsigned long i=0;i<inflight->superp_cnt;i++) {
	  if(i == inflight->request->skip_superp) {
	    inflight->request->skip_superp = -1UL;
	    continue;
	  }
	  for(unsigned long j=0;j<inflight->request->config->processors;j++) {
	    unsigned long buffer_bytes;
	    // Extract the buffer and squeeze out
	    inflight->zlib_streams[i]->next_in =
	      inflight->request->get_substream
	      (j, i, &buffer_bytes);
	    inflight->zlib_streams[i]->avail_in = buffer_bytes;
	    if(buffer_bytes > 0) {
	      squeeze(inflight, i);
	    }
	  }
	}
      }
    }
    
    void write_internal(disk_stream *inflight)
    {
      BOOST_ASSERT_MSG(inflight->request->dirty,
		       "Trying to write out clean buffer !");
      
      // Special case: one shot
      if(inflight->one_shot) {
	rewind_file(inflight->superp_fd[inflight->request_superp]);
	write_to_file(inflight->superp_fd[inflight->request_superp],
		      inflight->request->buffer, 
		      inflight->request->bufsize);
	inflight->one_shot = false;
	inflight->request->dirty = false;
	inflight->request->uptodate = true;
	return;
      }
      for(unsigned long i=0;i<inflight->superp_cnt;i++) {
       	if(i == inflight->request->skip_superp) {
	  inflight->request->skip_superp = -1UL;
	  continue;
	}
	for(unsigned long j=0;j<inflight->request->config->processors;j++) {
	  unsigned char *buffer;
	  unsigned long buffer_bytes;
	  // Extract the buffer
	  buffer = inflight->request->get_substream(j, i, &buffer_bytes);
	  // Do the write
	  unsigned long disk_buffer_pos = inflight->disk_pos[i];
	  inflight->disk_pos[i]   += buffer_bytes;
	  inflight->disk_bytes[i] += buffer_bytes; 
	  do_write_IO(inflight->superp_fd[i], 
		      buffer, 
		      buffer_bytes,
		      disk_buffer_pos,
		      inflight->disk_pages[i]);
	}
      }
    }

    void flush_one(disk_stream *inflight,
		   unsigned long superp)
    {
      if(inflight->compressed) {
	zlib_stream_flush(inflight, superp);
      }
      else {
	unsigned long disk_buffer_pos = inflight->disk_pos[superp];
	unsigned long disk_page_offset = disk_buffer_pos % DISK_PAGE_SIZE;
	if(disk_page_offset > 0) {
	  write_to_file(inflight->superp_fd[superp],
			inflight->disk_pages[superp],
			DISK_PAGE_SIZE);
	}
      }
    }

    void flush(disk_stream *inflight)
    {
      for(unsigned long i=0;i<inflight->superp_cnt;i++) {
	flush_one(inflight, i);
      }
    }

    void write_operator(disk_stream *inflight)
    {
      if(inflight->request == NULL) {
	flush(inflight);
	inflight->flush_complete = true;
      }
      else {
	if(inflight->compressed) {
	  compress_and_write_internal(inflight);
	}
	else {
	  write_internal(inflight);
	}
	inflight->request->dirty    = false;
	inflight->request->uptodate = true;
	inflight->flush_complete    = true;
      }
    }
    
  public:
    disk_io(unsigned long stream_unit_in, ioq *wq_in)
      :stream_unit(stream_unit_in),
       wq(wq_in)
    {
      bounce_buffer = (unsigned char *) 
	map_anon_memory(stream_unit, true, "Bounce buffer");
    }
    
    void operator() ()
    {
      while(1) {
	disk_stream *inflight = wq->get_work();
	if(inflight == NULL) {
	  break;
	}
	if(inflight->write_op) {
	  write_operator(inflight);
	}
	else {
	  if(inflight->compressed) {
	    read_and_decompress_operator(inflight);
	  }
	  else {
	    read_operator(inflight);
	  }
	}
      }
    }

  };
}
#endif
