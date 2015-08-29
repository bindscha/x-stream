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

#include <fcntl.h>
#include <unistd.h>
#include <stdlib.h>
#include <errno.h>
#include <cstdio>
#include <cassert>
#include <boost/thread/locks.hpp>
#include "output.h"

edge_buffer::edge_buffer(const size_t size, const threadid_t owner)
  : m_pos(0), m_owner(owner)
{
  void* ptr;
  if (0 != posix_memalign(&ptr, DEFAULT_BLOCK_SIZE, size)) {
    fprintf(stderr, "Error: posix_memalign call failed with errno %d\n", errno);
    exit(1);
  }
  m_data = (struct edge_struct*)ptr;
  m_size = size / sizeof(struct edge_struct);
}

edge_buffer::~edge_buffer()
{
  free(m_data);
}

// -----------------------------------------------------

void buffer_queue::enqueue(edge_buffer* pbuffer)
{
  boost::unique_lock<boost::mutex> lock(m_mutex);
  m_data.push(pbuffer);
  m_cond.notify_one();
}

// This method can return null only if interrupted
// by calling signal()
edge_buffer* buffer_queue::dequeue()
{
  boost::unique_lock<boost::mutex> lock(m_mutex);
  if (m_data.empty()) {
    m_cond.wait(lock);
  }

  edge_buffer* pbuffer = NULL;
  if (!m_data.empty()) {
    pbuffer = m_data.front();
    m_data.pop();
  }
  return pbuffer;
}

edge_buffer* buffer_queue::try_dequeue()
{
  edge_buffer* pbuffer = NULL;
  if (!m_data.empty()) {
    pbuffer = m_data.front();
    m_data.pop();
  }
  return pbuffer;
}

// This method can return null only if interrupted
// by calling signal()
edge_buffer* buffer_queue::front()
{
  boost::unique_lock<boost::mutex> lock(m_mutex);
  if (m_data.empty()) {
    m_cond.wait(lock);
  }

  edge_buffer* pbuffer = NULL;
  if (!m_data.empty()) {
    pbuffer = m_data.front();
  }
  return pbuffer;
}

edge_buffer* buffer_queue::try_front()
{
  edge_buffer* pbuffer = NULL;
  if (!m_data.empty()) {
    pbuffer = m_data.front();
  }
  return pbuffer;
}

void buffer_queue::signal()
{
  m_cond.notify_one();
}

// -----------------------------------------------------

thread_buffer::thread_buffer(buffer_queue* pflushq)
  : m_pflushq(pflushq)
{}

thread_buffer::~thread_buffer()
{
  while (edge_buffer* pbuffer = m_queue.try_dequeue()) {
    delete pbuffer;
  }
}

struct edge_struct* thread_buffer::edge_struct()
{
  // Queue can never be empty here
  edge_buffer* pbuffer = m_queue.try_front();
  assert(pbuffer);

  if (pbuffer->m_pos == pbuffer->m_size) {
    m_queue.try_dequeue();
    m_pflushq->enqueue(pbuffer);

    // Here thread blocks if queue empty
    pbuffer = m_queue.front();
  }

  return pbuffer->m_data + pbuffer->m_pos++;
}

void thread_buffer::flush()
{
  // Queue can never be empty here
  edge_buffer* pbuffer = m_queue.try_dequeue();
  assert(pbuffer);
  m_pflushq->enqueue(pbuffer);

  // Wait until queue has at least one edge_buffer available
  m_queue.front();
}

buffer_queue* thread_buffer::get_queue()
{
  return &m_queue;
}

// -----------------------------------------------------

buffer_manager::buffer_manager(buffer_queue* pflushq, const int queue_size, const size_t buffer_size)
  : m_pflushq(pflushq), m_queue_size(queue_size), m_buffer_size(buffer_size)
{}

thread_buffer* buffer_manager::register_thread(const threadid_t tid)
{
  thread_buffer* pthrbuf = new thread_buffer(m_pflushq);
  buffer_queue* pqueue = pthrbuf->get_queue();

  for (int i = 0; i < m_queue_size; ++i) {
    edge_buffer* pbuffer = new edge_buffer(m_buffer_size, tid);
    pqueue->enqueue(pbuffer);
  }

  m_allbuffers.insert( map_t::value_type(tid, pthrbuf) );
  return pthrbuf;
}

void buffer_manager::unregister_thread(const threadid_t tid)
{
  map_t::iterator res = m_allbuffers.find(tid);
  if (res != m_allbuffers.end()) {
    delete res->second;
    m_allbuffers.erase(res);
  }
}

thread_buffer* buffer_manager::get_buffer(const threadid_t tid)
{
  map_t::iterator res = m_allbuffers.find(tid);
  if (res == m_allbuffers.end())
    return NULL;
  else
    return res->second;
}

// -----------------------------------------------------

io_thread_func::io_thread_func(const char* filename, const edge_t nedges, buffer_queue* pflushq, buffer_manager* pmanager, const size_t buffer_size)
  : m_pflushq(pflushq), m_pmanager(pmanager), m_remainder(buffer_size, 0), m_stop(false)
{
  m_fd = open(filename, O_WRONLY | O_CREAT | O_TRUNC | O_DIRECT | O_SYNC, 0666);
  if (m_fd == -1) {
    fprintf(stderr, "Error: open call failed with errno %d\n", errno);
    exit(1);
  }

  // Compute the filesize that the file should have at the end
  m_filesize = nedges * sizeof(struct edge_struct);
}

void io_thread_func::operator() ()
{
  while (true)
  {
    edge_buffer* pbuffer = m_pflushq->dequeue();

    while (pbuffer) {
      if (pbuffer->m_pos == pbuffer->m_size) {
	flush_buffer(pbuffer);
      } else {
	save_for_later(pbuffer);
      }
      m_pmanager->get_buffer(pbuffer->m_owner)->get_queue()->enqueue(pbuffer);
      pbuffer = m_pflushq->try_dequeue();
    }

    if (m_stop) break;
  }

  // Flush the remainder, which may not be a full buffer, and truncate the file to
  // the computed filesize to drop the junk data from the buffer
  if (m_remainder.m_pos > 0) {
    flush_buffer(&m_remainder);	
    if (ftruncate(m_fd, m_filesize) == -1) {
      fprintf(stderr, "Error: ftruncate call failed with errno %d\n", errno);
      exit(1);
    }
  }
  close(m_fd);
}

void io_thread_func::stop()
{
  m_stop = true;
  m_pflushq->signal();
}

void io_thread_func::flush_buffer(edge_buffer* pbuffer)
{
  ssize_t bytes_left = pbuffer->m_size * sizeof(struct edge_struct);
  char* buffer_ptr = (char*)pbuffer->m_data;
  while (bytes_left > 0) {
    ssize_t bytes_written = write(m_fd, (void*)buffer_ptr, bytes_left);
    if (bytes_written == -1) {
      fprintf(stderr, "Error: write call failed with errno %d\n", errno);
      exit(1);
    }
    bytes_left -= bytes_written;
    buffer_ptr += bytes_written;
  }
  pbuffer->m_pos = 0;
}

void io_thread_func::save_for_later(edge_buffer* pbuffer)
{
  size_t avail = m_remainder.m_size - m_remainder.m_pos;
  size_t n = (avail < pbuffer->m_pos) ? avail : pbuffer->m_pos;
  size_t i = 0;
  while (i < n) {
    m_remainder.m_data[m_remainder.m_pos++] = pbuffer->m_data[i++];
  }

  if (m_remainder.m_pos == m_remainder.m_size)
    flush_buffer(&m_remainder);

  while (i < pbuffer->m_pos) {
    m_remainder.m_data[m_remainder.m_pos++] = pbuffer->m_data[i++];
  }

  pbuffer->m_pos = 0;
}

// -----------------------------------------------------

void make_ini_file(const char* graphname, const uint64_t vertices, const uint64_t edges)
{
  char filename[255];
  sprintf(filename, "%s.ini", graphname);
  int fd = open(filename, O_WRONLY | O_CREAT | O_TRUNC, 0666);

  char str[255];
#ifdef WEIGHT
  sprintf(str, "[graph]\nname=%s\ntype=1\n", graphname);
#else
  sprintf(str, "[graph]\nname=%s\ntype=2\n", graphname);
#endif
  ssize_t bytes = write(fd, str, strlen(str));
  sprintf(str, "vertices=%" PRIu64 "\nedges=%" PRIu64 "\n", vertices, edges);
  bytes = write(fd, str, strlen(str));
  (void)bytes; // to suppress warnings
  
  close(fd);
}

