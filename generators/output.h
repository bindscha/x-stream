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

#ifndef HPGP_GENERATORS_OUTPUT_H
#define HPGP_GENERATORS_OUTPUT_H

#include <queue>
#include <map>
#include <boost/thread/mutex.hpp>
#include <boost/thread/condition_variable.hpp>
#include "defs.h"

const size_t DEFAULT_BLOCK_SIZE = 4096;

struct edge_struct {
  vertex_t src;
  vertex_t dst;
#ifdef WEIGHT
  value_t  weight;
#endif
} __attribute__((packed));

class threadid_t {
public:
  threadid_t(const int id) : m_id(id) {}
  bool operator< (const threadid_t& rhs) const {
    return m_id < rhs.m_id;
  }
private:
  int m_id;
};

class edge_buffer {
public:
  edge_buffer(const size_t size, const threadid_t owner);
  ~edge_buffer();

  struct edge_struct* m_data;
  size_t m_size;
  size_t m_pos;
  threadid_t m_owner;
};

// This queue is designed to work properly only with
// one consumer thread
class buffer_queue {
public:
  void enqueue(edge_buffer* pbuffer);
  edge_buffer* dequeue();
  edge_buffer* try_dequeue();
  edge_buffer* front();
  edge_buffer* try_front();
  void signal();

private:
  typedef std::queue<edge_buffer*> storage_t;
  storage_t m_data;
  boost::condition_variable m_cond;
  boost::mutex m_mutex;
};

class thread_buffer {
public:
  thread_buffer(buffer_queue* pflushq);
  ~thread_buffer();

  struct edge_struct* edge_struct();
  void flush();
  buffer_queue* get_queue();

private:
  buffer_queue m_queue;
  buffer_queue* m_pflushq;
};

class buffer_manager {
public:
  buffer_manager(buffer_queue* pflushq, const int queue_size, const size_t buffer_size); 

  thread_buffer* register_thread(const threadid_t tid);
  void unregister_thread(const threadid_t tid);
  thread_buffer* get_buffer(const threadid_t tid);

private:
  typedef std::map<threadid_t, thread_buffer*> map_t;
  map_t m_allbuffers;
  buffer_queue* m_pflushq;
  int m_queue_size;
  size_t m_buffer_size;
};

class io_thread_func {
public:
  io_thread_func(const char* filename, const edge_t nedges, buffer_queue* pflushq, buffer_manager* pmanager, const size_t buffer_size);

  void operator() ();
  void stop();

private:
  void flush_buffer(edge_buffer* pbuffer);
  void save_for_later(edge_buffer* pbuffer);

  int m_fd;
  size_t m_filesize;
  buffer_queue* m_pflushq;
  buffer_manager* m_pmanager;
  edge_buffer m_remainder;
  bool m_stop;
};

void make_ini_file(const char* graphname, const uint64_t vertices, const uint64_t edges);

#endif /* HPGP_GENERATORS_OUTPUT_H */
