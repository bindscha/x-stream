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

#ifndef _X_SPLITTER_
#define _X_SPLITTER_
#include "split_stream.hpp"
#include<boost/assert.hpp>
#include<boost/thread/mutex.hpp>
#include<stdlib.h>

namespace x_lib {

  struct __attribute__((__packed__)) filter_entry {
    struct filter_entry* volatile next;
  };

  struct padded_work_q {
    struct filter_entry * volatile work_q;
    char padding[64];
  };

  class filter {
    struct filter_entry *work_items;
    struct padded_work_q *work_q;
    struct padded_work_q *work_q_visible;
    unsigned long num_queues;
  public:
    filter(unsigned long items, unsigned long processors)
      :num_queues(processors)
    {
      work_items = (struct filter_entry *)
	map_anon_memory(items*sizeof(struct filter_entry),
			true, "Filter");
      for(unsigned long i=0;i<items;i++) {
	work_items[i].next  = &work_items[i];
      }
      work_q = new padded_work_q[processors];
      for(unsigned long i=0;i<processors;i++) {
	work_q[i].work_q = NULL;
      }
      work_q_visible = new padded_work_q[processors];
      for(unsigned long i=0;i<processors;i++) {
	work_q_visible[i].work_q = NULL;
      }
    }

    void q(unsigned long id)
    {
      struct filter_entry *itm = &work_items[id];
      unsigned long q = id & (num_queues - 1);
      struct filter_entry *prev;
      if(__sync_bool_compare_and_swap(&itm->next, itm, NULL)) {
	do {
	  prev = itm->next = work_q[q].work_q;
	} while(!__sync_bool_compare_and_swap(&work_q[q].work_q, prev, itm));
      }
    }
    
    void prep_dq(unsigned long qid)
    {
      work_q_visible[qid] = work_q[qid];
    }
    
    unsigned long dq(unsigned long qid)
    {
      struct filter_entry *avail;
      unsigned long to_steal_from = qid;
      do {
	while((avail = work_q_visible[to_steal_from].work_q) != NULL) {
	  if(__sync_bool_compare_and_swap(&work_q_visible[to_steal_from].work_q,
					  avail, avail->next)) {
	    return avail - &work_items[0];
	  }
	}
	to_steal_from++;
	if(to_steal_from == num_queues) {
	  to_steal_from = 0;
	}
      } while(to_steal_from != qid);
      return ULONG_MAX; // No work
    }

    void done(unsigned long qid)
    {
      filter_entry *head = work_q[qid].work_q;
      while(head != NULL) {
	filter_entry *tmp = head;
	head = head->next;
	tmp->next = tmp;
      }
      work_q[qid].work_q = work_q_visible[qid].work_q = NULL;
    }
  };

  /* return true if final output has moved to aux */
  // T = item type, M = partition map
  template<typename T, typename M>
  bool x_split(unsigned long *index_in,
	       unsigned char *buffer_in,
	       unsigned long *index_aux,
	       unsigned char *buffer_aux,
	       unsigned long input_bytes,
	       unsigned long fanout,
	       unsigned long final_split_cnt,
	       filter *workq)
  {
    unsigned long *indices[2];
    unsigned long input, num_in = 1, num_out;
    unsigned char *buffers[2];
    unsigned long output_id;
    indices[0] = index_in;
    indices[1] = index_aux;
    buffers[0] = buffer_in;
    buffers[1] = buffer_aux;
    input = 0;
    unsigned long split_size_bytes = T::item_size();

    if(fanout > final_split_cnt) {
      fanout = final_split_cnt;
    }

    /* A special case */
    if(final_split_cnt == 1) {
      if(input_bytes > 0) {
	workq->q(0);
      }
      return false;
    }

    unsigned long shift = 0;
    num_out = final_split_cnt;
    while(num_out != 1) {
      shift++;
      num_out = num_out >> 1;
    }
    unsigned long shift_delta = 0;
    num_out = fanout;
    while(num_out != 1) {
      shift_delta++;
      num_out = num_out >> 1;
    }
    while(num_in < final_split_cnt) {
      if(shift < shift_delta) {
	shift_delta = shift;
	fanout = (1 << shift_delta);
      }
      shift-=shift_delta;
      num_out = num_in*fanout;
      BOOST_ASSERT_MSG(num_out <= final_split_cnt,
		       "Shift error!");
      BOOST_ASSERT_MSG(num_out%num_in == 0, "Improper split tree");
      unsigned long output_base = indices[input][0];
      unsigned long input_base = output_base;
      for(unsigned long i=0;i<num_in;i++) {
	unsigned char * input_stream = buffers[input] + indices[input][i];
	unsigned long input_size;
	if(i == (num_in - 1)) {
	  input_size = (input_base + input_bytes) - indices[input][i];
	}
	else {
	  input_size = indices[input][i + 1] - indices[input][i];
	}
	BOOST_ASSERT_MSG(input_size % split_size_bytes == 0,
			 "Misalinged substream!");
	indices[input][i] = 0;
	unsigned long output_start = fanout*i;
	unsigned long output_stop = fanout*(i + 1);	
	if(input_size == 0) {
	  for(unsigned long j=output_start;j<output_stop;j++) {
	    indices[1-input][j] = output_base;
	  }
	  continue;
	}
	unsigned char *output_stream = buffers[1-input];
	bool sorted = true;
	unsigned long old_key = 0;
	for(unsigned long j=0;j<input_size;j+=split_size_bytes) {
	  unsigned long key = M::map(T::key(input_stream + j));
	  sorted  = sorted && (key >= old_key);
	  old_key = key;
	  output_id = key >> shift; //sorted does not change !
	  output_id = output_id & (num_out - 1);
	  indices[1-input][output_id]+=split_size_bytes;
	}
	if(num_out == final_split_cnt) {
	  BOOST_ASSERT_MSG(shift == 0, "BUG: shift is not zero.");
	  for(unsigned long j=output_start;j<output_stop;j++) {
	    if(indices[1-input][j] != 0) {
		workq->q(j);
	    }
	    unsigned long temp = output_base;
	    output_base += indices[1-input][j];
	    indices[1-input][j] = temp;
	  }
	}
	else {
	  for(unsigned long j=output_start;j<output_stop;j++) {
	    unsigned long temp = output_base;
	    output_base += indices[1-input][j];
	    indices[1-input][j] = temp;
	  }
	}
	if(sorted) {
	  memcpy(output_stream + indices[1-input][output_start],
		 input_stream, input_size);
	}
	else {
	  unsigned long saved_start_index = indices[1-input][output_start];
	  for(unsigned long j=0;j<input_size;j+=split_size_bytes) {
	    output_id = M::map(T::key(input_stream + j))>> shift;
	    output_id = output_id & (num_out - 1);
	    memcpy(output_stream + indices[1-input][output_id],
		   input_stream + j, split_size_bytes);
	    indices[1-input][output_id] += split_size_bytes;
	  }
	  /* Re-adjust offsets */
	  for(unsigned long j=output_stop - 1;j > output_start;j--) {
	    indices[1-input][j] = indices[1-input][j-1];
	  }
	  indices[1-input][output_start] = saved_start_index;
	}
      }
      num_in = num_out;
      input = 1 - input;
    }
    return (input == 1);
  }

  // Do an in-situ quicksort
  // A = algorithm, T = item type, M = partition map
  extern unsigned long qsort_keys;
  template<typename T, typename M>
  int qsort_compare(const void *left, const void *right)
  {
    unsigned long key_left  = M::map(T::key((unsigned char *)left));
    unsigned long key_right = M::map(T::key((unsigned char *)right));
    key_left  = key_left  & (qsort_keys - 1);
    key_right = key_right & (qsort_keys - 1);
    if(key_left < key_right) {
      return -1;
    }
    else if(key_left == key_right) {
      return 0;
    }
    else {
      return 1;
    }
  }
  
  template<typename T, typename M>
  void x_split_qsort(unsigned long *index,
		     unsigned char *buffer,
		     unsigned long input_bytes,
		     filter *workq)
  {
    /* A special case */
    if(qsort_keys == 1) {
      if(input_bytes > 0) {
	workq->q(0);
      }
      return;
    }
    unsigned long split_size_bytes = T::item_size();
    unsigned char *input_stream = buffer + index[0];
    qsort(input_stream,
	  input_bytes/split_size_bytes,
	  split_size_bytes,
	  qsort_compare<T,M>);
    unsigned long output_base = index[0];
    index[0] = 0;
    for(unsigned long i=0;i<input_bytes;i+=split_size_bytes) {
      unsigned long output_id = M::map(T::key(input_stream + i));
      output_id = output_id & (qsort_keys - 1);
      index[output_id] += split_size_bytes;
    }
    for(unsigned long i=0;i<qsort_keys;i++) {
      unsigned long temp = output_base;
      if(index[i] != 0) {
	workq->q(i);
      }
      output_base += index[i];
      index[i] = temp;
    }
  }
}

#endif
