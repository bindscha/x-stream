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

#ifndef _X_INGEST_
#define _X_INGEST_
#include<sys/types.h>
#include<sys/shm.h>
#include "../utils/boost_log_wrapper.h"
struct ingest_t {
  unsigned long max;
  volatile unsigned long avail;
  volatile bool eof;
  unsigned char buffer[0];
};

static
ingest_t* attach_ingest(unsigned long key)
{
  void * data = shmat(shmget(key, 1, S_IRWXU), NULL, 0);
  if(data == MAP_FAILED) {
    perror("Failed to attach to ingest SHM:");
    exit(-1);
  }
  return (ingest_t*) data;
}

static
ingest_t* create_ingest_buffer(unsigned long key,
			       unsigned long size)
{
  int shmid = shmget(key, 
		     sizeof(ingest_t) + size,
		     S_IRWXU|IPC_CREAT);
  if(shmid == -1) {
    perror("Failed to create delta segment:");
    exit(-1);
  }
  ingest_t * ingest = attach_ingest(key);
  ingest->max   = size;
  ingest->avail = 0;
  ingest->eof   = false;
  BOOST_LOG_TRIVIAL(info) << "Ingest buffer ready";
  return ingest;
}
#endif
