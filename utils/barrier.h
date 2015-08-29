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

#ifndef _BARRIER_
#define _BARRIER_
// Sense reversing barrier 
// without syscall overheads of pthread_barrier

class x_barrier {
  volatile unsigned long count[2];
  volatile unsigned long sense;
  unsigned long expected;
 public:
  x_barrier(unsigned long expected_in)
    :sense(0), expected(expected_in)
  {
    count[0] = 0;
    count[1] = 0;
  }
  void wait()
  {
    unsigned long sense_used = sense;
    unsigned long arrived =
      __sync_fetch_and_add(&count[sense_used], 1);
    if(arrived == (expected - 1)) {
      sense = 1 - sense_used; // Reverse sense
      count[sense_used] = 0;
    }
    while(count[sense_used] != 0);
    __sync_synchronize(); // Also clobber memory
  }
};
#endif
