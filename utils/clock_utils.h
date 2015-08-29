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

#ifndef _CLOCK_UTILS_
#define _CLOCK_UTILS_

#include "clock.hpp"
#include<sys/time.h>
#include "boost_log_wrapper.h"

//! Real time clock
class rtc_clock:public clock {
  unsigned long start_time;
  unsigned long elapsed_useconds;
  unsigned long get_current_rtc()
  {
    struct timeval tm;
    gettimeofday(&tm, NULL);
    return tm.tv_sec*1000000 + tm.tv_usec;
  }

 public:
 rtc_clock()
   :elapsed_useconds(0)
    {
    }
  void start()
  {
    start_time = get_current_rtc();
  }
  void stop()
  {
    elapsed_useconds += (get_current_rtc() - start_time);
  }
  void reset()
  {
    elapsed_useconds = 0;
  }
  unsigned long elapsed_time()
  {
    return elapsed_useconds;
  }
  void print(const char header[])
  {
    double elapsed_seconds = (double)elapsed_useconds/1000000;
    BOOST_LOG_TRIVIAL(info) << header << " " << elapsed_seconds << " seconds";
  }
};

#endif
