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

#ifndef _CLOCK_GENERIC_
#define _CLOCK_GENERIC_
#include<sstream>
#include<string>
#include<iostream>
class clock {
public:
  virtual void start() = 0;
  virtual void stop() = 0;
  virtual void reset() = 0;
  virtual void print(const char header[]) = 0;
  virtual unsigned long elapsed_time() = 0;

  static std::string timestamp()
  {
    struct timeval tv;
    struct tm *now;
    char buf[64];

    gettimeofday(&tv, NULL);
    now = localtime(&tv.tv_sec);
    strftime(buf, sizeof buf, "%Y-%m-%d %H:%M:%S", now);
    std::string str(buf);
    return str;
  }
};
#endif
