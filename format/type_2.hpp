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

#ifndef _TYPE_2_FORMAT_
#define _TYPE_2_FORMAT_
#include<iostream>
#include<string>

namespace format {
  namespace type2 {
    class format_utils {
    public:
      static unsigned long split_size_bytes()
      {
	return 2*sizeof(vertex_t);
      }
      static void read_edge(const unsigned char* buffer,
			    vertex_t& src,
			    vertex_t& dst)
      {
	src = *(vertex_t *)buffer;
	dst = *(vertex_t *)(buffer + sizeof(vertex_t));
      }
      static void read_edge(const unsigned char* buffer,
                            vertex_t& src,
                            vertex_t& dst,
                            weight_t& value)
      {
        src = *(vertex_t *)buffer;
        dst = *(vertex_t *)(buffer + sizeof(vertex_t));
      }
      static void write_edge(unsigned char* buffer,
			     vertex_t& src,
			     vertex_t& dst,
			     weight_t& value)
      {
        *(vertex_t *)buffer = src;
	*(vertex_t *)(buffer + sizeof(vertex_t)) = dst;
      }

      static unsigned long split_key(const unsigned char *buffer, unsigned long jump)
      {
	vertex_t key;
	memcpy(&key, buffer, sizeof(vertex_t)); 
	key = key >> jump;
	return (unsigned long)key;
      }
    };
  }
}
#endif
