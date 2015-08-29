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

#ifndef HPGP_GENERATORS_DEFS_H
#define HPGP_GENERATORS_DEFS_H

#include <cstdint>
#include <cinttypes>

#ifndef VERTEX_TYPE_32
typedef uint64_t vertex_t;
#define VT_MAX (UINT64_C(0xFFFFFFFFFFFFFFFF))
#define PRIvt PRIu64
#else
typedef uint32_t vertex_t;
#define VT_MAX (UINT32_C(0xFFFFFFFF))
#define PRIvt PRIu32
#endif

typedef uint64_t edge_t;
#define PRIet PRIu64

#ifndef VALUE_TYPE_32
typedef double value_t;
#else
typedef float value_t;
#endif

#ifdef __GNUC__
#define LIKELY(x)       __builtin_expect((x),1)
#define UNLIKELY(x)     __builtin_expect((x),0)
#else
#define LIKELY(x)       (x)
#define UNLIKELY(x)     (x)
#endif

#endif /* HPGP_GENERATORS_DEFS_H */
