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

#ifndef _HYPER_LOG_LOG_
#define _HYPER_LOG_LOG_
#include<boost/assert.hpp>
#include "boost_log_wrapper.h"
// An implementation of hyper log log counters
// Based on the implementation at http://dsiutils.di.unimi.it/
// originally by Paolo Boldi and Sebastiano Vigna

// Adapted to the X-stream model by Amitabha Roy
// We fix register width to 8 to avoid any bitshift operations
// Note that this restricts us to a maximum cardinality of 2^{256}
// We restrict this to < 2^{127} in order to fit in a float
// which should be enough for "big-data" :)

// This structure is populated with key parameter necessary to access the
// counters
struct hyper_log_log_params {
  float rsd;                     // desired relative standard deviation
  unsigned int log2_m;           // Log of register count
  unsigned int m_minus_1;        // Mask to get register index
  float alphaMM;
  float small_cnt_threshold;     // Threshold for small counts
  float *small_cnt_lookup;        // lookup table for small counts
};

static void setup_hll_params(hyper_log_log_params *params,
			     float rsd) // rsd == relative standard deviation
{
  params->rsd = rsd;
  params->log2_m =
    (int)ceil(log2((1.106/rsd)*(1.106/rsd)));
  params->m_minus_1 = ((1 << params->log2_m) - 1);
  unsigned int m = params->m_minus_1 + 1;
  params->small_cnt_lookup = (float *)malloc(m*sizeof(float));
  for(unsigned long i=0;i<m;i++) {
    params->small_cnt_lookup[i] = m*logf(((float)m)/(i+1));
  }
  switch(params->log2_m) {
  case 4:
    params->alphaMM = 0.673 * m * m;
    break;
  case 5:
    params->alphaMM = 0.697 * m * m;
    break;
  case 6:
    params->alphaMM = 0.709 * m * m;
    break;
  default:
    params->alphaMM = (0.7213 /(1 + 1.079/m)) * m * m;
    break;
  }
  params->small_cnt_threshold = (5.0f)*(params->m_minus_1 + 1)/2;
  params->small_cnt_threshold = params->alphaMM/params->small_cnt_threshold;
}

static void print_hll_params(hyper_log_log_params *param)
{
  BOOST_LOG_TRIVIAL(info) << "HLL::PARAMS::rsd " << 
    param->rsd;
  BOOST_LOG_TRIVIAL(info) << "HLL::PARAMS::alphaMM " << 
    param->alphaMM;
  BOOST_LOG_TRIVIAL(info) << "HLL::PARAMS::counters " << 
    (param->m_minus_1 + 1);
}

static int sizeof_hll_counter(hyper_log_log_params *param)
{
  return (1 << param->log2_m);
}

unsigned long jenkins(unsigned long x, unsigned long seed)
{
  unsigned long a, b, c;
  /* Set up the internal state */
  a = seed + x;
  b = seed;
  c = 0x9e3779b97f4a7c13L; /* the golden ratio; an arbitrary value */
  a -= b; a -= c; a ^= (c >> 43);
  b -= c; b -= a; b ^= (a << 9);
  c -= a; c -= b; c ^= (b >> 8);
  a -= b; a -= c; a ^= (c >> 38);
  b -= c; b -= a; b ^= (a << 23);
  c -= a; c -= b; c ^= (b >> 5);
  a -= b; a -= c; a ^= (c >> 35);
  b -= c; b -= a; b ^= (a << 49);
  c -= a; c -= b; c ^= (b >> 11);
  a -= b; a -= c; a ^= (c >> 12);
  b -= c; b -= a; b ^= (a << 18);
  c -= a; c -= b; c ^= (b >> 22);
  return c;
}

// Count an item
static void add_hll_counter(hyper_log_log_params *param,
			    unsigned char *counter, 
			    unsigned long hash)
{
  unsigned int index  = hash & param->m_minus_1;
  unsigned char value = counter[index];
  unsigned char new_value = __builtin_ffs(hash >> param->log2_m);
  // Note: with an 8 bit counter new_value + 1 must fit
  if(new_value > value) {
    counter[index] = new_value;
  }
}

// Union of two counters
// A = A U B
#ifdef __AVX2__
#define HLL_ALIGN __attribute__((aligned(32)))
static bool hll_union(unsigned char *counterA, 
		      unsigned char *counterB,
		      hyper_log_log_params *param)
{
  unsigned int bytes = param->m_minus_1 + 1;
  unsigned int index = 0;
  bool no_change = true;
  typedef char v32qi __attribute__((vector_size(32)));
  typedef long long int v4di __attribute__((vector_size(32)));
  typedef char v16qi __attribute__((vector_size(16)));
  typedef long long int v2di __attribute__((vector_size(16)));
  v4di mask_right2, left2, right2;
  v2di mask_right, left, right;
  while(bytes >= 32) {
    left2  = *(v4di *)&counterA[index];
    right2 = *(v4di *)&counterB[index];
    mask_right2  = (v4di)
      __builtin_ia32_pcmpgtb256((v32qi)right2, (v32qi)left2);
    no_change = no_change && __builtin_ia32_ptestz256(mask_right2, mask_right2);
    right2      = __builtin_ia32_andsi256(right2, mask_right2);
    mask_right2 = __builtin_ia32_andnotsi256(mask_right2, left2);
    left2       = __builtin_ia32_por256(right2, mask_right2);
    *(v4di *)&counterA[index] = left2;
    index +=32;
    bytes -=32;
  }
  while(bytes >= 16) {
    left  = *(v2di *)&counterA[index];
    right = *(v2di *)&counterB[index];
    mask_right  = (v2di)
      __builtin_ia32_pcmpgtb128((v16qi)right, (v16qi)left);
    no_change = no_change && __builtin_ia32_ptestz128(mask_right, mask_right);
    right      = __builtin_ia32_pand128(right, mask_right);
    mask_right = __builtin_ia32_pandn128(mask_right, left);
    left       = __builtin_ia32_por128(right, mask_right);
    *(v2di *)&counterA[index] = left;
    index +=16;
    bytes -=16;
  }
  while(bytes > 0) {
    if(counterA[index] < counterB[index]) {
      no_change = false;
      counterA[index] = counterB[index];
    }
    index++;
    bytes--;
  }
  return !no_change;
}
#elif __SSE4_1__
#define HLL_ALIGN __attribute__((aligned(16)))
static bool hll_union(unsigned char *counterA, 
		      unsigned char *counterB,
		      hyper_log_log_params *param)
{
  typedef char v16qi __attribute__((vector_size(16)));
  typedef long long int v2di __attribute__((vector_size(16)));
  v2di mask_right, left, right;
  unsigned int bytes = param->m_minus_1 + 1;
  unsigned int index = 0;
  bool no_change = true;
  v2di change_mask = {0};
  while(bytes >= 16) {
    left  = *(v2di *)&counterA[index];
    right = *(v2di *)&counterB[index];
    mask_right  = (v2di)
      __builtin_ia32_pcmpgtb128((v16qi)right, (v16qi)left);
    change_mask  = __builtin_ia32_por128(mask_right, change_mask);
    right      = __builtin_ia32_pand128(right, mask_right);
    mask_right = __builtin_ia32_pandn128(mask_right, left);
    left       = __builtin_ia32_por128(right, mask_right);
    *(v2di *)&counterA[index] = left;
    index +=16;
    bytes -=16;
  }
  no_change = no_change && __builtin_ia32_ptestz128(change_mask, change_mask);
  while(bytes > 0) {
    if(counterA[index] < counterB[index]) {
      no_change = false;
      counterA[index] = counterB[index];
    }
    index++;
    bytes--;
  }
  return !no_change;
}
#else
#warning "Using slow hyperloglog implementation as no SSE4.1 is available or is disabled"
#define HLL_ALIGN
static bool hll_union(unsigned char *counterA, 
		      unsigned char *counterB,
		      hyper_log_log_params *param)
{
  bool no_change = true;
  for(unsigned int i=0;i<= param->m_minus_1;i++) {
    if(counterA[i] < counterB[i]) {
      no_change = false;
      counterA[i] = counterB[i];
    }
  }
  return !no_change;
}
#endif

// Return count estimate
static float count_hll_counter(hyper_log_log_params *param,
			       unsigned char *counter)
{
  float s   = 0;
  float tmp = 0;
  unsigned int zeroes = 0;
  for(unsigned int i=0;i<=param->m_minus_1;i++) {
    if(counter[i] == 0) {
      zeroes++;
    }
    unsigned int twiddle = counter[i];
    // Raise 2 to -ve power using IEEE754 exponent twiddling
    twiddle = 127 - twiddle;
    twiddle <<= 23;
    memcpy(&tmp, &twiddle, sizeof(float));
    s += tmp;
  }
  // Small range correction (exactly as in the reference implementation)
  if(zeroes && s > param->small_cnt_threshold) {
    s = param->small_cnt_lookup[zeroes - 1];
  }
  else {
    s = (param->alphaMM/s);
  }
  return s;
}

//Initialize counter
void hll_init(unsigned char *counter, hyper_log_log_params *param)
{
  memset(counter, 0, sizeof_hll_counter(param));
}

#endif
