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

#ifndef _ALS_ASYNC_
#define _ALS_ASYNC_
#include "../../utils/options_utils.h"
#include "../../utils/desc_utils.h"
#include "../../utils/boost_log_wrapper.h"
#include "../../core/x-lib.hpp"
#include <cmath>
#include <boost/random.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/numeric/ublas/vector.hpp>
#include <boost/numeric/ublas/matrix.hpp>
#include <boost/numeric/ublas/io.hpp>
#include <boost/numeric/bindings/lapack/gesv.hpp>
#include <boost/numeric/bindings/traits/ublas_matrix.hpp>
#include <boost/numeric/bindings/traits/ublas_vector2.hpp>

#define LAMBDA 0.065
#define RANK   5

namespace ublas = boost::numeric::ublas;
namespace lapack = boost::numeric::bindings::lapack;

namespace algorithm {
  namespace sg_simple {
    class als_async_per_processor_data : public per_processor_data {
    public:
      static double sse;
      double sse_local;
      bool local_continue;
      als_async_per_processor_data() 
	:sse_local(0.0), local_continue(false)
      {}
      bool reduce(per_processor_data **per_cpu_array,
		  unsigned long processors)
      {
	bool global_continue = false;
	for(unsigned long i=0;i<processors;i++) {
	  als_async_per_processor_data * data = 
	    static_cast<als_async_per_processor_data *>(per_cpu_array[i]);
	  sse += data->sse_local;
	  data->sse_local = 0;
	  global_continue = global_continue || data->local_continue;
	  data->local_continue = false;
	}
	return !global_continue;
      }
    } __attribute__((__aligned__(64))) ;
  
    template <typename F>
    class als_async_factorization {
    
    private:

      struct vertex {
	vertex_t degree;
	vertex_t count;
	double feature_vec[RANK];
	double temp_mat[RANK][RANK];
      } __attribute__((__packed__));

      struct update {
	vertex_t target;
	double feature_vec[RANK];
	double rating;
      } __attribute__((__packed__));
    
      static unsigned long niters;

      // Helpers
      static void copy_vector(double src_vec[RANK], double dst_vec[RANK])
      {
	for (int i = 0; i < RANK; i++)
	  dst_vec[i] = src_vec[i];
      }
      static void zero_vector(double vec[RANK])
      {
	for (int i = 0; i < RANK; i++)
	  vec[i] = 0;
      }
      static void zero_matrix(double mat[RANK][RANK])
      {
	for (int i = 0; i < RANK; i++)
	  for (int j = 0; j < RANK; j++)
	    mat[i][j] = 0;
      }

      static void init_vertex(struct vertex& v)
      {
	v.degree = 0;
	v.count = 0;

	boost::mt19937 generator;
	boost::uniform_int<> distribution(0, 999);
	for (int i = 0; i < RANK; i++)
	  v.feature_vec[i] =  0.001 * distribution(generator);
      }

      // Solve the system Ax=b using LAPACK library and boost bindings for it
      static void solve(double mat[RANK][RANK], double vec[RANK])
      {
	ublas::matrix<double, ublas::column_major> A(RANK, RANK);
	ublas::vector<double> b(RANK);
      
	for (int i = 0; i < RANK; i++) {
	  b(i) = vec[i];
	  for (int j = 0; j < RANK; j++) {
	    A(i,j) = mat[i][j];
	  }
	}

	lapack::gesv(A,b);

	for (int i = 0; i < RANK; i++)
	  vec[i] = b(i);
      }

    public:
      static unsigned long vertex_state_bytes() {
	return sizeof(struct vertex);
      }
      static unsigned long split_size_bytes() {
	return sizeof(struct update);
      }

      static unsigned long split_key(unsigned char* buffer, unsigned long jump)
      {
	struct update* u = (struct update*)buffer;
	vertex_t key = u->target;
	key = key >> jump;
	return key;
      }

      static bool init(unsigned char* vertex_state,
		       unsigned long vertex_index,
		       unsigned long bsp_phase,
		       per_processor_data *cpu_state)
      {
	struct vertex* vertices = (struct vertex*)vertex_state;
	init_vertex(*vertices);
	return true;
      }

      static void apply_one_update(unsigned char* vertex_state,
				   unsigned char* update_stream,
				   per_processor_data *per_cpu_data,
				   bool loopback,
				   unsigned long bsp_phase)
      {
	struct update* u = (struct update*)update_stream;
	struct vertex* vertices = (struct vertex*)vertex_state;
	struct vertex* v = &vertices[x_lib::configuration::map_offset(u->target)];

	if (loopback)
	  bsp_phase++;

	if (bsp_phase <= niters)
	  {
	    // Track how many updates (edges) have been processed
	    v->count++;

	    // Initialize data structures for this iteration - zero out feature vector
	    // and temp matrix since we'll be adding to them.
	    if (v->count == 1) {
	      zero_vector(v->feature_vec);
	      zero_matrix(v->temp_mat);
	    }

	    // To compute the new feature vector of vertex v, we need to solve the system: A*feature_vec=b
	    // A is a matrix: A = O * O^T + D
	    // O is a submatrix of the other side of the graph where column vectors are feature vectors of
	    // those vertices that are connected to this vertex
	    // O^T is a transpose of O
	    // D is a diagonal matrix: D = lambda * degree * I, where I is an identity matrix
	    // b is a vector: b = O * r
	    // r is a ratings vector formed from the ratings of outgoing edges

	    // Calculating O*O^T and b
	    for (int i = 0; i < RANK; i++) {
	      v->feature_vec[i] += u->feature_vec[i] * u->rating;
	      for (int j = 0; j < RANK; j++) {
		v->temp_mat[i][j] += u->feature_vec[i] * u->feature_vec[j];
	      }
	    }

	    // Additional procesing after all updates have been gathered
	    if (v->count == v->degree)
	      {
		// Adding D to A
		for (int i = 0; i < RANK; i++) {
		  v->temp_mat[i][i] += LAMBDA * v->degree;
		}

		// Solve to get the new feature vector for the vertex
		solve(v->temp_mat, v->feature_vec);

		// Reset count for the next iteration
		v->count = 0;
	      }
	  }

	// After all iterations are finished, we use one more phase to
	// compute the sum of square errors. This is done on the right side.
	else
	{
	  double sqerror = u->rating;
	  for (int i = 0; i < RANK; i++)
	    sqerror -= v->feature_vec[i] * u->feature_vec[i];
	  sqerror *= sqerror;

	  static_cast<als_per_processor_data*>(per_cpu_data)->sse_local += sqerror;
	}
      }

      static bool generate_update(unsigned char* vertex_state,
				  unsigned char* edge_format,
				  unsigned char* update_stream,
				  per_processor_data* per_cpu_data,
				  unsigned long bsp_phase)
      {
	vertex_t src, dst;
	weight_t rating;
	F::read_edge(edge_format, src, dst, rating);

	struct vertex* vertices = (struct vertex*)vertex_state;
	struct vertex* v = &vertices[x_lib::configuration::map_offset(src)];

	// Iteration 0 is used to count the vertex degree
	if (bsp_phase == 0)
	{
	  v->degree++;
	  return false;
	}

        // Continue processing while bsp_phase <= niters
        if (bsp_phase > niters)
          return false;
        else
          static_cast<als_async_per_processor_data*>(per_cpu_data)->local_continue = true;
              
	// The graph is bipartite, and it is assumed that lower ids
	// form the left side.
	// The alternating starts by solving the right side in iteration 2,
	// after the left side generates first updates in iteration 1.
	// Therefore, in odd numbered iterations we solve the left
	// side, and in even numbered iterations we solve the right side.
	// Likewise, the left side generates updates in odd numbered 
	// iterations, and the right side in even numbered iterations.

	bool leftside = (src < dst) ? true : false;

	if ((bsp_phase % 2 == 1 && leftside) || (bsp_phase % 2 == 0 && !leftside))
	{
	  struct update* u = (struct update*)update_stream;
	  u->target = dst;
	  u->rating = (double)rating;
	  copy_vector(v->feature_vec, u->feature_vec);
	  return true;
	}
	else
	  return false;
      }

      static void preprocessing()
      {
	// We double the number, since in one phase we process one side of the
	// bipartite graph. The extra +1 is for the iteration 0 which is used
	//just to compute the vertex degrees
	niters = 1 + 2 * vm["als::niters"].as<unsigned long>();
      }

      static void postprocessing()
      {
	BOOST_LOG_TRIVIAL(info) << "ALGORITHM::ALS::SSE " << als_async_per_processor_data::sse;
	unsigned long nedges = pt.get<unsigned long>("graph.edges");
	double rmse = std::sqrt( als_async_per_processor_data::sse / (1. * nedges) );
	BOOST_LOG_TRIVIAL(info) << "ALGORITHM::ALS::RMSE " << rmse;
      }

      static per_processor_data * 
      create_per_processor_data(unsigned long processor_id)
      {
	return new als_async_per_processor_data();
      }
    
      static unsigned long min_super_phases()
      {
	return 1;
      }

      static bool need_init(unsigned long bsp_phase)
      {
	return (bsp_phase == 0);
      }
    
    };

    // These should be in a cpp file, but it's ok since we only include
    // this header once in driver.cpp
    template<typename F>
    unsigned long als_async_factorization<F>::niters;

    double als_async_per_processor_data::sse = 0;

  }
}
#endif
