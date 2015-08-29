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

#ifndef _SG_DRIVER_
#define _SG_DRIVER_
#include<sys/time.h>
#include<sys/resource.h>
#include "x-lib.hpp"
#include</usr/include/python3.2/Python.h>
#include</usr/lib/python3/dist-packages/numpy/core/include/numpy/arrayobject.h>


// Implement a wrapper for simpler graph algorithms that alternate between
// synchronously gathering updates and synchronously scattering them along edges

namespace algorithm {
  const unsigned long phase_edge_split    = 0;
  const unsigned long superphase_begin    = 1;
  const unsigned long phase_gather        = 2;
  const unsigned long phase_scatter       = 3;
  const unsigned long phase_post_scatter  = 4;
  const unsigned long phase_terminate     = 5;

  struct sg_pcpu:public per_processor_data {
    unsigned long processor_id;
    bool i_vote_to_stop;
    static sg_pcpu ** per_cpu_array;
    static x_barrier *sync;
    // Stats
    unsigned long update_bytes_out;
    unsigned long update_bytes_in;
    unsigned long edge_bytes_streamed;
    unsigned long partitions_processed;
    // 
    static x_lib::filter *scatter_filter;
    bool activate_partition_for_scatter;
    
    /* begin work specs. */
    static unsigned long bsp_phase;
    static unsigned long current_step;
    static bool do_algo_reduce;
    /* end work specs. */

    static per_processor_data **algo_pcpu_array;
    per_processor_data *algo_pcpu;

    bool reduce(per_processor_data **per_cpu_array,
		unsigned long processors)
    {
      if(algo_pcpu_array[0] != NULL && do_algo_reduce) {
	return algo_pcpu_array[0]->reduce(algo_pcpu_array, processors);
      }
      else {
	return false; // Should be don't care
      }
    }
  } __attribute__((__aligned__(64)));

  template<typename A, typename F>
  class scatter_gather {
    static PyObject *pModule, *pName, *pDict, *pClass, *pInstance, *pReturnValue;
    sg_pcpu ** pcpu_array;
    bool heartbeat;
    bool measure_scatter_gather;
    x_lib::streamIO<scatter_gather> *graph_storage;
    unsigned long vertex_stream;
    unsigned long edge_stream;
    unsigned long updates0_stream;
    unsigned long updates1_stream;
    unsigned long init_stream;
    rtc_clock wall_clock;
    rtc_clock setup_time;
    rtc_clock state_iter_cost;
    rtc_clock scatter_cost;
    rtc_clock gather_cost;

  public:
    scatter_gather();
    static void partition_pre_callback(unsigned long super_partition,
				       unsigned long partition,
				       per_processor_data* cpu_state);
    static void partition_callback(x_lib::stream_callback_state *state);
    static void partition_post_callback(unsigned long super_partition,
					unsigned long partition,
					per_processor_data *cpu_state);
    void operator() ();
    static unsigned long max_streams()
    {
      return 5; // vertices, edges, init_edges, updates0, updates1
    }

    static unsigned long max_buffers()
    {
      return 4;
    }

    static unsigned long vertex_state_bytes()
    {
      return A::vertex_state_bytes();
    }

    static unsigned long vertex_stream_buffer_bytes()
    {
      return A::split_size_bytes() + F::split_size_bytes();
    }

    static void state_iter_callback(unsigned long superp, 
				    unsigned long partition,
				    unsigned long index,
				    unsigned char *vertex,
				    per_processor_data *cpu_state)
    {
      unsigned long global_index = 
	x_lib::configuration::map_inverse(superp, partition, index);
      sg_pcpu *pcpu = static_cast<sg_pcpu *>(cpu_state);
      bool will_scatter = A::init(vertex, global_index,
				  sg_pcpu::bsp_phase,
				  sg_pcpu::algo_pcpu_array[pcpu->processor_id]);
      if(will_scatter) {
	sg_pcpu::scatter_filter->q(partition);
      }
    }

    static per_processor_data * 
    create_per_processor_data(unsigned long processor_id)
    {
      return sg_pcpu::per_cpu_array[processor_id];
    }
  
    static void do_cpu_callback(per_processor_data *cpu_state)
    {
      sg_pcpu *cpu = static_cast<sg_pcpu *>(cpu_state);
      if(sg_pcpu::current_step == superphase_begin) {
	cpu->i_vote_to_stop = true;
      }
      else if(sg_pcpu::current_step == phase_post_scatter) {
	sg_pcpu::scatter_filter->done(cpu->processor_id);
      }
      else if(sg_pcpu::current_step == phase_terminate) {
	BOOST_LOG_TRIVIAL(info)<< "CORE::PARTITIONS_PROCESSED " << cpu->partitions_processed;
	BOOST_LOG_TRIVIAL(info)<< "CORE::BYTES::EDGES_STREAMED " << cpu->edge_bytes_streamed;
	BOOST_LOG_TRIVIAL(info)<< "CORE::BYTES::UPDATES_OUT " << cpu->update_bytes_out;
	BOOST_LOG_TRIVIAL(info)<< "CORE::BYTES::UPDATES_IN " << cpu->update_bytes_in;
      }
    }
  };
  
  template<typename A, typename F>
  scatter_gather<A, F>::scatter_gather()
  {
    BOOST_LOG_TRIVIAL(info)<<"SG-DRIVER-PYTHON";
    wall_clock.start();
    setup_time.start();
    heartbeat = (vm.count("heartbeat") > 0);
    measure_scatter_gather = (vm.count("measure_scatter_gather") > 0);
    unsigned long num_processors = vm["processors"].as<unsigned long>();
    per_processor_data **algo_pcpu_array = new per_processor_data *[num_processors];
    sg_pcpu::per_cpu_array = pcpu_array = new sg_pcpu *[num_processors];
    sg_pcpu::sync = new x_barrier(num_processors);
    sg_pcpu::do_algo_reduce = false;
    for(unsigned long i=0;i<num_processors;i++) {
      pcpu_array[i] = new sg_pcpu();
      pcpu_array[i]->processor_id = i;
      pcpu_array[i]->update_bytes_in = 0;
      pcpu_array[i]->update_bytes_out = 0;
      pcpu_array[i]->edge_bytes_streamed = 0;
      pcpu_array[i]->partitions_processed = 0;
      algo_pcpu_array[i] = A::create_per_processor_data(i);
    }
    sg_pcpu::algo_pcpu_array = algo_pcpu_array;
    A::preprocessing(); // Note: ordering critical with the next statement
    graph_storage = new x_lib::streamIO<scatter_gather>();
    sg_pcpu::scatter_filter = new
      x_lib::filter(MAX(graph_storage->get_config()->cached_partitions,
			graph_storage->get_config()->super_partitions),
		    num_processors);
    sg_pcpu::bsp_phase = 0;
    vertex_stream = 
      graph_storage->open_stream("vertices", true, 
				 vm["vertices_disk"].as<unsigned long>(),
				 graph_storage->get_config()->vertex_size);
    if(graph_storage->get_config()->super_partitions == 1) {
      std::string efile = pt.get<std::string>("graph.name");
      edge_stream = 
	graph_storage->open_stream((const char *)efile.c_str(), false,
				   vm["input_disk"].as<unsigned long>(),
				   F::split_size_bytes());
    }
    else {
      edge_stream = 
	graph_storage->open_stream("edges", true, 
				   vm["edges_disk"].as<unsigned long>(),
				   F::split_size_bytes());
      std::string efile = pt.get<std::string>("graph.name");
      init_stream = 
	graph_storage->open_stream((const char *)efile.c_str(), false,
				   vm["input_disk"].as<unsigned long>(),
				   F::split_size_bytes(), 1);
    }
    updates0_stream = 
      graph_storage->open_stream("updates0", true, 
				 vm["updates0_disk"].as<unsigned long>(),
				 A::split_size_bytes());
    updates1_stream = 
      graph_storage->open_stream("updates1", true, 
				 vm["updates1_disk"].as<unsigned long>(),
				 A::split_size_bytes());
    setup_time.stop();
  }
  
  template<typename F> 
  struct edge_type_wrapper
  {
    static unsigned long item_size()
    {
      return F::split_size_bytes();
    }
    
    static unsigned long key(unsigned char *buffer)
    {
      return F::split_key(buffer, 0);
    }
  };

  template<typename A> 
  struct update_type_wrapper
  {
    static unsigned long item_size()
    {
      return A::split_size_bytes();
    }
    static unsigned long key(unsigned char *buffer)
    {
      return A::split_key(buffer, 0);
    }
  };

  template<typename A, typename F>
  void scatter_gather<A, F>::operator() ()
  {
    const x_lib::configuration *config = graph_storage->get_config();
    // Edge split
    if(config->super_partitions > 1) {
      sg_pcpu::current_step = phase_edge_split;
      x_lib::do_stream< scatter_gather<A, F>, 
			edge_type_wrapper<F>, 
			edge_type_wrapper<F> >
	(graph_storage, 0, init_stream, edge_stream, NULL);
      graph_storage->close_stream(init_stream);
    }
    //Module loading. Interpreter initialization is in x-lib!

    const char *module_bfs = "bfs";
    const char *module_pagerank = "pagerank";
    if(vm["benchmark"].as<std::string>() == "bfs") {
      pName = PyUnicode_FromString(module_bfs);
    }
    else if(vm["benchmark"].as<std::string>() == "pagerank"){
      pName = PyUnicode_FromString(module_pagerank);
    }
    pModule = PyImport_Import(pName);
    if(pModule == NULL){
      BOOST_LOG_TRIVIAL(info) << "ERROR IN IMPORTED MODULE!";
      PyErr_Print();
      exit(-1);     
    }
    pDict = PyModule_GetDict(pModule);
    if(vm["benchmark"].as<std::string>() == "bfs") {
      pClass = PyDict_GetItemString(pDict, "AlgorithmBFS");
    }
    else if(vm["benchmark"].as<std::string>() == "pagerank"){
      pClass = PyDict_GetItemString(pDict, "AlgorithmPagerank");
    }

    if(PyCallable_Check(pClass))
    {
      pInstance = PyObject_CallObject(pClass, NULL);
    }
    else{
      BOOST_LOG_TRIVIAL(info) << "CLASS NOT FOUND!";
      exit(-1);
    }
    //constant parameters initialization
    char* funName = new char[20];
    if(vm["benchmark"].as<std::string>() == "bfs") {
      strcpy(funName, "initialization_bfs");
    }
    else if(vm["benchmark"].as<std::string>() == "pagerank"){
      strcpy(funName, "initialization_pgrnk");
    }

    char* format = new char[5];
    unsigned long edge_size = F::split_size_bytes(); 
    if(vm["benchmark"].as<std::string>() == "bfs") {
      strcpy(format, "LLL");
      PyObject_CallMethod(pInstance, funName, format, x_lib::configuration::partition_shift,
      x_lib::configuration::super_partition_shift, edge_size);
    }
    else if(vm["benchmark"].as<std::string>() == "pagerank") {
      strcpy(format, "LLLL");
      unsigned long niters = 1 + vm["pagerank::niters"].as<unsigned long>();
      PyObject_CallMethod(pInstance, funName, format, x_lib::configuration::partition_shift,
      x_lib::configuration::super_partition_shift, edge_size, niters);
    }

    // Supersteps
    unsigned long PHASE = 0;
    bool global_stop = false;
    while(true) {
      sg_pcpu::current_step = superphase_begin;
      x_lib::do_cpu<scatter_gather<A, F> >(graph_storage, ULONG_MAX);
      unsigned long updates_in_stream = (PHASE == 0 ? updates1_stream:updates0_stream);
      unsigned long updates_out_stream = (PHASE == 0 ? updates0_stream:updates1_stream);
      graph_storage->rewind_stream(edge_stream);
      for(unsigned long i=0;i<graph_storage->get_config()->super_partitions;i++) {
	if(graph_storage->get_config()->super_partitions > 1) {
	  if(sg_pcpu::bsp_phase > 0) {
	    graph_storage->state_load(vertex_stream, i);
	  }
	  graph_storage->state_prepare(i);
	}
	else if(sg_pcpu::bsp_phase == 0) {
	  graph_storage->state_prepare(0);
	}
	if(A::need_init(sg_pcpu::bsp_phase)) {
	  if(measure_scatter_gather) {
	    state_iter_cost.start();
	  }
	  x_lib::do_state_iter<scatter_gather<A, F> > (graph_storage, i);
	  if(measure_scatter_gather) {
	    state_iter_cost.stop();
	  }
	}
	sg_pcpu::current_step = phase_gather;
	if(measure_scatter_gather) {
	    gather_cost.start();
	}
	x_lib::do_stream<scatter_gather<A, F>, 
			 update_type_wrapper<A>,
			 update_type_wrapper<A> >
	  (graph_storage, i, updates_in_stream, ULONG_MAX, NULL);
	if(measure_scatter_gather) {
	  gather_cost.stop();
	}
	graph_storage->reset_stream(updates_in_stream, i);
	sg_pcpu::current_step = phase_scatter;
	x_lib::do_cpu<scatter_gather<A, F> >(graph_storage, i);
	if(measure_scatter_gather) {
	  scatter_cost.start();
	}
	x_lib::do_stream<scatter_gather<A, F>, 
			 edge_type_wrapper<F>,
			 update_type_wrapper<A> >
	  (graph_storage, i, edge_stream, updates_out_stream, sg_pcpu::scatter_filter);
	if(measure_scatter_gather) {
	  scatter_cost.stop();
	}
	sg_pcpu::current_step = phase_post_scatter;
	if(i == (graph_storage->get_config()->super_partitions - 1)) {
	  sg_pcpu::do_algo_reduce = true;
	}
	global_stop = x_lib::do_cpu<scatter_gather<A, F> >(graph_storage, i);
	sg_pcpu::do_algo_reduce = false;
	if(graph_storage->get_config()->super_partitions > 1) {
	  graph_storage->state_store(vertex_stream, i);
	}
      }
      graph_storage->rewind_stream(updates_out_stream);
      if(graph_storage->get_config()->super_partitions > 1) {
	graph_storage->rewind_stream(vertex_stream);
      }
      unsigned long no_voter;
      PHASE = 1 - PHASE;
      sg_pcpu::bsp_phase++;
      if(heartbeat) {
	BOOST_LOG_TRIVIAL(info) << clock::timestamp() << " Completed phase " <<
	  sg_pcpu::bsp_phase;
      }
      if(sg_pcpu::bsp_phase > A::min_super_phases()) {
	if(global_stop) {
	  break;
	}
	for(no_voter=0;no_voter<graph_storage->get_config()->processors;no_voter++) {
	  if(!pcpu_array[no_voter]->i_vote_to_stop) {
	    break;
	  }
	}
      	if((no_voter == graph_storage->get_config()->processors)){
	  break;
	}
      }
    }
    if(graph_storage->get_config()->super_partitions == 1) {
      graph_storage->state_store(vertex_stream, 0);
    }

    if(vm["benchmark"].as<std::string>() == "bfs") {
      strcpy(funName, "postprocessing_bfs");
      PyObject_CallMethod(pInstance, funName, NULL);
    }
    else if(vm["benchmark"].as<std::string>() == "pagerank") {
      strcpy(funName, "postprocessing_pagerank");
      PyObject_CallMethod(pInstance, funName, NULL);
    }
    Py_DECREF(pReturnValue);
    Py_DECREF(pName);
    delete funName;
    delete format;
    Py_Finalize();
 
    sg_pcpu::current_step = phase_terminate;
    x_lib::do_cpu<scatter_gather<A, F> >(graph_storage, ULONG_MAX);
    setup_time.start();
    graph_storage->terminate();
    setup_time.stop();
    wall_clock.stop();
    BOOST_LOG_TRIVIAL(info) << "CORE::PHASES " << sg_pcpu::bsp_phase;
    setup_time.print("CORE::TIME::SETUP");
    if(measure_scatter_gather) {
      state_iter_cost.print("CORE::TIME::STATE_ITER");
      gather_cost.print("CORE::TIME::GATHER");
      scatter_cost.print("CORE::TIME::SCATTER");
    }
    wall_clock.print("CORE::TIME::WALL");
  }

  template<typename A, typename F>
  void scatter_gather<A, F>::partition_pre_callback(unsigned long superp, 
						    unsigned long partition,
						    per_processor_data *pcpu)
  {
    sg_pcpu *pcpu_actual = static_cast<sg_pcpu *>(pcpu);
    if(pcpu_actual->current_step == phase_gather) {
      pcpu_actual->activate_partition_for_scatter = false;
    }
  }

  
  template<typename A, typename F>
  void scatter_gather<A, F>::partition_callback
  (x_lib::stream_callback_state *callback)
  {
    sg_pcpu *pcpu = static_cast<sg_pcpu *>(callback->cpu_state);

    char *functionNamePy = new char[20];
    char *format = new char[5];
    size_t size_params;
    unsigned char *buffer_params;
    size_t tmp_offset;
    size_t tmpSize;
    PyObject *pParamBuffer;
    PyObject *pMemVertices;
    PyObject *pMemInput;
    PyObject *pMemOutput;

    switch(sg_pcpu::current_step) {
    case phase_edge_split: {
      unsigned long bytes_to_copy = 
	(callback->bytes_in < callback->bytes_out_max) ?
	callback->bytes_in:callback->bytes_out_max;
      callback->bytes_in -= bytes_to_copy;
      memcpy(callback->bufout, callback->bufin, bytes_to_copy);
      callback->bufin += bytes_to_copy;
      callback->bytes_out = bytes_to_copy;
      break;
    }
    case phase_scatter: {
        unsigned long tmp = callback->bytes_in;
        unsigned char *bufout = callback->bufout;
        unsigned long bytes_in_returned;
        //M: Preparing parameters for scatter: bsp_phase, bytes_in, pbuffin_offset, state_offset, bytes_out_max
        size_params = sizeof(unsigned long)*5; 
        buffer_params = new unsigned char[size_params];
        tmp_offset = 0;
        tmpSize = sizeof(unsigned long);
        memcpy(&buffer_params[tmp_offset], &(sg_pcpu::bsp_phase), tmpSize);
        tmp_offset += sizeof(unsigned long);
        memcpy(&buffer_params[tmp_offset], &(callback->bytes_in), tmpSize);
        tmp_offset += sizeof(unsigned long);
        memcpy(&buffer_params[tmp_offset], &(callback->pbufin_offset), tmpSize);
        tmp_offset += sizeof(unsigned long);
        memcpy(&buffer_params[tmp_offset], &(callback->pstate_offset), tmpSize);
        tmp_offset += sizeof(unsigned long);
        memcpy(&buffer_params[tmp_offset], &(callback->bytes_out_max), tmpSize);
        
        npy_intp dim[1]={size_params};
        pParamBuffer =  PyArray_SimpleNewFromData(1, dim, NPY_UINT8, (void *)buffer_params);
        pMemVertices = PyMemoryView_FromObject(callback->pstate);
        pMemInput = PyMemoryView_FromObject(callback->pbufin);
        pMemOutput = PyMemoryView_FromObject(callback->pbufout);
        strcpy(format, "OOOO");
        if(vm["benchmark"].as<std::string>() == "bfs") {
          strcpy(functionNamePy, "do_scatter_bfs"); 
        }
        else if(vm["benchmark"].as<std::string>() == "pagerank"){
          strcpy(functionNamePy, "do_scatter_pgrnk");
        }
        pReturnValue = PyObject_CallMethod(pInstance, functionNamePy, format, pMemVertices, pMemInput, pMemOutput, pParamBuffer);
        if(PyTuple_Check(pReturnValue)){
          callback->bytes_out = PyLong_AsUnsignedLongMask(PyTuple_GetItem(pReturnValue, 0));
          bytes_in_returned = PyLong_AsUnsignedLongMask(PyTuple_GetItem(pReturnValue, 1));
          callback->bufin += (callback->bytes_in - bytes_in_returned);
          unsigned long bytes_in_old = callback->bytes_in;
          callback->bytes_in = bytes_in_returned;
          callback->pbufin_offset += (bytes_in_old - bytes_in_returned);
        }
        else{
          BOOST_LOG_TRIVIAL(info)<<"NOT A PY_TUPLE!";
          exit(-1);
        } 

      pcpu->update_bytes_out    += callback->bytes_out;
      pcpu->edge_bytes_streamed += (tmp - callback->bytes_in); 
      break;
    }
    case phase_gather: {
      pcpu->update_bytes_in += callback->bytes_in;

      //M: Preparing parameters for gather: bsp_phase, bytes_in, pbufin_offset, state_offset
      size_t size_params;
      size_params = sizeof(unsigned long)*4;
      buffer_params = new unsigned char[size_params];
      tmp_offset = 0;
      tmpSize = sizeof(unsigned long);
      memcpy(&buffer_params[tmp_offset], &(sg_pcpu::bsp_phase), tmpSize);
      tmp_offset += sizeof(unsigned long);
      memcpy(&buffer_params[tmp_offset], &(callback->bytes_in), tmpSize);
      tmp_offset += sizeof(unsigned long);
      memcpy(&buffer_params[tmp_offset], &(callback->pbufin_offset), tmpSize);
      tmp_offset += sizeof(unsigned long);
      memcpy(&buffer_params[tmp_offset], &(callback->pstate_offset), tmpSize);
      npy_intp dim[1]={size_params};
      pParamBuffer =  PyArray_SimpleNewFromData(1, dim, NPY_UINT8, (void *)buffer_params);
      pMemVertices = PyMemoryView_FromObject(callback->pstate);
      pMemInput = PyMemoryView_FromObject(callback->pbufin);
      strcpy(format, "OOO");
      if(vm["benchmark"].as<std::string>() == "bfs") {
        strcpy(functionNamePy, "do_gather_bfs");
      }
      else if(vm["benchmark"].as<std::string>() == "pagerank"){
        strcpy(functionNamePy, "do_gather_pgrnk");
      }
      pReturnValue = PyObject_CallMethod(pInstance, functionNamePy, format, pMemVertices, pMemInput, pParamBuffer);
      bool or_activate;
      if(PyLong_AsLong(pReturnValue)){
        or_activate = true;
      }
      else{
        or_activate = false;
      }
      pcpu->activate_partition_for_scatter = or_activate;
      pcpu->i_vote_to_stop = pcpu->i_vote_to_stop && !or_activate;
      callback->bufin += callback->bytes_in;
      callback->bytes_in = 0;
      
      break;
    }
    default:
      BOOST_LOG_TRIVIAL(fatal) << "Unknown operation in stream callback !";
      exit(-1);
    }

      delete format;
      delete functionNamePy;
      Py_DECREF(pParamBuffer);
      delete buffer_params; 
  }

  template<typename A, typename F>
  void scatter_gather<A, F>::partition_post_callback(unsigned long superp, 
						     unsigned long partition,
						     per_processor_data *pcpu)
  {
    sg_pcpu *pcpu_actual = static_cast<sg_pcpu *>(pcpu);
    if(pcpu_actual->current_step == phase_gather) {
      if(pcpu_actual->activate_partition_for_scatter) {
	sg_pcpu::scatter_filter->q(partition);
      }
      pcpu_actual->partitions_processed++;
    }
  }
    template<typename A, typename F>
    PyObject *scatter_gather<A, F>::pModule = NULL;
    template<typename A, typename F>
    PyObject *scatter_gather<A, F>::pName = NULL; 
    template<typename A, typename F>
    PyObject *scatter_gather<A, F>::pDict = NULL;
    template<typename A, typename F> 
    PyObject *scatter_gather<A, F>::pClass = NULL;
    template<typename A, typename F>
    PyObject *scatter_gather<A, F>::pInstance = NULL;
    template<typename A, typename F>
    PyObject *scatter_gather<A, F>::pReturnValue = NULL;
}
#endif 
