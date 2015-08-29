#
# X-Stream
#
# Copyright 2013 Operating Systems Laboratory EPFL
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

from ctypes import *
import struct
import numpy as np
import sys
import traceback
import time

class pgrnk_vertex(Structure):
	_fields_ = [
	("degree", c_uint),
	("rank", c_float),
	("sum", c_float),
	]

class pgrnk_update(Structure):
	_fields_ = [
	("target", c_uint),
	("rank", c_float),
	]

class AlgorithmPagerank:
	#Common parameters
	partition_shift = 0
	super_partition_shift = 0
	edge_size = 0
	total_time_python = 0
	#Pagerank parameters
	DAMPING_FACTOR = 0.85
	niters = 4

	def initialization_pgrnk(self, partition_shift, super_partition_shift, edge_size):
			AlgorithmPagerank.partition_shift = partition_shift
			AlgorithmPagerank.super_partition_shift = super_partition_shift
			AlgorithmPagerank.edge_size = edge_size

	def do_scatter_pgrnk(self, mem_vertices, mem_edges, mem_output, params_stream):
			try:
				start_scatter_time = time.clock()
				offset_params = 0
				offset_edges = 0
				offset_updates = 0
				bytes_out = c_ulong(0)
				bsp_phase = struct.unpack('L', params_stream[offset_params:offset_params+sizeof(c_ulong)])[0]
				offset_params += sizeof(c_ulong)
				bytes_in = struct.unpack('L', params_stream[offset_params:offset_params+sizeof(c_ulong)])[0]
				offset_params += sizeof(c_ulong)
				start_edges = struct.unpack('L', params_stream[offset_params:offset_params+sizeof(c_ulong)])[0]
				offset_params += sizeof(c_ulong)
				state_offset = struct.unpack('L', params_stream[offset_params:offset_params+sizeof(c_ulong)])[0]
				offset_params += sizeof(c_ulong)
				bytes_out_max = struct.unpack('L', params_stream[offset_params:offset_params+sizeof(c_ulong)])[0]
				offset_params += sizeof(c_ulong)
				counter = 0

				while bytes_in > 0:
					if bytes_out.value + sizeof(pgrnk_update) > bytes_out_max:
						break
					update_generated = self.generate_update_pgrnk(mem_edges, offset_edges, state_offset, offset_updates, mem_vertices, mem_output, bsp_phase)
					offset_edges += AlgorithmPagerank.edge_size
					bytes_in -= AlgorithmPagerank.edge_size
					if update_generated:
						bytes_out.value += sizeof(pgrnk_update)
						offset_updates += sizeof(pgrnk_update)
					counter += 1
				end_scatter_time = time.clock()	
				AlgorithmPagerank.total_time_python += (end_scatter_time - start_scatter_time)
				return (bytes_out.value, bytes_in)

			except Exception as inst:
				exc_type, exc_value, exc_traceback = sys.exc_info()
				traceback.print_tb(exc_traceback, limit=1, file=sys.stdout)
				print(type(inst))
				print(inst)
				raise

	def do_gather_pgrnk(self, mem_vertices, mem_updates, params_stream):
			try:
				start_gather_time = time.clock()
				offset_params = 0
				offset_updates = 0
				local_vertices_discovered = 0
				bsp_phase = struct.unpack('L', params_stream[offset_params:offset_params+sizeof(c_ulong)])[0]
				offset_params += sizeof(c_ulong)
				bytes_in = struct.unpack('L', params_stream[offset_params:offset_params+sizeof(c_ulong)])[0]
				offset_params += sizeof(c_ulong)
				start_updates = struct.unpack('L', params_stream[offset_params:offset_params+sizeof(c_ulong)])[0]
				offset_params += sizeof(c_ulong)
				state_offset = struct.unpack('L', params_stream[offset_params:offset_params+sizeof(c_ulong)])[0]
				counter = 0
				activate_OR = False
				while(bytes_in > 0):
					activate = self.apply_one_update_pgrnk(mem_updates, offset_updates, mem_vertices, bsp_phase, start_updates, state_offset)
					offset_updates += sizeof(pgrnk_update)
					bytes_in -= sizeof(pgrnk_update)
					activate_OR = activate_OR or activate
					counter += 1
				end_gather_time = time.clock()
				AlgorithmPagerank.total_time_python += (end_gather_time - start_gather_time)
				return (activate_OR, mem_vertices)

			except Exception as inst:
				exc_type, exc_value, exc_traceback = sys.exc_info()
				traceback.print_tb(exc_traceback, limit=1, file=sys.stdout)
				print(type(inst))
				print(inst)
				raise

	def generate_update_pgrnk(self, edges_stream, offset_edges, state_offset, offset_updates,
					vertices_stream, mem_output, bsp_phase):
		edge = struct.unpack('II', edges_stream[offset_edges:offset_edges+2*sizeof(c_uint)])
		vindex = self.map_offset(edge[0], AlgorithmPagerank.super_partition_shift, AlgorithmPagerank.partition_shift)
		#print("State offset = ", state_offset)
		vertex = struct.unpack('Iff', vertices_stream[state_offset+vindex*sizeof(pgrnk_vertex):state_offset+vindex*sizeof(pgrnk_vertex)+sizeof(pgrnk_vertex)])
		if bsp_phase == 0:
			v_degree = vertex[0]+1
			vertices_stream[state_offset+vindex*sizeof(pgrnk_vertex):state_offset+vindex*sizeof(pgrnk_vertex)+sizeof(c_uint)] = struct.pack('I', v_degree)
			return False
		else:
			mem_output[offset_updates:offset_updates + sizeof(c_uint)] = struct.pack('I', edge[1])
			u_rank = vertex[1]/vertex[0]
			mem_output[offset_updates + sizeof(c_uint):offset_updates + sizeof(c_uint) + sizeof(c_float)] = struct.pack('f', u_rank)
			u_sum = 0
			vertices_stream[state_offset+vindex*sizeof(pgrnk_vertex)+sizeof(c_uint)+sizeof(c_float):state_offset+vindex*sizeof(pgrnk_vertex)+sizeof(c_uint)+2*sizeof(c_float)] = struct.pack('f',u_sum)
			return True

	def apply_one_update_pgrnk(self, updates, updates_offset, vertices_bytes, 
		bsp_phase, start_updates, state_offset):
		update = struct.unpack('If', updates[start_updates+updates_offset:start_updates+updates_offset+sizeof(pgrnk_update)])
		vindex = self.map_offset(update[0], AlgorithmPagerank.super_partition_shift, AlgorithmPagerank.partition_shift)
		vertex = struct.unpack('Iff', vertices_bytes[state_offset+vindex*sizeof(pgrnk_vertex):state_offset+vindex*sizeof(pgrnk_vertex)+sizeof(pgrnk_vertex)])
		v_sum = vertex[2] + update[1]
		v_rank = 1 - AlgorithmPagerank.DAMPING_FACTOR + AlgorithmPagerank.DAMPING_FACTOR * v_sum
		vertices_bytes[state_offset+vindex*sizeof(pgrnk_vertex)+sizeof(c_uint):state_offset+vindex*sizeof(pgrnk_vertex)+sizeof(c_uint)+2*sizeof(c_float)]=struct.pack('ff', v_rank, v_sum)
		if bsp_phase==AlgorithmPagerank.niters:
			#print("RANK: ", v_rank)
			return False
		else:
			return True

	def map_offset(self, key, super_partition_shift, partition_shift):
		return key >> (super_partition_shift + partition_shift)

	def postprocessing_pagerank(self):
		print("Total time in python: ", AlgorithmPagerank.total_time_python)
