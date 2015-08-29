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

class bfs_update(Structure):
	_fields_ = [
	("parent", c_uint),
	("child", c_uint),
      ]

class bfs_vertex(Structure):
	_fields_ = [
	("bfs_parent", c_uint),
	("bsp_phase", c_uint),
      ]

class AlgorithmBFS:
	#Common parameters
	partition_shift = 0
	super_partition_shift = 0
	edge_size = 0
	total_time_python = 0
	#BFS parameters
	vertices_discovered = 0
	edges_explored = 0

	def initialization_bfs(self, partition_shift, super_partition_shift, edge_size):
			AlgorithmBFS.partition_shift = partition_shift
			AlgorithmBFS.super_partition_shift = super_partition_shift
			AlgorithmBFS.edge_size = edge_size

	def do_scatter_bfs(self, mem_vertices, mem_edges, mem_output, params_stream):
			
			try:
				#unpacking parameters
				start_scatter_time = time.clock()
				offset_params = 0
				offset_edges = 0
				offset_updates = 0
				local_edges_explored = 0
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
				edges_stream = mem_edges[start_edges:start_edges+bytes_in]
				counter = 0
				while bytes_in > 0:
					if bytes_out.value + sizeof(bfs_update) > bytes_out_max:
						break
					update_generated = self.generate_update_bfs(edges_stream, offset_edges, state_offset, offset_updates, mem_vertices, mem_output, bsp_phase)
					offset_edges += AlgorithmBFS.edge_size
					bytes_in -= AlgorithmBFS.edge_size
					if update_generated:
						local_edges_explored += 1
						bytes_out.value += sizeof(bfs_update)
						offset_updates += sizeof(bfs_update)
					counter += 1
				AlgorithmBFS.edges_explored += local_edges_explored
				end_scatter_time = time.clock()	
				AlgorithmBFS.total_time_python += (end_scatter_time - start_scatter_time)
				return (bytes_out.value, bytes_in)

			except Exception as inst:
				exc_type, exc_value, exc_traceback = sys.exc_info()
				traceback.print_tb(exc_traceback, limit=1, file=sys.stdout)
				print(type(inst))
				print(inst)
				raise

	def do_gather_bfs(self, mem_vertices, mem_updates, params_stream):	
		
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
				activate = self.apply_one_update_bfs(mem_updates, offset_updates, 
					mem_vertices, bsp_phase, start_updates, state_offset)
				if activate:
					local_vertices_discovered += 1
				offset_updates += sizeof(bfs_update)
				bytes_in -= sizeof(bfs_update)
				activate_OR = activate_OR or activate
				counter += 1
			AlgorithmBFS.vertices_discovered += local_vertices_discovered
			end_gather_time = time.clock()
			AlgorithmBFS.total_time_python += (end_gather_time - start_gather_time)
			return (activate_OR, mem_vertices)
		except Exception as inst:
			print(type(inst))
			print(inst)
			raise

	def generate_update_bfs(self, edges_stream, offset_edges, state_offset, offset_updates,
					vertices_stream, mem_output, bsp_phase):
		#should be changed depending on the type of egde!! now it is (src, dst) type
		edge = struct.unpack('II', edges_stream[offset_edges:offset_edges+2*sizeof(c_uint)])
		vindex = self.map_offset(edge[0], AlgorithmBFS.super_partition_shift, AlgorithmBFS.partition_shift)
		current_vertex = struct.unpack('II', vertices_stream[state_offset+vindex*sizeof(bfs_vertex):state_offset+vindex*sizeof(bfs_vertex)+sizeof(bfs_vertex)])
		if current_vertex[0] != 4294967295 and  current_vertex[1] == bsp_phase:
			single_update = bfs_update(edge[0], edge[1])
			mem_output[offset_updates:offset_updates + sizeof(c_uint)] = struct.pack('I', single_update.parent)
			mem_output[offset_updates + sizeof(c_uint):offset_updates + 2*sizeof(c_uint)] = struct.pack('I', single_update.child)
			return True
		else:
			return False

	def apply_one_update_bfs(self, updates, updates_offset, vertices_bytes, 
		bsp_phase, start_updates, state_offset):
		current_update = struct.unpack('II', updates[start_updates+updates_offset:start_updates+updates_offset+2*sizeof(c_uint)])
		vindex = self.map_offset(current_update[1], AlgorithmBFS.super_partition_shift, AlgorithmBFS.partition_shift)
		child_vertex = self.read_vertex(vertices_bytes, vindex, state_offset)
		if child_vertex[0] == 4294967295:
			new_parent = current_update[0]
			new_phase = bsp_phase
			vertices_bytes[vindex*sizeof(bfs_vertex):vindex*sizeof(bfs_vertex)+sizeof(bfs_vertex)] = struct.pack('II', new_parent, new_phase)	
			return True
		else:
			return False

	def map_offset(self, key, super_partition_shift, partition_shift):
		return key >> (super_partition_shift + partition_shift)

	def read_vertex(self, vertices_bytes, vindex, state_offset):
		tmp = vertices_bytes[state_offset + vindex*sizeof(bfs_vertex):state_offset + vindex*sizeof(bfs_vertex)+sizeof(bfs_vertex)]
		return struct.unpack('II', tmp)

	def postprocessing_bfs(self):
		print("PYTHON::BFS::VERTICES_DISCOVERED ", AlgorithmBFS.vertices_discovered);
		print("PYTHON::BFS::EDGES_EXPLORED ", AlgorithmBFS.edges_explored);
		print("Total time in python: ", AlgorithmBFS.total_time_python)

	
