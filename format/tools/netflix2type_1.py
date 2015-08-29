#Convert netflix prize files into x-stream type 1 inputs
#Arguments <name of matrix market file> <output file name>
import sys
import struct
import random

#Choose one
add_rev_edges = False
#add_rev_edges = True

movies      = 17770
users       = 480189
user_ids    = 2649429 
user_id_map = []

for i in range(user_ids):
    user_id_map.append(-1)

outfile=file(sys.argv[1], "wtb")
outfile_meta=file(sys.argv[1]+".ini", "wt")
outfile_meta.write("[graph]\n")
outfile_meta.write("type=1\n")
outfile_meta.write("name="+sys.argv[1]+"\n")
outfile_meta.write("vertices=" + str(users+movies) + "\n")
s = struct.Struct('@IIf')

edges = 0

for i in range(movies):
    infile=file("mv_" + str(i+1).zfill(7) + ".txt", "r")
    infile.readline()
    for line in infile:
        vector = line.strip().split(",")
        vec_out = [0, 0, 0]
        vec_out[0] = i
        user_id = int(vector[0]) - 1
        if user_id_map[user_id] == -1:
            user_id_map[user_id] = users - 1
            users = users - 1
        vec_out[1] = user_id_map[user_id] + movies
        vec_out[2] = float(vector[1])
        outfile.write(s.pack(*vec_out))
        edges = edges + 1
        if add_rev_edges:
            tmp = vec_out[0]
            vec_out[0] = vec_out[1]
            vec_out[1] = tmp
            outfile.write(s.pack(*vec_out))
            edges = edges + 1
    infile.close()
    
outfile.close()
outfile_meta.write("edges=" + str(edges) + "\n")
outfile_meta.close()

