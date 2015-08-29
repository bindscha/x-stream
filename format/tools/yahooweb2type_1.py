#Convert yahoo web into x-stream type 1 inputs
#Arguments <name of yahoo web file> <output file name>
import sys
import struct
import random
import io

#Choose one
#add_rev_edges = False
add_rev_edges = True

random.seed(0)

infile=file(sys.argv[1], "r")
#outfile=file(sys.argv[2], "wtb")
outfile_stream=io.open(sys.argv[2], "wb", 16777216)
outfile_meta=file(sys.argv[2]+".ini", "wt")

outfile_meta.write("[graph]\n")
outfile_meta.write("type=1\n")
outfile_meta.write("name="+sys.argv[2]+"\n")

s = struct.Struct('@IIf')
edges=0
vertices=0
for line in infile:
    if line[0] == '%':
        pass
    else:
        vector = line.strip().split(" ")
        vector = list(map(int, vector))
        src=vector[0]
        if src > vertices:
            vertices = src
        for i in vector[2:]:
            weight = random.random()
            if i > vertices:
                vertices = i
            out = [src, i, weight]
            #print out
            outfile_stream.write(s.pack(*out))
            edges = edges + 1
            if add_rev_edges:
                out = [i, src, weight]
                #print out
                outfile_stream.write(s.pack(*out))
                edges = edges + 1

vertices = vertices + 1
outfile_meta.write("vertices="+str(vertices)+"\n")
outfile_meta.write("edges="+str(edges)+"\n")
infile.close()
#outfile.close()
outfile_meta.close()
