#Convert matrix market files into x-stream type 1 inputs
#Arguments <name of matrix market file> <output file name>
import sys
import struct
import random

#Choose one
add_rev_edges = False
#add_rev_edges = True

random.seed(0)

infile=file(sys.argv[1], "r")
outfile=file(sys.argv[2], "wtb")
outfile_meta=file(sys.argv[2]+".ini", "wt")

outfile_meta.write("[graph]\n")
outfile_meta.write("type=1\n")
outfile_meta.write("name="+sys.argv[2]+"\n")

s = struct.Struct('@IIf')

for line in infile:
    if line[0] == '%':
        pass
    else:
        vector = line.strip().split(" ")
        if len(vector) == 3:
            outfile_meta.write("vertices="+vector[0]+"\n")
            if add_rev_edges:
                vector[2] = str(2 * int(vector[2]))
                outfile_meta.write("edges="+vector[2]+"\n")
            else:
                outfile_meta.write("edges="+vector[2]+"\n")

        else:
            vector = list(map(int, vector))
            vector[0] = vector[0] - 1
            vector[1] = vector[1] - 1
            vector.append(random.random()) # Edge weight
            outfile.write(s.pack(*vector))
            if add_rev_edges:
                tmp = vector[0]
                vector[0] = vector[1]
                vector[1] = tmp
                outfile.write(s.pack(*vector))

infile.close()
outfile.close()
outfile_meta.close()
