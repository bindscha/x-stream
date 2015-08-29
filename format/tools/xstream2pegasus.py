# Converts X-Stream input into Pegasus-compatible input
import sys
import struct

print 'Converting from ' + sys.argv[1] + ' to ' + sys.argv[2]

with open(sys.argv[1],'rb') as infile:
    with open(sys.argv[2],'w') as outfile:
       for chunk in iter((lambda:infile.read(12)),''):
           source = struct.unpack('I', chunk[0:4])[0]
           target = struct.unpack('I', chunk[4:8])[0]
           outfile.write(str(source) + '\t' + str(target) + '\n')

print 'Done!'
