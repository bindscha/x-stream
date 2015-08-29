#!/bin/bash
BIN=../x-stream/bin
chunk_size=8
start_thread_cnt=1
end_thread_cnt=4
#rm -f memspeed.seq.out
#rm -f memspeed.rand.out
printf "# %-15s%-15s%-15s%-15s%-15s\n" "threads" "chunk_size" "read(MB/s)" "write(MB/s)" "memcpy(MB/s)" >>  memspeed.seq.out
printf "# %-15s%-15s%-15s%-15s%-15s\n" "threads" "chunk_size" "read(MB/s)" "write(MB/s)" "memcpy(MB/s)" >>  memspeed.rand.out
for i in `seq $start_thread_cnt 1 $end_thread_cnt`
do
numactl --interleave=all ${BIN}/mem_speed_sequential ${chunk_size} ${i} 2> dump
numactl --interleave=all ${BIN}/mem_speed_sequential ${chunk_size} ${i} 2>> dump
numactl --interleave=all ${BIN}/mem_speed_sequential ${chunk_size} ${i} 2>> dump
numactl --interleave=all ${BIN}/mem_speed_sequential ${chunk_size} ${i} 2>> dump
memread=$(cat dump | grep MEMREAD | awk '{sum += $4} END {printf "%f ",sum/4}')
memwrite=$(cat dump | grep MEMWRITE | awk '{sum += $4} END {printf "%f ",sum/4}')
memcpy=$(cat dump | grep MEMCPY | awk '{sum += $4} END {printf "%f ",sum/4}')
printf "  %-15s%-15s%-15s%-15s%-15s\n" $i $chunk_size $memread $memwrite $memcpy >> memspeed.seq.out
numactl --interleave=all ${BIN}/mem_speed_random ${chunk_size} ${i} 2> dump
numactl --interleave=all ${BIN}/mem_speed_random ${chunk_size} ${i} 2>> dump
numactl --interleave=all ${BIN}/mem_speed_random ${chunk_size} ${i} 2>> dump
numactl --interleave=all ${BIN}/mem_speed_random ${chunk_size} ${i} 2>> dump
memread=$(cat dump | grep MEMREAD | awk '{sum += $4} END {printf "%f ",sum/4}')
memwrite=$(cat dump | grep MEMWRITE | awk '{sum += $4} END {printf "%f ",sum/4}')
memcpy=$(cat dump | grep MEMCPY | awk '{sum += $4} END {printf "%f ",sum/4}')
printf "  %-15s%-15s%-15s%-15s%-15s\n" $i $chunk_size $memread $memwrite $memcpy >> memspeed.rand.out
rm -f dump
done
