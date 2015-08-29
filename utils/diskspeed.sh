#!/bin/bash
DIR=/tmp/
SIZE=4G
start="4096"
end="16777216"
rm -f diskspeed.out
echo "# blocksize read(KB/s) write(KB/s)" > diskspeed.out
i=$start
while [ $i -le $end ]
do
    rm -f dump
    echo -n "$i " >> diskspeed.out
    fio --name=test --runtime=300 --ioengine=sync --direct=1 --rw=read --bs=${i} --size=${SIZE} --directory=${DIR} --minimal --output dump
    cat dump | grep ';' | cut -d ';' -f 6 | xargs echo -n >> diskspeed.out
    echo -n " " >> diskspeed.out
    cat dump | grep ';' | cut -d ';' -f 20 | xargs echo -n >> diskspeed.out
    echo -n " " >> diskspeed.out
    cat dump | grep ';' | cut -d ';' -f 21 | xargs echo -n >> diskspeed.out
    echo -n " " >> diskspeed.out
    cat dump | grep ';' | cut -d ';' -f 23 | xargs echo -n >> diskspeed.out
    echo -n " " >> diskspeed.out
    fio --name=test --runtime=300 --ioengine=sync --direct=1 --rw=write --bs=${i} --size=${SIZE} --directory=${DIR} --minimal --output dump
    cat dump | grep ';' | cut -d ';' -f 26 | xargs echo -n >> diskspeed.out
    echo -n " " >> diskspeed.out
    cat dump | grep ';' | cut -d ';' -f 40 | xargs echo -n >> diskspeed.out
    echo -n " " >> diskspeed.out
    cat dump | grep ';' | cut -d ';' -f 41 | xargs echo -n >> diskspeed.out
    echo -n " " >> diskspeed.out
    cat dump | grep ';' | cut -d ';' -f 43 | xargs echo -n >> diskspeed.out
    echo " " >> diskspeed.out
    let "i *= 2"
done