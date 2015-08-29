X-Stream
========

What is it?
-----------

X-Stream is a graph processing system for analytics on big graphs using a single machine. X-Stream is based on the philosophy that sequential access to data works best for all types of storage media: main memory, SSD and magnetic disk. It exposes the familiar scatter-gather programming model, but is based on the principle of streaming data from storage.

Installation
------------
Please see the file called INSTALL.

Getting started
---------------

X-Stream expects the input graphs in the binary edge list format. For details see the file called TYPES in the format directory. In addition to the edge list file, X-Stream expects an ini file that specifies the graph type, the number of vertices and the number of edges. Provided with X-Stream are two synthetic graph generators - a random graph generator and an RMAT graph generator. Thay also generate the needed ini files. For example, to create a scale-free graph with 1M vertices and 16M edges run:
    
    $ rmat --name test --scale 20 --edges 16777216

This creates a directed graph. To create an undirected graph instead, use `--symmetric`. Note that some algorithms require directed and some undirected graphs to work properly.

Directory format/tools contains a number of scripts to convert graphs from different formats into the format that X-Stream expects.

To run any of the currently implemented algorithms in X-Stream, you can use the provided example program called benchmark_driver. To see supported options run it with `-h`. For example, to run 10 iterations of pagerank on the test graph generated above, invoke benchmark_driver as:

    $ benchmark_driver -g test -b pagerank --pagerank::niters 10 -a -p 16 --physical_memory 268435456

This will run X-Stream with 16 worker threads and with 256MB of physical memory allotted to it. The -a flag tells X-Stream to automatically tune the number of partitions and other system parameters. In addition to the number of threads and the amount of physical memory available, you might also want to tune the per-processor cache size and cacheline size parameters.

Please note that that if you do not run the program as superuser, you will receive a number of error messages about being unable to "mlock" memory pages. These are not fatal errors, *X-stream will execute successfully*. However, as X-Stream does its own buffer management, runtime numbers can show some variation if the OS decides to page out X-Stream's buffers or due to the cost of the initial minor faults on small runs. For benchmarking performance, running as superuser is the right thing to do.

Licensing
---------
Please see the file called LICENSE.

Python support
--------------
X-Stream has experimental Python support -- algorithms written in Python can be executed using the X-Stream C++ engine. To turn Python support on, uncomment relevant lines in the Makefile. For examples how to write algorithms in Python, check *algorithms/bfs/bfs.py* and
*algorithms/pagerank/pagerank.py*.

Compression
-----------

X-Stream also has experimental zlib support for processing compressed graphs. To turn it on, uncomment relevant lines in the Makefile.

Benchmarking
-----------
If you are benchmarking X-Stream, particularly in comparison to Graphchi, please be sure to read the [following guide](http://labos.epfl.ch/files/content/sites/labos/files/x-stream-code/benchmarking.pdf).

There are also some tuning parameters that can help improve X-Stream's performance in some cases.

a. X-Stream incorporates an autotuner that picks good streaming partition sizes. The autotuner however is biased towards running efficiently from disk and does not do a good job of squeezing things into memory. The following parameters can sometime force the graph to run from memory and avoid disk I/O:

    --force_buffers 2

b. X-Stream uses direct I/O. The assumption here is that you are dealing with graphs *much* (orders of magnitude) larger than the RAM in your machine. If you use small graphs you might be better off by using the pagecache instead. To do so give X-Stream half the physical memory on your machine and use the following parameter

    --no_dio

Contact
-------
- Laurent Bindschaedler <laurent.bindschaedler@epfl.ch>
- Jasmina Malicevic <jasmina.malicevic@epfl.ch>
- Amitabha Roy <amitabha.roy@gmail.com>
