# HDFS Support for X-Stream #

**X-Stream** is a single-machine graph processing system based on the idea of streaming edges from secondary storage.

However, the processing ability of X-Stream is limited by the capacity of local storage in a single machine. Also graph data are usually stored in HDFS, not local file system.

These observation motivates us to add HDFS support for X-Stream, and hence leads to this project.

##Solution Overview##

- libhdfs: a JNI-based API in hadoop project
- libhdfs: same API as libhdfs, based on RPC
- hdfs-fuse: mount HDFS like a disk

##How to Setup##

###libhdfs3###

libhdfs3 is easy to deploy and use. You can read the setup document on the github page of libhdfs3. [https://github.com/PivotalRD/libhdfs3/wiki/Get-Started](https://github.com/PivotalRD/libhdfs3/wiki/Get-Started)
<br/>
Things are much easier if you are using ubuntu 12.04. You can install the dependencies of libhdfs3 by following commands.
> sudo add-apt-repository -y ppa:boost-latest/ppa



> sudo add-apt-repository -y ppa:chris-lea/protobuf



> sudo apt-get update



> sudo apt-get install -qq cmake libxml2 libxml2-dev uuid-dev protobuf-compiler libprotobuf-dev libgsasl7-dev libkrb5-dev libboost1.54-all-dev

Assume libhdfs3 top source directory is LIBHDFS3_HOME. Following commands are used to build libhdfs3.

    cd LIBHDFS3_HOME

    mkdir build
    
    cd build
    
    ../bootstrap
    
    make install

Then you need to make sure the header files of libhdfs3 are in /usr/include or /usr/local/include, and the built lib files are in /usr/lib or /usr/local/lib. If not, you should copy them to corresponding folder or create the symbol link manually.

In the Makefile of X-Stream, there is a parameter linking X-Stream to libhdfs3, namely -lhdfs3.

###libhdfs###

libhdfs is prebuilt in hadoop project, so in order to use this library, you need to have a already compiled hadoop project.

And what you need to do is adding hdfs jars which libhdfs uses into the CLASSPATH of java. You should notice that **wildcard is not allowed in CLASSPATH** due to JNI.

To use libhdfs, you should change -lhdfs3 to -lhdfs, add -ljvm, and use -L parameter in case that the compiler does not recognize -lhdfs. There is a sample commented after -lhdfs3 in Makefile.

###hdfs_fuse###
At first, you need to download download cdh5-repository_1.0\_all.deb. (Attention: You should the right version of cdh corresponding to the version of your hadoop.) 

Below is a sample for setting up hdfs_fuse.

> wget http://archive.cloudera.com/cdh5/one-click-install/precise/amd64/cdh5-repository_1.0\_all.deb
>
> sudo dpkg -i cdh5-repository_1.0\_all.deb
> 
> curl -s http://archive.cloudera.com/cdh5/ubuntu/precise/amd64/cdh/archive.key | sudo apt-key add -
> 
> sudo apt-get update
> 
> sudo apt-get install hadoop-hdfs-fuse
> 
> sudo hadoop-fuse-dfs dfs://labossrv14:9000 /export/hdfs/

In the last command of above example, we mount the hdfs to the local file path /export/hdfs. And you can umount it like below.

> umount /export/hdfs

##How to Use X-Stream with HDFS##

Generally, there is not significant difference between using X-Stream with HDFS library and without these libraries. Therefore, you should take a look at the README file at first.

To use libhdfs3, you should build X-Stream with libhdfs3. Remember the only thing you need to change is the link option in Makefile. Then you can run X-Stream like this:

    benchmark_driver -g twitter_rv -b pagerank --pagerank::niters 10 -a -p 16 --physical_memory 268435456 --hdfs --filename result.csv --graphpath /hpgp_data

Here the parameter --hdfs is telling X-Stream to run with hdfs library, --filename is the output file for performance evaluation, and --graphpath is the file folder where you store graph dataset in hdfs. Another thing you should pay attention to is that you still need to have a local .ini file for the graph.

It's totally the same thing when you try to use libhdfs. You just change the linker information in Makefile and rebuild X-Stream.

For hdfs_fuse, you don't need to do any special thing for Makefile. Just replace --hdfs with --fuse like following.
    
    benchmark_driver -g twitter_rv -b pagerank --pagerank::niters 10 -a -p 16 --physical_memory 268435456 --fuse --filename result.csv --graphpath /export/hdfs/hpgp_data

You must find that --graphpath is local file path now, because you have mounted hdfs like a local disk.