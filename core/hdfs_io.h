//created by Junyao, 2015-7-6
#ifndef _HDFS_IO_
#define _HDFS_IO_
//Use libhdfs3 by default
//To use libhdfs, comment next line and modify the Makefile
#define _LIB_HDFS_3_
#include <iostream>
#include <map>
#include <climits>
#include <string>
#ifdef _LIB_HDFS_3_
#include <hdfs/hdfs.h>
#else
#include "/home/junzhao/hadoop/hadoop-2.5.2/include/hdfs.h"
#endif
#include <boost/thread/pthread/mutex.hpp>

namespace x_lib{
    //Class hdfs_io is a singleton
    class hdfs_io{
        unsigned int filecnt;
        struct hdfsBuilder *builder;
        hdfsFS fs;
        boost::mutex mtx;
	//File description for read operation
        std::map<unsigned int, hdfsFile> fileRDmap;
	//File description for write operation
        std::map<unsigned int, hdfsFile> fileWRmap;
	//Mapping from file ID to file path      
	std::map<unsigned int, std::string> pathmap;
	//Record whether the file needs to be reopened for write operation
        std::map<unsigned int, bool> needRefreshWR;

	//Connect to hdfs
        hdfs_io(const char* nn_address = "labossrv14.epfl.ch", int nn_port = 9000){
            builder = hdfsNewBuilder();
            hdfsBuilderSetNameNode(builder, nn_address);
            hdfsBuilderSetNameNodePort(builder, nn_port);
            fs = hdfsBuilderConnect(builder);
            filecnt= 0;
        }
	
	//Overload copy constructor and equal operator for singleton pattern
        hdfs_io(hdfs_io const&); 
	void operator=(hdfs_io const&);
        
	public:
            static const int RDONLY = 1;
            static const int WRONLY = 2;
            static const int APPEND = 4;
            static const int SYNC = 8;

            //Call this function to get the singleton instance
	    static hdfs_io& get_instance(){
                static hdfs_io _hdfs_io;
                return _hdfs_io;
            }

            /*~hdfs_io(){
                hdfsFreeBuilder(builder);
            }*/

            /*
             * @param flags:
             * supported flags are hdfs_io::RDONLY, hdfs_io::WRONLY, hdfs_io::APPEND and hdfs_io::SYNC,
             * Other flags are generally ignored other than (O_RDWR || (O_EXCL & O_CREAT)) which return NULL and set errno equal ENOTSU
            */
            int open(const char* filename, int flags){            
                if(flags == RDONLY){
                    hdfsFile filehandle = hdfsOpenFile(fs, filename, O_RDONLY, 0/*not used*/, 0/*use default value*/, 0/*use default value*/);
                    mtx.lock();
                    while(fileRDmap[filecnt]) {
                        filecnt = (filecnt + 1) % UINT_MAX;
                    }
                    fileRDmap[filecnt] = filehandle;
                    std::string name = filename;
                    pathmap[filecnt] = name;
                    mtx.unlock();
                }
                else if(flags == WRONLY){
                    hdfsFile filehandle = hdfsOpenFile(fs, filename, O_WRONLY, 0/*not used*/, 1/*replica*/, 0/*use default value*/);
                    mtx.lock();
                    while(fileWRmap[filecnt]) {
                        filecnt = (filecnt + 1) % UINT_MAX;
                    }
                    fileWRmap[filecnt] = filehandle;
                    std::string name = filename;
                    pathmap[filecnt] = name;
                    mtx.unlock();
                }
                else if(flags == (WRONLY | RDONLY)){
                    hdfsFile fileWRhandle = hdfsOpenFile(fs, filename, O_WRONLY, 0/*not used*/, 1/*replica*/, 0/*use default value*/);
                    hdfsFile fileRDhandle = hdfsOpenFile(fs, filename, O_RDONLY, 0/*not used*/, 0/*use default value*/, 0/*use default value*/);
                    mtx.lock();
                    while(fileRDmap[filecnt]) {
                        filecnt = (filecnt + 1) % UINT_MAX;
                    }
                    fileRDmap[filecnt] = fileRDhandle;
                    fileWRmap[filecnt] = fileWRhandle;
                    std::string name = filename;
                    pathmap[filecnt] = name;
                    mtx.unlock();
                }
                else{
                    //hdfs_io::APPEND and hdfs_io::SYNC shouldn't be called at present
                }
                return filecnt;
            }
	    
	    //Close the file description for write operation
            void turnOnRefreshWR(int fd){
                if(!needRefreshWR[fd]){
                    mtx.lock();
		    /*int retval = hdfsFlush(fs, fileWRmap[fd]);
		    if(retval){
			std::cout << "flush failed..." << std::endl;
	 	    }*/
                    retval = hdfsCloseFile(fs, fileWRmap[fd]);
		    needRefreshWR[fd] = true;
                    mtx.unlock();
                }
            }
	    
  	    //Reopen the file desription for read operation
            void refreshReadHandle(int fd){
                mtx.lock();
                //lseek(fd, 0);
		int closeretval = hdfsCloseFile(fs, fileRDmap[fd]);
                hdfsFile newfilehandle = hdfsOpenFile(fs, pathmap[fd].c_str(), O_RDONLY, 0/*not used*/, 0/*use default value*/, 0/*use default value*/);
                fileRDmap.erase(fd);
                fileRDmap[fd] = newfilehandle;
                mtx.unlock();
            }

	    //Reopen the file desription for write operation
            void refreshWriteHandle(int fd){
                if(needRefreshWR[fd]){
                    mtx.lock();
                    hdfsFile newfilehandle = hdfsOpenFile(fs, pathmap[fd].c_str(), O_WRONLY, 0/*not used*/, 1/*use default value*/, 0/*use default value*/);
                    fileWRmap[fd] = newfilehandle;
                    needRefreshWR[fd] = false;
                    mtx.unlock();
                }
            }
	    
	    //Flush to HDFS, not called at present
            void flush(int fd){
                hdfsFlush(fs, fileWRmap[fd]);
            }
	    
	    //Close all the file descriptions relevant to file ID fd
            void close(int fd){
                mtx.lock();
                if (fileRDmap[fd]) {
                    int closeretval = hdfsCloseFile(fs, fileRDmap[fd]);
                    fileRDmap.erase(fd);
                    pathmap.erase(fd);
                    if (closeretval == -1) {
                        std::cout << "fail to close rd file.." << std::endl;
                    }
                }
                if (fileWRmap[fd]) {
                    int closeretval = hdfsCloseFile(fs, fileWRmap[fd]);
                    fileWRmap.erase(fd);
                    if (closeretval == -1) {
                        std::cout << "fail to close wr file.." << std::endl;
                    }
                }
                mtx.unlock();
            }

            //Close all the file descriptions maintained in this instance
            void closeAll(){
                mtx.lock();
                for(std::map<unsigned int, hdfsFile>::iterator it = fileRDmap.begin(); it != fileRDmap.end(); ++it){
                    int closeretval = hdfsCloseFile(fs, it->second);
                    if (closeretval == -1) {
                        std::cout << "fail to close rd file.." << std::endl;
                    }
                }
                for(std::map<unsigned int, hdfsFile>::iterator it = fileWRmap.begin(); it != fileWRmap.end(); ++it){
                    if(!needRefreshWR[it->first]){
		    	int closeretval = hdfsCloseFile(fs, it->second);
                    	if (closeretval == -1) {
                        	std::cout << "fail to close wr file.." << std::endl;
                      	}
		    }
                }
                mtx.unlock();
            }
	     
	    //Write to HDFS
            int write(int fd, 
                      unsigned char *output,
                      unsigned long bytes_to_write){
                hdfsFile& filehandle = fileWRmap[fd];
                int writeretval = hdfsWrite(fs, filehandle, output, bytes_to_write);
                if(writeretval == -1){
                    std::cout << "fail to write to file.." << std::endl;
                }
                return writeretval;
            }

	    //Read from HDFS
            int read(int fd, 
                     unsigned char *input,
                     unsigned long bytes_to_read){
                mtx.lock();
		hdfsFile& filehandle = fileRDmap[fd];
		int readretval = hdfsRead(fs, filehandle, input, bytes_to_read);
                if(readretval == -1){
                    std::cout << "fail to read from file.." << std::endl;
                }
	    	mtx.unlock();
                return readretval;
            }

            //This works only for files opened in read-only mode
            int lseek(int fd,
                      int offset){
                hdfsFile& filehandle = fileRDmap[fd];
                int seekretval =  hdfsSeek(fs, filehandle, offset);
                if(seekretval == -1){
                    std::cout << "fail to seek.." << std::endl;
                }
                return seekretval;
            }
	    
	    //Truncate the whole file
            int ftruncate(int fd,
                         int pos){
                std::string& filepath = pathmap[fd];
                int closeretval = hdfsCloseFile(fs, fileWRmap[fd]);
                fileWRmap.erase(fd);
                if (closeretval == -1) {
                    std::cout << "fail to close wr file.." << std::endl;
                }
                hdfsFile filehandle = hdfsOpenFile(fs, filepath.c_str(), O_WRONLY, 0/*not used*/, 1/*use default value*/, 0/*use default value*/);
                mtx.lock();
                fileWRmap[fd] = filehandle;
                mtx.unlock();
                return 0;
            }

	    //Get the size of file
	    //LIBHDFS3 has corresponding API, but LIBHDFS doesn't
	    //The solution here is using hdfsSeek go through the file to get the size
            unsigned long getFileSize(int fd){
                std::string& filepath = pathmap[fd];
#ifdef _LIB_HDFS_3_
                hdfsFileInfo* info = hdfsGetPathInfo(fs, filepath.c_str());
                std::cout << filepath << ": what's my size, " << info->mSize << std::endl;
                return info->mSize;
#else
		long long size = 0, add = 1;
                hdfsFile& filehandle = fileRDmap[fd];
	        while(add){
		   add = hdfsAvailable(fs, filehandle);
		   size += add;
		   hdfsSeek(fs, filehandle, size);
	        }
                std::cout << filepath << ": what's my size, " << size << std::endl;
                return size;
#endif
            }
    };
}
#endif
