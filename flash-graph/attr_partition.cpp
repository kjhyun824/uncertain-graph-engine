#include "attr_partition.h"
#include <string>

#include "worker_thread.h"
#include <time.h>

using namespace fg;

long loadTime[16];
long saveTime[16];

void attr_init(attribute_t* temp, int vid) {
    temp->vid = vid;
    temp->distance = ~0;
    temp->active = false;
    temp->adaptation = false;
}

attrPart_t::attrPart_t() {
    for(int i = 0; i < 16; i++) {
        loadTime[i] = 0;
        saveTime[i] = 0;
    }
}

attrPart_t::~attrPart_t() {
}

void attrPart_t::init(int seed, int partId, int partSize, char* attrBuf) {
    this->partId = partId;
    this->dirty = false;
    this->loaded = false;
    
    /* attrBuf initialize value */
    attribute_t* temp = (attribute_t*) attrBuf;
    for(int i = 0; i < partSize; i++) {
        attr_init(&(temp[i]), partId * partSize + i);
    }

    char *file = NULL;
    int flag = PROT_WRITE | PROT_READ;
    int fd;
    struct stat fileInfo = {0};

    std::string filename = "map" + std::to_string(seed) + "_" + std::to_string(partId) + ".txt";

    if((fd = open(filename.c_str(), O_RDWR|O_CREAT, 0664)) < 0) {
        perror("File Open Error");
        exit(1);
    }

    /*
    if(fstat(fd, &fileInfo) == -1) {
        perror("Error for getting size");
        exit(1);
    }

    // KJH TODO : Change size of file correctly
    if((file = (char*) mmap(0, partSize * sizeof(attribute_t), flag, MAP_SHARED, fd, 0)) == NULL) { 
        perror("mmap error");
        exit(1);
    }
    */

    if(write(fd, attrBuf, partSize * sizeof(attribute_t)) == -1) {
        perror("write error");
        exit(1);
    }

    /*
    if(msync(file, partSize * sizeof(attribute_t), MS_SYNC) == -1) {
        perror("Could not sync the file to disk");
        exit(1);
    }

    if(munmap(file, partSize * sizeof(attribute_t)) == -1) {
        perror("munmap error");
        exit(1);
    }
    */

    close(fd);
}

void attrPart_t::save(int seed, int partSize, char* attrBuf) {
    if(!loaded) 
        return;

    loaded = false;

    if(dirty) {
        worker_thread *t = (worker_thread*) thread::get_curr_thread();
        int w_id= t->get_worker_id();
        struct timeval start, end; 
        gettimeofday(&start, NULL);

        std::string filename = "map" + std::to_string(seed) + "_" + std::to_string(partId) + ".txt";
        int fd = open(filename.c_str(), O_WRONLY);
        write(fd, attrBuf, partSize * sizeof(attribute_t));
        close(fd);

        dirty = false;

        gettimeofday(&end, NULL);
        long diff = (end.tv_sec*1e6+ end.tv_usec) - (start.tv_sec*1e6 + start.tv_usec);
        saveTime[w_id] += diff;
    }
}

void attrPart_t::load(int seed, int partSize, char* attrBuf, bool isAll) {
    if(loaded) 
        return;

    worker_thread *t = (worker_thread*) thread::get_curr_thread();
    int w_id = t->get_worker_id();
    struct timeval start, end;
    gettimeofday(&start, NULL);

    std::string filename = "map" + std::to_string(seed) + "_" + std::to_string(partId) + ".txt";
    int fd = open(filename.c_str(), O_RDONLY);
    read(fd, attrBuf, partSize * sizeof(attribute_t));
    loaded = true;
    close(fd);

    //if(!isAll)
    dirty = true;

    gettimeofday(&end, NULL);

    long diff = (end.tv_sec*1e6+ end.tv_usec) - (start.tv_sec*1e6 + start.tv_usec);
    loadTime[w_id] += diff;
}

void attrPart_t::destroy(int seed) {
    std::string filename = "map" + std::to_string(seed) + "_" + std::to_string(partId) + ".txt";
    
    if(remove(filename.c_str()) != 0) {
        perror("Error deleting file");
    }
}

void attrPart_t::printTime() {
    printf("[I/O TIME] thread id, load time, save time\n");
    long loadTotal = 0, saveTotal = 0;
    for(int i = 0; i < 16; i++) {
        loadTotal += loadTime[i];
        saveTotal += saveTime[i];
        printf("%d,%lu,%lu\n",i,loadTime[i],saveTime[i]);
    }

    printf("[I/O TIME] load total : %lu, save total : %lu, total : %lu\n",loadTotal, saveTotal, loadTotal+saveTotal);
}
