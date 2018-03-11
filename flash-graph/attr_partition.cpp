#include "attr_partition.h"
#include <string>

using namespace fg;

void attr_init(attribute_t* temp, int vid) {
    temp->vid = vid;
    temp->distance = ~0;
    temp->active = true;
    temp->adaptation = false;
}

attrPart_t::attrPart_t() {
}

attrPart_t::~attrPart_t() {
}

void attrPart_t::init(int seed, int partId, int partSize, char* attrBuf) {
    this->partId = partId;
    
    /* attrBuf initialize value */
    attribute_t* temp = (attribute_t*) attrBuf;
    for(int i = 0; i < partSize; i++) {
        attr_init(&temp[i], partId * partSize + i);
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

    if(fstat(fd, &fileInfo) == -1) {
        perror("Error for getting size");
        exit(1);
    }

    // KJH TODO : Change size of file correctly
    if((file = (char*) mmap(0, partSize * sizeof(attribute_t), flag, MAP_SHARED, fd, 0)) == NULL) { 
        perror("mmap error");
        exit(1);
    }

    if(write(fd, attrBuf, partSize * sizeof(attribute_t)) == -1) {
        perror("write error");
        exit(1);
    }

    if(msync(file, partSize * sizeof(attribute_t), MS_SYNC) == -1) {
        perror("Could not sync the file to disk");
        exit(1);
    }

    if(munmap(file, partSize * sizeof(attribute_t)) == -1) {
        perror("munmap error");
        exit(1);
    }

    close(fd);
}

void attrPart_t::save(int seed, int partSize, char* attrBuf) {
    std::string filename = "map" + std::to_string(seed) + "_" + std::to_string(partId) + ".txt";
    int fd = open(filename.c_str(), O_RDWR);
    write(fd, attrBuf, partSize * sizeof(attribute_t));
    close(fd);
}

void attrPart_t::load(int seed, int partSize, char* attrBuf) {
    std::string filename = "map" + std::to_string(seed) + "_" + std::to_string(partId) + ".txt";
    int fd = open(filename.c_str(), O_RDWR);
    read(fd, attrBuf, partSize * sizeof(attribute_t));
    close(fd);
}
