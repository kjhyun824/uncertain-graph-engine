#include "vertex_attribute.h"
#include <string>

using namespace fg;


attrPartArr_t::attrPartArr_t() {
}

attrPartArr_t::~attrPartArr_t() {
}

void attrPartArr_t::init(int* fd,int numParts, int seed, int partSize, char* attrBuf) {
    this->numParts = numParts;

    attrPartitions = new attrPart_t[numParts];

    std::string filename = "tmp" + std::to_string(seed%2) + ".txt";

    if((fd[seed%2] = open(filename.c_str(), O_RDWR|O_CREAT, 0664)) < 0) {
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

    for(int i = 0; i < numParts; i++) {
        attrPartitions[i].init(seed, i, partSize, attrBuf+(i * partSize * sizeof(attribute_t)));
    }

    int pwg_size = numParts * partSize * sizeof(attribute_t);
    if (write(fd[seed % 2], attrBuf, pwg_size) == -1) {
        perror("write error");
        exit(1);
    }
}

attrPart_t* attrPartArr_t::getPartition(int partId) {
    return &(attrPartitions[partId]); 
}

void attrPartArr_t::destroy(int seed) {
    for(int i = 0; i < numParts; i++) {
        attrPartitions[i].destroy(seed);
    }

    delete [] attrPartitions;
}
