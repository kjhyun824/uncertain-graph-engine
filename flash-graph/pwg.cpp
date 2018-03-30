#include <cstdlib> 

#include "pwg.h"

using namespace fg;

pwg_t::pwg_t() {
}

pwg_t::~pwg_t() {
}


int fd[2];
void pwg_t::init(int seed, int numParts, int partSize, char* attrBuf) {
    this->seed = seed;
    this->numParts = numParts;
    this->partSize = partSize;
    this->attrBuf = attrBuf;


    attrPartArr = new attrPartArr_t();
    attrPartArr->init(fd, numParts, seed, partSize, attrBuf);
}

void pwg_t::destroy() {
    attrPartArr->destroy(seed);
    delete attrPartArr;
}

void pwg_t::save(int partId) {
    attrPartArr->getPartition(partId)->save(fd, seed, partSize, attrBuf+(partId * partSize * sizeof(attribute_t)));
}

void pwg_t::load(int partId) {
    attrPartArr->getPartition(partId)->load(fd, seed, partSize, attrBuf+(partId * partSize * sizeof(attribute_t)), false);
}

void pwg_t::loadAll() {
    for(int i = 0; i < numParts; i++) {
        attrPartArr->getPartition(i)->load(fd, seed, partSize, attrBuf+(i * partSize * sizeof(attribute_t)), true);
    }
}

void pwg_t::saveAll() {
    for(int i = 0; i < numParts; i++) {
        attrPartArr->getPartition(i)->save(fd, seed, partSize, attrBuf+(i * partSize * sizeof(attribute_t)));
    }
}

attrPart_t* pwg_t::getPart(int partId) {
    return attrPartArr->getPartition(partId);
}
