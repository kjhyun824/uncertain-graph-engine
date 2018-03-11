#include <cstdlib> 

#include "pwg.h"

using namespace fg;

pwg_t::pwg_t() {
}

pwg_t::~pwg_t() {
}

void pwg_t::init(int seed, int numParts, int partSize, char* attrBuf) {
    this->seed = seed;
    this->numParts = numParts;
    this->partSize = partSize;
    this->attrBuf = attrBuf;

    attrPartArr = new attrPartArr_t();
    attrPartArr->init(numParts, seed, partSize, attrBuf); 
}

void pwg_t::save(int partId) {
    attrPartArr->getPartition(partId)->save(seed, partSize, attrBuf+(partId * partSize * sizeof(attribute_t)));
}

void pwg_t::load(int partId) {
    attrPartArr->getPartition(partId)->load(seed, partSize, attrBuf+(partId * partSize * sizeof(attribute_t)));
}
