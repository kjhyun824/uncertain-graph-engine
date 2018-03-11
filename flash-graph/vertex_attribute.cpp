#include "vertex_attribute.h"

using namespace fg;

attrPartArr_t::attrPartArr_t() {
}

attrPartArr_t::~attrPartArr_t() {
}

void attrPartArr_t::init(int numParts, int seed, int partSize, char* attrBuf) {
    this->numParts = numParts;

    attrPartitions = new attrPart_t[numParts];
    
    for(int i = 0; i < numParts; i++) {
        attrPartitions[i].init(seed, i, partSize, attrBuf+(i * partSize * sizeof(attribute_t)));
    }
}

attrPart_t* attrPartArr_t::getPartition(int partId) {
    return &(attrPartitions[partId]); // KJH TODO : Right expression?
}
