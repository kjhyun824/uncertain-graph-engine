#ifndef __VERTEX_ATTRIBUTE_H__
#define __VERTEX_ATTRIBUTE_H__

#include "attr_partition.h"

namespace fg{
class attrPartArr_t {
    private:
        int numParts;
        attrPart_t* attrPartitions;

    public:
        attrPartArr_t();
        ~attrPartArr_t();
        void init(int numParts, int seed, int partSize, char* attrBuf);
        attrPart_t* getPartition(int partId);

        void destroy();

};
}
#endif
