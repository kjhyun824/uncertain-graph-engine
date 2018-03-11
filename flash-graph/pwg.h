#ifndef __PWG_H__
#define __PWG_H__

#include "vertex_attribute.h"

namespace fg{
class pwg_t {
    private:
        int seed;
        int numParts;
        int partSize;
        char* attrBuf;
        attrPartArr_t* attrPartArr;

    public:
        pwg_t();
        ~pwg_t();
        void init(int seed, int numParts, int partSize, char* attrBuf);
        void save(int partId);
        void load(int partId);

        int getSeed() {
            return seed;
        }
};
}
#endif
