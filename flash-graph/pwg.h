#ifndef __PWG_H__
#define __PWG_H__

#include "vertex_attribute.h"
#include <set>

namespace fg{
class pwg_t {
    private:
        int seed;
        int numParts;
        int partSize;
        char* attrBuf;
        attrPartArr_t* attrPartArr;

    public:
        std::set<vertex_id_t> activatedSet;

        pwg_t();
        ~pwg_t();
        void init(int seed, int numParts, int partSize, char* attrBuf);
        void save(int partId);
        void load(int partId);
        void loadAll();
        void saveAll();

        attrPart_t* getPart(int partId);

        void destroy();

        int getSeed() {
            return seed;
        }
};
}
#endif
