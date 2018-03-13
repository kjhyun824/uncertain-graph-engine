#ifndef __ATTR_PARTITION_H__
#define __ATTR_PARTITION_H__

#include "FG_basic_types.h"

#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <fcntl.h>

namespace fg{

typedef struct {
    vertex_id_t vid;
    unsigned int distance;
    bool active;
    bool adaptation;
} attribute_t;

class attrPart_t {
    private:
        int seed;
        int partId;
        bool dirty;

    public:
        attrPart_t();
        ~attrPart_t();
        void init(int seed, int partId, int partSize, char* attrBuf);
        void save(int partSize, char* attrBuf);
        void load(int partSize, char* attrBuf, bool allOrNot);
        
        void destroy();
};
}
#endif
