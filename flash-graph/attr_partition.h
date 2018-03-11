#ifndef __ATTR_PARTITION_H__
#define __ATTR_PARTITION_H__
/*
#include "graph_engine.h"
#include "graph_config.h"
#include "FGlib.h"
*/

#include "FG_basic_types.h"

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
        int partId;
        //char* buf; // KJH TODO : How to use Buffer here?

        /* KJH
         * TODO : type of vertices?
         * knn_vertex type should be stored in disk
         * How can I get the type? (user-defined type)
         */
        //attribute_t* attributes; 

    public:
        attrPart_t();
        ~attrPart_t();
        void init(int seed, int partId, int partSize, char* attrBuf);
        void save(int seed, int partSize, char* attrBuf);
        void load(int seed, int partSize, char* attrBuf);
};
}
#endif
