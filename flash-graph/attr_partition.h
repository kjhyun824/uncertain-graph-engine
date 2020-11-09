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
        int partId;
        bool dirty;
        bool loaded;

    public:
        attrPart_t();
        ~attrPart_t();
        void init(int seed, int partId, int partSize, char* attrBuf);
        void save(int*fd, int seed, int partSize, char* attrBuf);
		void save_async(int seed, int partSize, char* attrBuf);
        void load(int*fd, int seed, int partSize, char* attrBuf, bool isAll);
        
        void destroy(int seed);

        void printTime();
        bool isDirty() {
            return dirty;
        }
};
}
#endif
