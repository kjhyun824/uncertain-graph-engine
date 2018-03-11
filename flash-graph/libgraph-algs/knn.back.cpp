//
// Created by kckjn97 on 17. 11. 22.
//

#ifndef VATTR_SAVE
#define VATTR_SAVE 1
#endif

#include <signal.h>
#ifdef PROFILER
#include <gperftools/profiler.h>
#endif

#include <set>
#include <vector>

#include "graph_engine.h"
#include "graph_config.h"
#include "FGlib.h"
#include <mutex>

/* KJH : For mmap() */
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <fcntl.h>

using namespace safs;
using namespace fg;

#define partSize 1000

int key;
int nResult;
int nSample;
int bound;
int diff;

vertex_id_t sv;
std::set<vertex_id_t> res;
std::mutex lock;

struct distribution {
    int dist;
    double probability;
    struct distribution* next; // Linked list format
};


// compute_directed_vertex_t;
/*
   class vertex_part_t {
   compute_directed_vertex_t *vertices;
   int part_id;
   }

   class pwg_t {
   int seed;
   vertex_part_t* parts;
   }
   */

// KJH TODO : Where should these attributes stored?
typedef struct {
    vertex_id_t vid;
    unsigned int distance; // Vertices distribution
    bool active; //Check the vertex is activated
    bool adaptation; // Distance used for distribution
} vattr_t;

typedef vattr_t* part_t;

// TODO : pwgs_vertex_attr - (num_vertex, num_sample, partition list) & make folder for that PWG
// TODO : partition - (buffer, attr) & make file for that PWG's partition

class pwgs_vertex_attr{
    unsigned int num_vertex;
    unsigned int num_sample;
    char* buf;
    vattr_t* attr;
    part_t* partitions;

    public:
    int key;

    ~pwgs_vertex_attr(){
    }

    void init(unsigned int num_sample, unsigned int num_vertex){
        char *file = NULL;
        int flag = PROT_WRITE | PROT_READ;
        int fd;
        struct stat fileInfo = {0};

        this->num_sample = num_sample;
        this->num_vertex = num_vertex;
        int numParts = ((num_vertex / partSize) == 0) ? (num_vertex / partSize) : ((num_vertex / partSize) + 1);
        partitions = new part_t[numParts];

        key = ~0;
        buf = (char*) malloc(partSize * sizeof(vattr_t));
        attr = (vattr_t*) buf;
        for(int i=0; i < partSize; i++) {
            attr[i].vid = i;
            attr[i].distance = ~0;
            attr[i].active = true;
            attr[i].adaptation = false;
        }

        for(int i=0; i < num_sample; i++){
            /* KJH */
            for(int j = 0; j < numParts; j++) {
                std::string filename2 = "map" + std::to_string(i) + "_" + std::to_string(j) + ".txt";
                if((fd=open(filename2.c_str() ,O_RDWR|O_CREAT,0664)) < 0) {
                    perror("File Open Error");
                    exit(1);
                }
                if(fstat(fd,&fileInfo) == -1) {
                    perror("Error for getting size");
                    exit(1);
                }

                if((file = (char*) mmap(0,partSize * sizeof(vattr_t), flag, MAP_SHARED, fd, 0)) == NULL) {
                    perror("mmap error");
                    exit(1);
                }

                if(write(fd,buf,partSize*sizeof(vattr_t)) == -1) {
                    perror("write error");
                    exit(1);
                }

                if (msync(file , partSize * sizeof(vattr_t), MS_SYNC) == -1)
                {
                    perror("Could not sync the file to disk");
                }   

                if (munmap(file, partSize * sizeof(vattr_t)) == -1) {
                    perror("munmap error");
                    exit(1);
                }

                close(fd);
            }
        }
    }
    // Use mmap, file per one partition
    // File descriptor list

    void save(unsigned int key, int partId) {
        assert(this->key == key);
        std::string filename2 = "map" + std::to_string(key) + "_" + std::to_string(partId) + ".txt";
        int fd = open(filename2.c_str(), O_RDWR);
        write(fd,buf,partSize * sizeof(vattr_t));
        close(fd);
    };

    void load(unsigned int key, int partId) {
        this->key = key;
        std::string filename2 = "map" + std::to_string(key) + "_" + std::to_string(partId) + ".txt";
        int fd = open(filename2.c_str(), O_RDWR);
        read(fd,buf,partSize * sizeof(vattr_t));
        close(fd);
    };

    vattr_t* get_value(vertex_id_t vid) {
        assert(attr[vid].vid==vid);
        return attr+vid;
    }
};

pwgs_vertex_attr pwgs_vattr;

namespace
{
    class distance_message: public vertex_message{
        public:
            unsigned int distance;

            distance_message(unsigned int distance): vertex_message(sizeof(distance_message), true){
                this->distance = distance;
            }
    };

    class knn_vertex: public compute_directed_vertex
    {
        public:
#if VATTR_SAVE
            vattr_t* vattr;
#else
            vertex_id_t vid;
            bool *active; //Check the vertex is activated
            unsigned int *distance; // Vertices distribution
            bool *adaptation; // Distance used for distribution
#endif
            distribution distHead; // Distribution from source
            knn_vertex(vertex_id_t id): compute_directed_vertex(id) {
#if VATTR_SAVE
                vattr = pwgs_vattr.get_value(id); // TODO : Maybe erase later
#else
                vid = id;
                active = new bool[nSample];
                distance = new unsigned int[nSample];
                adaptation = new bool[nSample];
                for(int i = 0; i < nSample; i++) {
                    distance[i] = ~0;
                    active[i] = true;
                    adaptation[i] = false;
                }
#endif
                distHead.next = NULL;
            }

            void run(vertex_program &prog) {
#if VATTR_SAVE
                vertex_id_t vid = prog.get_vertex_id(*this);
                vattr = pwgs_vattr.get_value(vid);

                if(vid != sv && vattr->distance >= bound) {
                    return;
                }

                // For non-visited vertices, request their edge list
                if (vattr->active) {
                    directed_vertex_request req(vid, OUT_EDGE);
                    request_partial_vertices(&req, 1);
                }
#else
                if(vid != sv && distance[key] >= bound) {
                    return;
                }

                // For non-visited vertices, request their edge list
                if (active[key]) {
                    directed_vertex_request req(vid, OUT_EDGE);
                    request_partial_vertices(&req, 1);
                }
#endif
            }

            void run(vertex_program &prog, const page_vertex &vertex){
#if VATTR_SAVE
                vertex_id_t vid = prog.get_vertex_id(*this);
                vattr->active = false;

                int num_dests = vertex.get_num_edges(OUT_EDGE);
                if (num_dests == 0)
                    return;

                srand(vid*(key+1)); // Give seed to generate random specific number; TODO: Add time instance to give randomness

                edge_iterator it_neighbor = vertex.get_neigh_begin(OUT_EDGE);
                edge_iterator it_neighbor_end = vertex.get_neigh_end(OUT_EDGE);

                safs::page_byte_array::const_iterator<float> it_data = ((const page_directed_vertex &) vertex).get_data_begin<float>(OUT_EDGE);

                // Iterates neighbors
                for (; it_neighbor != it_neighbor_end;) {
                    float prob = *it_data - (int) *it_data;
                    unsigned int weight = (unsigned int) *it_data;

                    unsigned int distance_sum = weight ;

                    if (vattr->distance != ~0)
                        distance_sum += vattr->distance;

                    distance_message msg(distance_sum);

#define PRECISION 1000
                    float r = rand() % PRECISION;

                    if(r <= prob * PRECISION) {
                        prog.send_msg(*it_neighbor, msg);
                    }

                    it_neighbor += 1;
                    it_data += 1;
                }
#else

                active[key] = false;

                int num_dests = vertex.get_num_edges(OUT_EDGE);
                if (num_dests == 0)
                    return;

                srand(vid*(key+1)); // Give seed to generate random specific number; TODO: Add time instance to give randomness

                edge_iterator it_neighbor = vertex.get_neigh_begin(OUT_EDGE);
                edge_iterator it_neighbor_end = vertex.get_neigh_end(OUT_EDGE);

                safs::page_byte_array::const_iterator<float> it_data = ((const page_directed_vertex &) vertex).get_data_begin<float>(OUT_EDGE);

                // Iterates neighbors
                for (; it_neighbor != it_neighbor_end;) {
                    float prob = *it_data - (int) *it_data;
                    unsigned int weight = (unsigned int) *it_data;

                    unsigned int distance_sum = weight ;

                    if (distance[key] != ~0)
                        distance_sum += distance[key];

                    distance_message msg(vertex.get_id(), distance_sum);

#define PRECISION 1000
                    float r = rand() % PRECISION;

                    if(r <= prob * PRECISION) {
                        prog.send_msg(*it_neighbor, msg);
                    }

                    it_neighbor += 1;
                    it_data += 1;
                }
#endif
            };

            void run_on_message(vertex_program &prog, const vertex_message &msg) {
                vertex_id_t vid = prog.get_vertex_id(*this);

                const distance_message &w_msg = (const distance_message&) msg;

#if VATTR_SAVE
                if(vattr->distance > w_msg.distance){
                    vattr->distance = w_msg.distance;
                    vattr->active = true; // Reactivation doesn't need
                    prog.activate_vertex(vid);
                }
#else

                if(distance[key] > w_msg.distance){
                    distance[key] = w_msg.distance;
                    active[key] = true; // Reactivation doesn't need
                    prog.activate_vertex(vid);
                }
#endif
            }
    };

    // KJH
    template<class vertex_type>
        class accum_vertex_query: public vertex_query
    {
        public:
            accum_vertex_query() {
            }

            virtual void run(graph_engine &graph, compute_vertex &v) {
                vertex_type &knn_v = (vertex_type &) v;
                // 1. check all samples that has adapted before (adaptation)
                // 2. Check the distance has set (distance)
                // 3. If it has set, add to distribution
                // 3.1 If it's in distribution, just add the PWG of the sample world
                // 3.2 If it's not, append one more node and add the PWG's prob to list

                double add_prob = 1.0 / (double) nSample;
#if VATTR_SAVE
                if(!knn_v.vattr->adaptation && knn_v.vattr->distance != ~0) {
                    int currDist = knn_v.vattr->distance;

                    distribution *curr = &knn_v.distHead;
                    while(curr->next != NULL) {
                        if(curr->next->dist <= currDist) break;
                        curr = curr->next;
                    }

                    if(curr->next != NULL && curr->next->dist == currDist) { // Already distribution
                        curr->next->probability += add_prob;
                    } else { // New one
                        distribution *node = new struct distribution;
                        node->dist = currDist;
                        node->probability = add_prob;
                        node->next = curr->next;
                        curr->next = node;
                    }

                    knn_v.vattr->adaptation = true;
                }
#else
                if(!knn_v.adaptation[key] && knn_v.distance[key] != ~0) {
                    int currDist = knn_v.distance[key];
                    distribution *curr = &knn_v.distHead;
                    while(curr->next != NULL) {

                        if(curr->next->dist <= currDist) break;
                        curr = curr->next;
                    }

                    //std::cout <<curr->next<<"\n";
                    if(curr->next != NULL && curr->next->dist == currDist) { // Already distribution
                        curr->next->probability += add_prob;
                    } else { // New one
                        distribution *node = new struct distribution;
                        node->dist = currDist;
                        node->probability = add_prob;
                        node->next = curr->next;
                        curr->next = node;
                    }

                    //std::cout << currDist << " " <<curr->next->probability<< " "<<curr->next->dist<<"\n";
                    knn_v.adaptation[key] = true;
                }
#endif
            }

            virtual void merge(graph_engine &graph, vertex_query::ptr q) {
            }

            virtual ptr clone() {
                return vertex_query::ptr(new accum_vertex_query());
            }
    };

    template<class vertex_type>
        class res_vertex_query: public vertex_query
    {
        public:
            res_vertex_query() {
            }

            virtual void run(graph_engine &graph, compute_vertex &v) {
                if(res.size() >= nResult) return;

                vertex_type &knn_v = (vertex_type &) v;
#if VATTR_SAVE
                vertex_id_t t_vid = knn_v.vattr->vid;
                if(res.find(t_vid) == res.end()) {
                    distribution *curr = &knn_v.distHead;
                    double probSum = 0.0;
                    while(curr->next != NULL) {
                        probSum += curr->next->probability;

                        if(probSum > 0.5) break;
                        curr = curr->next;
                    }

                    if(curr->next != NULL) {
                        if(curr->next->dist < bound) {
                            lock.lock();
                            res.insert(t_vid);
                            lock.unlock();
                        }
                    }
                }
#else
                vertex_id_t t_vid = knn_v.vid;
                if(res.find(t_vid) == res.end()) {
                    distribution *curr = &knn_v.distHead;
                    double probSum = 0.0;
                    while(curr->next != NULL) {
                        probSum += curr->next->probability;
                        if(probSum > 0.5) break;
                        curr = curr->next;
                    }

                    if(curr->next != NULL) {
                        if(curr->next->dist < bound) {
                            lock.lock();
                            res.insert(t_vid);
                            lock.unlock();
                        }
                    }
                }
#endif
            }

            virtual void merge(graph_engine &graph, vertex_query::ptr q) {
            }

            virtual ptr clone() {
                return vertex_query::ptr(new res_vertex_query());
            }
    };
}

std::set<vertex_id_t> knn(FG_graph::ptr fg, vertex_id_t start_vertex, int k, int _nSample) {
    std::cout << "[DEBUG] sizeof(knn_vertex) : " << sizeof(knn_vertex) << std::endl;
    std::cout << "[DEBUG] sizeof(vattr_t*) : " << sizeof(vattr_t*) << std::endl;
    std::cout << "[DEBUG] sizeof(vattr_t) : " << sizeof(vattr_t) << std::endl;
    std::cout << "[DEBUG] sizeof(distribution) : " << sizeof(distribution) << std::endl;
    nResult = k;
    nSample = _nSample;

#if VATTR_SAVE
    pwgs_vattr.init(nSample, fg->get_index_data()->get_max_id()+1);
#endif

    sv = start_vertex;

    bound = 0;
    diff = 1; // Just for accuracy, Maybe set by user

    graph_index::ptr index;
    index = NUMA_graph_index<knn_vertex>::create(fg->get_graph_header());
    graph_engine::ptr graph = fg->create_engine(index);

    //graph->setNumSample(nSample);
    //graph->generatePWGs();

    /* KJH */
    vertex_query::ptr avq, rvq;
    avq = vertex_query::ptr(new accum_vertex_query<knn_vertex>());
    rvq = vertex_query::ptr(new res_vertex_query<knn_vertex>());
    bool start=true;
    while(res.size() < k) {
        for(int i=0; i < nSample; i++) {
            // TODO : Load PWG Vertex Attr
            key = i;

#if VATTR_SAVE
            pwgs_vattr.load(key, 0); // TODO : partId instead 0
#endif
            if(start)
                graph->start(&start_vertex,1);
            else
                graph->start_all();
            start=false;
            graph->wait4complete();

            // Calculate distribution for a specific PWG
            graph->query_on_all(avq);

            // TODO : Save PWG Vertex Attr
            // Divide two things : According to how many updates have done
            // Many -> Full flush / Small -> Logging
#if VATTR_SAVE
            pwgs_vattr.save(key, 0); // TODO : partId instead 0
#endif
        }
        // Aggregate results from all PWGs
        // Median Distance calc
        // Add to result set
        graph->query_on_all(rvq);

        bound += diff;
    }

    return res;
}
