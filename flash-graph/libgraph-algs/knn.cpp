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

using namespace safs;
using namespace fg;

#define partSize 1000
#define PRECISION 1000

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
            vertex_id_t vid;
            distribution distHead; // Distribution from source
            knn_vertex(vertex_id_t id): compute_directed_vertex(id) {
                this->vid = id;
                distHead.next = NULL;
            }

            void run(vertex_program &prog) {
                vertex_id_t vid = prog.get_vertex_id(*this);
                attribute_t* vattr = prog.get_graph().getAttrBuf(vid);
                if(vid != sv && vattr->distance >= bound) { // KJH TODO : Wow! It works!
                    return;
                }

                if(vattr->active) {
                    directed_vertex_request req(vid, OUT_EDGE);
                    request_partial_vertices(&req, 1);
                }
            }

            void run(vertex_program &prog, const page_vertex &vertex){
                vertex_id_t vid = prog.get_vertex_id(*this);
                attribute_t* vattr = prog.get_graph().getAttrBuf(vid);

                vattr->active = false;

                int numDests = vertex.get_num_edges(OUT_EDGE);
                if(numDests == 0) return;

                srand(vid*(key+1));

                edge_iterator itNeighbor = vertex.get_neigh_begin(OUT_EDGE);
                edge_iterator itNeighborEnd = vertex.get_neigh_end(OUT_EDGE);

                safs::page_byte_array::const_iterator<float> itData = ((const page_directed_vertex &) vertex).get_data_begin<float>(OUT_EDGE);

                for(; itNeighbor != itNeighborEnd;) {
                    float prob = *itData - (int) *itData;
                    unsigned int weight = (unsigned int) *itData;

                    unsigned int distanceSum = weight;

                    if(vattr->distance != ~0)
                        distanceSum += vattr->distance;

                    distance_message msg(distanceSum);

                    float r = rand() % PRECISION;

                    if(r <= prob * PRECISION) {
                        prog.send_msg(*itNeighbor, msg);
                    }

                    itNeighbor += 1;
                    itData += 1;
                }
            }

            void run_on_message(vertex_program &prog, const vertex_message &msg) {
                vertex_id_t vid = prog.get_vertex_id(*this);
                attribute_t* vattr = prog.get_graph().getAttrBuf(vid);

                const distance_message &w_msg = (const distance_message&) msg;
                if(vattr->distance > w_msg.distance) {
                    vattr->distance = w_msg.distance;
                    vattr->active = true;
                    prog.activate_vertex(vid);
                }
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
                double add_prob = 1.0 / (double) nSample;

                vertex_id_t vid = knn_v.vid;
                attribute_t* vattr = graph.getAttrBuf(vid);

                if(!vattr->adaptation && vattr->distance != ~0) {
                    int currDist = vattr->distance;
                    
                    distribution *curr = &knn_v.distHead;
                    while(curr->next != NULL) {
                        if(curr->next->dist <= currDist) break;
                        curr = curr->next;
                    }

                    if(curr->next != NULL && curr->next->dist == currDist) {
                        curr->next->probability += add_prob;
                    } else {
                        distribution *node = new struct distribution;
                        node->dist = currDist;
                        node->probability = add_prob;
                        node->next = curr->next;
                        curr->next = node;
                    }

                    vattr->adaptation = true;
                }
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
                vertex_id_t t_vid = knn_v.vid;
                attribute_t* vattr = graph.getAttrBuf(t_vid);

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
            }

            virtual void merge(graph_engine &graph, vertex_query::ptr q) {
            }

            virtual ptr clone() {
                return vertex_query::ptr(new res_vertex_query());
            }
    };
}

std::set<vertex_id_t> knn(FG_graph::ptr fg, vertex_id_t start_vertex, int k, int _nSample) {
    nResult = k;
    nSample = _nSample;

    sv = start_vertex;

    bound = 0;
    diff = 1; // Just for accuracy, Maybe set by user

    graph_index::ptr index;
    index = NUMA_graph_index<knn_vertex>::create(fg->get_graph_header());
    graph_engine::ptr graph = fg->create_engine(index);

    graph->setNumSample(nSample);
    graph->generatePWGs(partSize);

    /* KJH */
    vertex_query::ptr avq, rvq;
    avq = vertex_query::ptr(new accum_vertex_query<knn_vertex>());
    rvq = vertex_query::ptr(new res_vertex_query<knn_vertex>());
    bool start=true;
    while(res.size() < k) {
        for(int i=0; i < nSample; i++) {
            key = i;

            if(start) {
                graph->start(&start_vertex,1);
                start = false;
            }
            else
                graph->start_all();
            graph->wait4complete();

            // Calculate distribution for a specific PWG
            graph->query_on_all(avq);

        }
        graph->query_on_all(rvq);

        bound += diff;
    }

    //KJH TODO : destroy generated PWGs including attributes
    //graph->destroyPWGs();

    return res;
}
