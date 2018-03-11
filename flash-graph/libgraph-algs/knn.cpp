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
            distribution distHead; // Distribution from source
            knn_vertex(vertex_id_t id): compute_directed_vertex(id) {
                distHead.next = NULL;
            }

            void run(vertex_program &prog) {
                /*
                if(vid != sv && distance[key] >= bound) {
                    return;
                }

                // For non-visited vertices, request their edge list
                if (active[key]) {
                    directed_vertex_request req(vid, OUT_EDGE);
                    request_partial_vertices(&req, 1);
                }
                */
            }

            void run(vertex_program &prog, const page_vertex &vertex){
                /*
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
                */
            }

            void run_on_message(vertex_program &prog, const vertex_message &msg) {
                /*
                vertex_id_t vid = prog.get_vertex_id(*this);

                const distance_message &w_msg = (const distance_message&) msg;

                if(distance[key] > w_msg.distance){
                    distance[key] = w_msg.distance;
                    active[key] = true; // Reactivation doesn't need
                    prog.activate_vertex(vid);
                }
                */
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
                /*
                vertex_type &knn_v = (vertex_type &) v;
                // 1. check all samples that has adapted before (adaptation)
                // 2. Check the distance has set (distance)
                // 3. If it has set, add to distribution
                // 3.1 If it's in distribution, just add the PWG of the sample world
                // 3.2 If it's not, append one more node and add the PWG's prob to list

                double add_prob = 1.0 / (double) nSample;
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
                */
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
                /*
                vertex_id_t t_vid = knn_v.get_id();
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
                */
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

    return res;
}
