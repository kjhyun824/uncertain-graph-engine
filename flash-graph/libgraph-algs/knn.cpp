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
#include "vertex_attribute.h"

using namespace safs;
using namespace fg;

#define partSize 1000
#define PRECISION 1000

int seed;
int nResult;
int nSample;
unsigned int bound;
unsigned int diff;

vertex_id_t sv;
std::set<vertex_id_t> res;
std::mutex lock;

pwgs_vertex_attribute pwgs_vattr;

struct distribution {
    unsigned int dist;
    double probability;
    struct distribution* next; // Linked list format
};


namespace
{
	class knn_vertex_attribute: public vertex_attribute{
	public:
		unsigned int distance;
		bool active;
		bool adaptation;

		virtual size_t getSize(){
			return sizeof(knn_vertex_attribute);
		};
		virtual void initValue(){
			distance = ~0;
			active = false;
			adaptation = false;
		};
		virtual void copy(char* dst){
			memcpy(dst, this, sizeof(knn_vertex_attribute));
		};
	};

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
				knn_vertex_attribute* vattr = (knn_vertex_attribute*)pwgs_vattr.getVertexAttributeInPwgBuf(vid);

                if(vid != sv && vattr->distance >= bound && vattr->distance != ~0) { 
                    return;
                }

                if(vattr->active) {
                    directed_vertex_request req(vid, OUT_EDGE);
                    request_partial_vertices(&req, 1);
                }
            }

            void run(vertex_program &prog, const page_vertex &vertex){
                vertex_id_t vid = prog.get_vertex_id(*this);
				knn_vertex_attribute* vattr = (knn_vertex_attribute*)pwgs_vattr.getVertexAttributeInPwgBuf(vid);

                vattr->active = false;

                int numDests = vertex.get_num_edges(OUT_EDGE);
                if(numDests == 0) {
                    return;
                }

                srand(vid*(seed+1));

                edge_iterator itNeighbor = vertex.get_neigh_begin(OUT_EDGE);
                edge_iterator itNeighborEnd = vertex.get_neigh_end(OUT_EDGE);

                safs::page_byte_array::const_iterator<float> itData = ((const page_directed_vertex &) vertex).get_data_begin<float>(OUT_EDGE);

                for(; itNeighbor != itNeighborEnd;) {
                    float prob = *itData - (unsigned int) *itData;
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
				knn_vertex_attribute* vattr = (knn_vertex_attribute*)pwgs_vattr.getVertexAttributeInPwgBuf(vid);

                const distance_message &w_msg = (const distance_message&) msg;
                if(vattr->distance > w_msg.distance) {
//                    prog.get_graph().getPWG(seed)->load(vid/partSize);
                    vattr->distance = w_msg.distance;
                    vattr->active = true;
                    //prog.activate_vertex(vid);
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
				knn_vertex_attribute* vattr = (knn_vertex_attribute*)pwgs_vattr.getVertexAttributeInPwgBuf(vid);

                if(!vattr->adaptation && vattr->distance != ~0) {
                    unsigned int currDist = vattr->distance;
                    
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
				knn_vertex_attribute* vattr = (knn_vertex_attribute*)pwgs_vattr.getVertexAttributeInPwgBuf(t_vid);

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
    diff = 5; // Just for accuracy, Maybe set by user

    graph_index::ptr index;
    index = NUMA_graph_index<knn_vertex>::create(fg->get_graph_header());
    graph_engine::ptr graph = fg->create_engine(index);

	unsigned int num_vertex = graph->get_max_vertex_id() + 1;
	knn_vertex_attribute vattr;
	vattr.initValue();
	pwgs_vattr.init(_nSample, num_vertex, vattr);

	pwgs_vertex_attribute_io_req pwgs_vattr_io = pwgs_vertex_attribute_io_req(&pwgs_vattr, true);

    graph->setPwgsVattr(&pwgs_vattr);

    struct timeval startTime, endTime;

//    gettimeofday(&startTime, NULL);
//    graph->generatePWGs(partSize);
//    gettimeofday(&endTime, NULL);

    long diffTime = (endTime.tv_sec*1e6 + endTime.tv_usec) - (startTime.tv_sec*1e6 + startTime.tv_usec);

    /* KJH */
    vertex_query::ptr avq, rvq;
    avq = vertex_query::ptr(new accum_vertex_query<knn_vertex>());
    rvq = vertex_query::ptr(new res_vertex_query<knn_vertex>());
    bool start=true;

    while(res.size() < k) {
        for(int i=0; i < nSample; i++) {
            seed = i;

          graph->setCurrSeed(seed);

//			pwgs_vattr_io.loadPWG(seed);
            if(start) {
				pwgs_vattr_io.loadPartByVertex(seed, start_vertex);
//                graph->getPWG(seed)->load(start_vertex / partSize);
				knn_vertex_attribute* vattr = (knn_vertex_attribute*)pwgs_vattr.getVertexAttributeInPwgBuf(start_vertex);

                vattr->active = true;
                vattr->distance = 0;
				pwgs_vattr_io.savePartByVertex(seed, start_vertex);
//                graph->getPWG(seed)->save(start_vertex / partSize);

                graph->start(&start_vertex,1);
            } else {
                graph->start_all();
            }

            graph->wait4complete();

            // Calculate distribution for a specific PWG

//            graph->getPWG(seed)->loadAll();
            graph->query_on_all(avq);
			pwgs_vattr_io.savePWG(seed);
        }
        start = false;
        graph->query_on_all(rvq);

        bound += diff;
    }

//    graph->getPWG(0)->getPart(0)->printTime();

    //KJH TODO : destroy generated PWGs including attributes
    printf("[DEBUG] Generating PWGs : %lu\n",diffTime);
//    graph->destroyPWGs();

    return res;
}
