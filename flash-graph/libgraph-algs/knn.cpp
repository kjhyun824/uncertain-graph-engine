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

int key;
int nResult;
int nSample;
int bound;
int diff;

//pwg *pwgs; // PWGs
//pwg *currPWG;

//double *probs;
vertex_id_t sv;
//std::set<vertex_id_t> *start_set;
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

//typedef int vattr_t;
typedef struct {
    vertex_id_t vid;
    vertex_id_t prior; // XXX : Which vertex is pointing?
    unsigned int distance; // Vertices distribution
    bool active; //Check the vertex is activated
    bool adaptation; // Distance used for distribution
} vattr_t;

class pwgs_vertex_attr{
    unsigned int num_vertex;
    unsigned int num_sample;
    char* buf;
    vattr_t* attr;

public:
    int key;

    ~pwgs_vertex_attr(){
        free(buf);
    }
    void init(unsigned int num_sample, unsigned int num_vertex){
        this->num_sample = num_sample;
        this->num_vertex = num_vertex;

        key = ~0;
        buf = (char*) malloc(num_vertex * sizeof(vattr_t));
        attr = (vattr_t*) buf;
        memset(buf, 0x00, num_vertex * sizeof(vattr_t));
        for(int i=0; i < num_vertex; i++) {
            attr[i].vid = i;
            attr[i].prior = ~0;
            attr[i].distance = ~0;
            attr[i].active = true;
            attr[i].adaptation = false;
        }
        for(int i=0; i < num_sample; i++){
            std::string filename = "attr" + std::to_string(i) + ".txt";
            FILE* fp=fopen(filename.data(), "w+");
            fwrite(buf, sizeof(vattr_t), num_vertex, fp);
            fclose(fp);
        }
    }
    // Use mmap, file per one partition
    // File descriptor list

    void save(unsigned int key){
        assert(this->key == key);
        std::string filename = "attr" + std::to_string(key) + ".txt";
        FILE* fp=fopen(filename.data(), "w");
        fwrite(buf, sizeof(vattr_t), num_vertex, fp);
        fclose(fp);
    };

    void load(unsigned int key){
        this->key = key;
        std::string filename = "attr" + std::to_string(key) + ".txt";
        FILE* fp=fopen(filename.data(), "r");
        fread(buf, sizeof(vattr_t), num_vertex, fp);
        fclose(fp);
    };

    vattr_t* get_value(vertex_id_t vid){
        assert(attr[vid].vid==vid);
        return attr+vid;
    }
};

pwgs_vertex_attr pwgs_vattr;

namespace
{
    class distance_message: public vertex_message{
        public:
            vertex_id_t prior;
            unsigned int distance;

            distance_message(vertex_id_t prior, unsigned int distance): vertex_message(sizeof(distance_message), true){
                this->prior = prior;
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
            vertex_id_t *prior; // XXX : Which vertex is pointing?
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
                prior = new vertex_id_t[nSample];
                distance = new unsigned int[nSample];
                adaptation = new bool[nSample];
                for(int i = 0; i < nSample; i++) {
                    distance[i] = ~0;
                    prior[i] = ~0;
                    active[i] = true;
                    adaptation[i] = false;
                }
#endif
                //distHead = new struct distribution;
                distHead.next = NULL;
            }

            void run(vertex_program &prog) {
                // Run dijkstra up to current bound
                // If it's over, save it as start_vertex for next iteration
#if VATTR_SAVE
                vertex_id_t vid = prog.get_vertex_id(*this);
                vattr = pwgs_vattr.get_value(vid);

                if(vid != sv && vattr->distance >= bound) {
//                    lock.lock();
//                    start_set[key].insert(vid);
//                    lock.unlock();
                    return;
                }

                // For non-visited vertices, request their edge list
                if (vattr->active) {
//                    res.insert(vid);
                    directed_vertex_request req(vid, OUT_EDGE);
                    request_partial_vertices(&req, 1);
                }
#else
                if(vid != sv && distance[key] >= bound) {
//                    lock.lock();
//                    start_set[key].insert(vid);
//                    lock.unlock();
                    return;
                }

                // For non-visited vertices, request their edge list
                if (active[key]) {
//                    res.insert(vid);
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
                    //unsigned int weight = 1;
                    unsigned int weight = (unsigned int) *it_data;

                    unsigned int distance_sum = weight ;

                    if (vattr->distance != ~0)
                        distance_sum += vattr->distance;

                    distance_message msg(vertex.get_id(), distance_sum);

#define PRECISION 1000
                    float r = rand() % PRECISION;

                    if(r <= prob * PRECISION) {
                        prog.send_msg(*it_neighbor, msg);
                    }
                    /*
                    if(r <= prob * PRECISION) {
                        prog.send_msg(*it_neighbor, msg);
                        probs[key] *= prob; // Accum probability
                    } else {
                        probs[key] *= (1.0-prob); // Accum probability
                    }
                    */

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
                    //unsigned int weight = 1;
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
                    /*
                    if(r <= prob * PRECISION) {
                        prog.send_msg(*it_neighbor, msg);
                        probs[key] *= prob; // Accum probability
                    } else {
                        probs[key] *= (1.0-prob); // Accum probability
                    }
                    */

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
                    vattr->prior = w_msg.prior;
                    vattr->active = true; // Reactivation doesn't need
                    prog.activate_vertex(vid);
                }
#else

                if(distance[key] > w_msg.distance){
                    distance[key] = w_msg.distance;
                    prior[key] = w_msg.prior;
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
    nResult = k;
    nSample = _nSample;


//    pwgs = new pwg[nSample];

#if VATTR_SAVE
    pwgs_vattr.init(nSample, fg->get_index_data()->get_max_id()+1);
#endif

//    start_set = new std::set<vertex_id_t>[nSample];
    //probs = new double[nSample];
    sv = start_vertex;

    for (int i = 0; i < nSample; i++) {
//        start_set[i].insert(start_vertex);
        //probs[i] = 1.0;
    }

    bound = 0;
    diff = 1; // Just for accuracy, Maybe set by user

    graph_index::ptr index;
    index = NUMA_graph_index<knn_vertex>::create(fg->get_graph_header());
    graph_engine::ptr graph = fg->create_engine(index);


    // KJH
    vertex_query::ptr avq, rvq;
    avq = vertex_query::ptr(new accum_vertex_query<knn_vertex>());
    rvq = vertex_query::ptr(new res_vertex_query<knn_vertex>());
    bool start=true;
    while(res.size() < k) {
        for(int i=0; i < nSample; i++) {
            // TODO : Load PWG Vertex Attr
            key = i;

//            currPWG = &pwgs[key];
//            currPWG->load();
#if VATTR_SAVE
            pwgs_vattr.load(key);
#endif
//            int num_start = start_set[i].size();

//            if (num_start != 0) {
//                std::set<vertex_id_t>::iterator it = start_set[i].begin();
//                int idx = 0;
//                vertex_id_t *_start_set = new vertex_id_t[num_start];
//
//                while (true) {
//                    if (it == start_set[i].end()) break;
//                    _start_set[idx++] = *it;
//                    it++;
//                }
//
//                start_set[i].clear();
//
//                graph->start(_start_set, num_start);
//                delete[] _start_set;
            if(start)
                graph->start(&start_vertex,1);
            else
                graph->start_all();
            start=false;
            graph->wait4complete();
//            }
            // Calculate distribution for a specific PWG
            graph->query_on_all(avq);

            // TODO : Save PWG Vertex Attr
            // Divide two things : According to how many updates have done
            // Many -> Full flush / Small -> Logging
//            currPWG->save();
#if VATTR_SAVE
            pwgs_vattr.save(key);
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

/*
void print_usage()
{
    fprintf(stderr,
            "knn conf_file graph_file index_file [alg-options]\n");
    graph_conf.print_help();
    safs::params.print_help();
}

void run_knn(FG_graph::ptr graph, int argc, char* argv[])
{
    int opt;
    int num_opts = 0;
    vertex_id_t start_vertex = 0;
    int num_sample = 1;
    int k = 1;

    std::string edge_type_str;
    while ((opt = getopt(argc, argv, "k:s:p:")) != -1) {
        num_opts++;
        switch (opt) {
            case 'k':
                k = atoi(optarg);
                num_opts++;
                break;
            case 's':
                start_vertex = atoi(optarg);
                num_opts++;
                break;
            case 'p':
                num_sample = atoi(optarg);
                num_opts++;
                break;
            default:
                print_usage();
                abort();
        }
    }

    // KJH
    struct timeval start, end;
    gettimeofday(&start,NULL);
    //std::set<vertex_id_t>  knn(FG_graph::ptr fg, vertex_id_t start_vertex,int k, int num_sample);
    std::set<vertex_id_t> res = knn(graph, start_vertex, k, num_sample);
    gettimeofday(&end,NULL);

    std::cout << "[DEBUG] Total Elapsed Time : " << time_diff(start,end) << std::endl;
    std::cout << "[DEBUG] Num K-NN : " << res.size() << std::endl;
    std::cout << "[DEBUG] KNN neighbor list: \n";

    std::set<vertex_id_t>::iterator it;
    for(it = res.begin(); it != res.end(); ++it) {
        std::cout << "[DEBUG] n.b. : " << *it <<std::endl;
    }
}

int main(int argc, char *argv[])
{
    argv++;
    argc--;
    if (argc < 3) {
        print_usage();
        exit(-1);
    }

    std::string conf_file = argv[0];
    std::string graph_file = argv[1];
    std::string index_file = argv[2];
    // We should increase by 3 instead of 4. getopt() ignores the first
    // argument in the list.
    argv += 2;
    argc -= 2;

    config_map::ptr configs = config_map::create(conf_file);
    if (configs == NULL)
        configs = config_map::ptr();
//    set_log_level(fatal);
    graph_engine::init_flash_graph(configs);
    FG_graph::ptr graph;
    try {
        graph = FG_graph::create(graph_file, index_file, configs);
    } catch(std::exception &e) {
        fprintf(stderr, "%s\n", e.what());
        exit(-1);
    }
    std::cout << "[DEBUG] Vertex Attribute Save-Load : " << VATTR_SAVE << std::endl;
    run_knn(graph, argc, argv);

    graph_engine::destroy_flash_graph();
}
*/

