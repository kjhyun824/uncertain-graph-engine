#ifndef __VERTEX_ATTRIBUTE_H__
#define __VERTEX_ATTRIBUTE_H__

//#include "attr_partition.h"
#include "FG_basic_types.h"
#include "safs_file.h"
#include "io_interface.h"

#define DEFAULT_PART_SIZE 4096
#define TMP_FILE "vattr.tmp"

using namespace safs;
namespace fg{

	typedef unsigned int pwg_id_t;
	typedef unsigned int part_id_t;

	class vertex_attribute{
	public:
		virtual size_t getSize() = 0;
		virtual void initValue() = 0;
		virtual void copy(char* dst) = 0;
	};

	class pwgs_vertex_attribute{
	private:

	public:
		unsigned int numPwg;
		size_t pwgSize;
		size_t partSize;
		size_t vAttrSize;

		unsigned int numVertex;
		unsigned int numParts;
		unsigned int numVertexInPart;
		char* pwgBuf;
		bool use_pwgBuf=true;

		size_t fileSize;
		safs_file* vattr_file;

		file_io_factory::shared_ptr factory;

		pwgs_vertex_attribute(){

		};
		~pwgs_vertex_attribute(){
			free(pwgBuf);
		};
		void init(unsigned int numPwg, unsigned int numVertex, vertex_attribute& initVattr, bool use_pwgBuf=true, size_t partSize=DEFAULT_PART_SIZE);

		vertex_attribute* getVertexAttributeInPwgBuf(vertex_id_t vid);
//		vertex_attribute* getVertexAttributeInPart(vertex_id_t vid, part_id_t partid, char* partBuf);
//		void update(pwg_id_t pwg, vertex_id_t vid, vertex_attribute vattr);
	};

	class pwgs_vertex_attribute_io_req{
	private:
		pwgs_vertex_attribute* pwgs_vattr;
		io_interface::ptr vattrIO;

	public:
		pwgs_vertex_attribute_io_req(pwgs_vertex_attribute* pwgs_vattr, bool init_buf = false){
			this->pwgs_vattr = pwgs_vattr;
			this->vattrIO = create_io(pwgs_vattr->factory, thread::get_curr_thread());
			if(init_buf)
				for(int i = 0; i < pwgs_vattr->numPwg; i++)
					vattrIO->access(pwgs_vattr->pwgBuf, i * pwgs_vattr->pwgSize, pwgs_vattr->pwgSize, WRITE);
		};

		void savePartByVertex(pwg_id_t pwg, vertex_id_t vid, callback* cb=NULL);
		void loadPartByVertex(pwg_id_t pwg, vertex_id_t vid, callback* cb=NULL);
		void savePart(pwg_id_t pwg, part_id_t partid, callback* cb=NULL);
		void loadPart(pwg_id_t pwg, part_id_t partid, callback* cb=NULL);
		void savePWG(pwg_id_t pwg, callback* cb=NULL);
		void loadPWG(pwg_id_t pwg, callback* cb=NULL);
	};
}


#endif
