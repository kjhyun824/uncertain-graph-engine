

#include "vertex_attribute.h"
namespace fg {

	void pwgs_vertex_attribute::init(unsigned int numPwg, unsigned int numVertex, vertex_attribute& initVattr, bool use_pwgBuf, size_t partSize){
		this->numPwg = numPwg;
		this->partSize = partSize;
		this->numVertex = numVertex;
		this->numVertexInPart = partSize / initVattr.getSize();
		this->numParts = (numVertex + numVertexInPart - 1) / numVertexInPart;
		this->pwgSize = numParts * partSize;
		this->fileSize = pwgSize * numPwg;
		this->use_pwgBuf = use_pwgBuf;
		this->vAttrSize = initVattr.getSize();

		vattr_file = new safs_file(get_sys_RAID_conf(), TMP_FILE);
		vattr_file->create_file(fileSize);
		factory = create_io_factory(TMP_FILE, GLOBAL_CACHE_ACCESS);

		pwgBuf = (char *) valloc(pwgSize);

		char* vattrInitBuf = (char*) valloc(partSize);

		char *ptrVattr = (char *) vattrInitBuf;
		for (int i = 0; i < numVertexInPart; i++) {
			initVattr.copy(ptrVattr);
			ptrVattr += vAttrSize;
		}

		for(int i = 0; i < numParts; i++)
			memcpy(pwgBuf + i * partSize, vattrInitBuf, partSize);

		if(!use_pwgBuf) {
			free(pwgBuf);
			assert(pwgBuf == NULL);
		}
	};

	void pwgs_vertex_attribute_io_req::savePartByVertex(pwg_id_t pwg, vertex_id_t vid, callback *cb) {
		part_id_t partId = vid / pwgs_vattr->numVertexInPart;
		savePart(pwg, partId, cb);
	}

	void pwgs_vertex_attribute_io_req::loadPartByVertex(pwg_id_t pwg, vertex_id_t vid, callback *cb) {
		part_id_t partId = vid / pwgs_vattr->numVertexInPart;
		loadPart(pwg, partId, cb);
	}


	void pwgs_vertex_attribute_io_req::savePart(pwg_id_t pwg, part_id_t partid, callback *cb) {
		if (cb == NULL) {
			// syncIO
			vattrIO->access(pwgs_vattr->pwgBuf + pwgs_vattr->partSize * partid, pwg * pwgs_vattr->pwgSize + partid * pwgs_vattr->partSize, pwgs_vattr->partSize, WRITE);
		} else {
			//Todo
		}
	}

	void pwgs_vertex_attribute_io_req::loadPart(pwg_id_t pwg, part_id_t partid, callback *cb) {
		if (cb == NULL) {
			// syncIO
			vattrIO->access(pwgs_vattr->pwgBuf + pwgs_vattr->partSize * partid, pwg * pwgs_vattr->pwgSize + partid * pwgs_vattr->partSize, pwgs_vattr->partSize, READ);
		}
			// asyncIO
		else {
			//Todo
		}
	}

	void pwgs_vertex_attribute_io_req::savePWG(pwg_id_t pwg, callback *cb) {
		if (cb == NULL) {
			// syncIO
			vattrIO->access(pwgs_vattr->pwgBuf, pwg * pwgs_vattr->pwgSize, pwgs_vattr->pwgSize, WRITE);
		}
			// asyncIO
		else {
			//Todo
		}
	}

	void pwgs_vertex_attribute_io_req::loadPWG(pwg_id_t pwg, callback *cb) {
		if (cb == NULL) {
			// syncIO
			vattrIO->access(pwgs_vattr->pwgBuf, pwg * pwgs_vattr->pwgSize, pwgs_vattr->pwgSize, READ);
		}
			// asyncIO
		else {
			//Todo
		}
	}

	vertex_attribute* pwgs_vertex_attribute::getVertexAttributeInPwgBuf(vertex_id_t vid){
		return (vertex_attribute*) (pwgBuf + vid * vAttrSize);
	}
};