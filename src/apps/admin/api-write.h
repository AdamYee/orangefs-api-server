/*
 * api-write.h
 *
 *  Created on: Oct 6, 2011
 *      Author: adam
 */

#ifndef API_WRITE_H_
#define API_WRITE_H_

#include "pvfs2.h"

/* write 'count' bytes from 'buffer' into pvfs2 file 'dest' */
size_t api_generic_write(pvfs2_file_object *dest, char *buffer, int64_t offset,
		size_t count, PVFS_credentials *credentials) {
	PVFS_Request mem_req, file_req;
	PVFS_sysresp_io resp_io;
	PVFS_hint hints = NULL;
	PVFS_hint_import_env(&hints);
	int ret;

	file_req = PVFS_BYTE;
	ret = PVFS_Request_contiguous(count, PVFS_BYTE, &mem_req);
	if (ret < 0) {
		PVFS_perror("PVFS_Request_contiguous", ret);
		return (ret);
	}
	ret = PVFS_sys_write(dest->ref, file_req, offset, buffer, mem_req,
			credentials, &resp_io, hints);
	if (ret == 0) {
		PVFS_Request_free(&mem_req);
		PVFS_hint_free(hints);
		return (resp_io.total_completed);
	} else
		PVFS_perror("PVFS_sys_write", ret);

	PVFS_hint_free(hints);
	return ret;
}


#endif /* API_WRITE_H_ */
