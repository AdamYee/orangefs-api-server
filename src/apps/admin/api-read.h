/*
 * api-read.h
 *
 *  Created on: Oct 7, 2011
 *      Author: adam
 */

#ifndef API_READ_H_
#define API_READ_H_

#include <stdlib.h>
#include "pvfs2.h"

/* read 'count' bytes from pvfs2 file 'src', placing the result in
 * 'buffer' */
size_t api_generic_read(pvfs2_file_object *src, char *buffer, int64_t offset,
		size_t count, PVFS_credentials *credentials) {
	PVFS_Request mem_req, file_req;
	PVFS_sysresp_io resp_io;
	PVFS_hint hints = NULL;
	PVFS_hint_import_env(&hints);
	int ret;

	file_req = PVFS_BYTE;
	ret = PVFS_Request_contiguous(count, PVFS_BYTE, &mem_req);
	if (ret < 0) {
		fprintf(stderr, "Error: PVFS_Request_contiguous failure\n");
		return (ret);
	}
	ret = PVFS_sys_read(src->ref, file_req, offset,
			buffer, mem_req, credentials, &resp_io, hints);
	if (ret == 0) {
		PVFS_Request_free(&mem_req);
		PVFS_hint_free(hints);
		return (resp_io.total_completed);
	} else
		PVFS_perror("PVFS_sys_read", ret);

	PVFS_hint_free(hints);
	return ret;
}

#endif /* API_READ_H_ */
