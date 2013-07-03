/*
 * api-open.h
 *
 *  Created on: Oct 6, 2011
 *      Author: adam
 */

#ifndef API_OPEN_H_
#define API_OPEN_H_

#include <stdlib.h>
#include "pvfs2.h"

typedef struct pvfs2_file_object_s {
    PVFS_fs_id fs_id;
    PVFS_object_ref ref;
    char pvfs2_path[2048];
    char user_path[2048];
    PVFS_sys_attr attr;
    PVFS_permissions perms;
} pvfs2_file_object;

static int api_open(pvfs2_file_object *obj, PVFS_credentials *credentials) {

	PVFS_sysresp_getattr resp_getattr;
	int ret = -1;

	memset(&resp_getattr, 0, sizeof(PVFS_sysresp_getattr));
	ret = PVFS_sys_getattr(obj->ref, PVFS_ATTR_SYS_ALL_NOHINT, credentials,
			&resp_getattr, NULL);
	if (ret) {
		fprintf(stderr, "Failed to do pvfs2 getattr on %s\n",
				obj->pvfs2_path);
		return ret;
	}

	if (resp_getattr.attr.objtype != PVFS_TYPE_METAFILE) {
		fprintf(stderr, "Not a meta file!\n");
		return -1;
	}
	obj->perms = resp_getattr.attr.perms;
	memcpy(&obj->attr, &resp_getattr.attr, sizeof(PVFS_sys_attr));
	obj->attr.mask = PVFS_ATTR_SYS_ALL_SETABLE;

	return 0;
}

#endif /* API_OPEN_H_ */
