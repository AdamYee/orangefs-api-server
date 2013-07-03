/*
 * api-createfile.h
 *
 *  Created on: Sep 27, 2011
 *      Author: adam
 */

#ifndef API_CREATEFILE_H_
#define API_CREATEFILE_H_

#include <time.h>
#include <stdlib.h>
#include <string.h>
#include "pvfs2.h"
#include "str-utils.h"

int countsegments(char* str)
{
//	char* p = "/mnt/pvfs2/itworks.txt/";
//			"/"; //                 - returns  0 check
//	 	 	"filename"; //          - returns  1 check
//	 	 	"/filename"; //         - returns  1 check
//	 	 	"/filename/"; //        - returns  1 check
//	 	 	"/filename//"; //       - returns  1 check
//	 	 	"/dirname/filename"; // - returns  2 check
//	 	 	"dirname/filename"; //
//	int len = strlen(str);
//	char* pstr = (char*) malloc(len);
//	memset(pstr, 0, sizeof(len));
//	strncpy(pstr, str, len);
//	pstr[len] = '\0';
	if (str == NULL)
	{
		printf("null string, no segments\n");
		return 0;
	} else if (*str == '/' && strlen(str) == 1) {
		printf("Only root slash, return -1\n");
		return 0;
	} else {
		int count = 0;
		char * pch;
		pch = strtok(str, "/");
		while (pch != NULL) {
			count++;
			pch = strtok(NULL, "/");
		}
//		memset(str, 0, sizeof(strlen(str)));
		return count;
	}
//	free(pstr);
}

int api_createfile(char* fullpath)
{
    int ret = 0;
    PVFS_sys_layout layout;

    layout.algorithm = PVFS_SYS_LAYOUT_ROUND_ROBIN;
    layout.server_list.count = 0;
    layout.server_list.servers = NULL;

    int rc;
	int num_segs;
	char directory[PVFS_NAME_MAX];
	char filename[PVFS_SEGMENT_MAX];

	layout.algorithm = PVFS_SYS_LAYOUT_ROUND_ROBIN;
	layout.server_list.count = 0;
	if (layout.server_list.servers) {
		free(layout.server_list.servers);
	}
	layout.server_list.servers = NULL;

	char pvfs_path[PVFS_NAME_MAX] = { 0 };
	PVFS_fs_id cur_fs;
	PVFS_sysresp_lookup resp_lookup;
	PVFS_sysresp_create resp_create;
	PVFS_credentials credentials;
	PVFS_object_ref parent_ref;
	PVFS_sys_attr attr;

	/* Translate path into pvfs2 relative path */
	rc = PINT_get_base_dir(fullpath, directory, PVFS_NAME_MAX);
	char* str = (char*) malloc(strlen(fullpath)+1); // JAS: +1 since we want the null char at end
	strncpy(str, fullpath, strlen(fullpath)+1);
	num_segs = countsegments(str);
	free(str);
	rc = PINT_get_path_element(fullpath, num_segs - 1, filename,
			PVFS_SEGMENT_MAX);
	if (rc) {
		fprintf(stderr, "Unknown path format: %s\n", fullpath);
		ret = -1;
	}

	PVFS_util_gen_credentials(&credentials);

	rc = PVFS_util_resolve(directory, &cur_fs, pvfs_path, 256);
	if (rc) {
		PVFS_perror("PVFS_util_resolve", rc);
		return rc;
	}
	else {
		memset(&resp_lookup, 0, sizeof(PVFS_sysresp_lookup));
		rc = PVFS_sys_lookup(cur_fs,
							 pvfs_path,
							 &credentials,
							 &resp_lookup,
							 PVFS2_LOOKUP_LINK_NO_FOLLOW, NULL);
		if (rc) {
			PVFS_perror("PVFS_sys_lookup", rc);
			return rc;
		}

		/* Set attributes */
		memset(&attr, 0, sizeof(PVFS_sys_attr));
		attr.owner = credentials.uid;
		attr.group = credentials.gid;
		attr.perms = 0777;
		attr.atime = time(NULL);
		attr.mtime = attr.atime;
		attr.mask = PVFS_ATTR_SYS_ALL_SETABLE;
		attr.dfile_count = 0;

		parent_ref = resp_lookup.ref;

		layout.algorithm = PVFS_SYS_LAYOUT_RANDOM;

		rc = PVFS_sys_create(filename, parent_ref, attr, &credentials, NULL,
				&resp_create, &layout, NULL);
		if (rc) {
			fprintf(stderr, "Error: An error occurred while creating %s\n",
					fullpath);
			PVFS_perror("PVFS_sys_create", rc);
			return rc;
		}
	}
    return ret;
}


#endif /* API_CREATEFILE_H_ */
