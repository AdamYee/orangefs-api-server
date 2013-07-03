/*
 * api-delete.h
 *
 *  Created on: Oct 3, 2011
 *      Author: adam
 */

#ifndef API_DELETE_H_
#define API_DELETE_H_

#include <stdlib.h>
#include <pvfs2.h>
#include "api-createfile.h"

int api_delete(char* full_path)
{
	/* Remove specified file */
	int rc;
	int num_segs;
	char directory[PVFS_NAME_MAX];
	char filename[PVFS_SEGMENT_MAX];

	char pvfs_path[PVFS_NAME_MAX] = {0};
	PVFS_fs_id cur_fs;
	PVFS_sysresp_lookup resp_lookup;
	PVFS_credentials credentials;
	PVFS_object_ref parent_ref;

	PVFS_util_gen_credentials(&credentials);

	/* Translate path into pvfs2 relative path */
	rc = PVFS_util_resolve(full_path, &cur_fs, pvfs_path,
		PVFS_NAME_MAX);
	if(rc < 0)
	{
		PVFS_perror("PVFS_util_resolve", rc);
		return -1;
	}

	/* break into file and directory */
	rc = PINT_get_base_dir(pvfs_path, directory, PVFS_NAME_MAX);
	if(rc < 0)
	{
		PVFS_perror("PINT_get_base_dir", rc);
		return -1;
	}
	num_segs = PINT_string_count_segments(pvfs_path);
	rc = PINT_get_path_element(pvfs_path, num_segs - 1,
							   filename, PVFS_SEGMENT_MAX);

	if (rc)
	{
		PVFS_perror("PINT_get_path_element", rc);
		return -1;
	}

	memset(&resp_lookup, 0, sizeof(PVFS_sysresp_lookup));
	rc = PVFS_sys_lookup(cur_fs, directory, &credentials,
						 &resp_lookup, PVFS2_LOOKUP_LINK_NO_FOLLOW, NULL);
	if (rc)
	{
		PVFS_perror("PVFS_sys_lookup", rc);
		return -1;
	}

	parent_ref = resp_lookup.ref;
	rc = PVFS_sys_remove(filename, parent_ref, &credentials, NULL);
	if (rc)
	{
		fprintf(stderr, "Error: An error occurred while "
				"removing %s\n", full_path);
		PVFS_perror("PVFS_sys_remove", rc);
		return -1;
	}

	return 0;
}

#endif /* API_DELETE_H_ */
