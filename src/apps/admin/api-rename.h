/*
 * api-rename.h
 *
 *  Created on: Oct 15, 2011
 *      Author: adam
 */

#ifndef API_RENAME_H_
#define API_RENAME_H_

#include "pvfs2.h"
#include <stdlib.h>

int lookupParent(char *full_path, PVFS_object_ref *ret_parent_ref,
		char *ret_filename) {
	int rc;
	int num_segs;
	char directory[2048];
	char pvfs_path[2048] = { 0 };
	PVFS_fs_id cur_fs;
	PVFS_sysresp_lookup resp_lookup;
	PVFS_credentials credentials;

	/* Translate path into pvfs2 relative path */
	rc = PINT_get_base_dir(full_path, directory, 2048);
	char* str = (char*) malloc(strlen(full_path));
	strncpy(str, full_path, strlen(full_path));
	num_segs = countsegments(str);
	free(str);
	rc = PINT_get_path_element(full_path, num_segs - 1, ret_filename,
			2048);

	if (rc) {
		return rc;
	}

	rc = PVFS_util_resolve(directory, &cur_fs, pvfs_path, 2048);
	if (rc) {
		return rc;
	}

	PVFS_util_gen_credentials(&credentials);

	memset(&resp_lookup, 0, sizeof(PVFS_sysresp_lookup));
	rc = PVFS_sys_lookup(cur_fs, pvfs_path, &credentials, &resp_lookup,
			PVFS2_LOOKUP_LINK_NO_FOLLOW, NULL);
	if (rc) {
		return rc;
	}

	*ret_parent_ref = resp_lookup.ref;

	return 0;
}

int api_rename(char* origName, char* newName) {
	int rc;
//	char orig_working_file[2048];
	char orig_filename[2048];
	PVFS_object_ref orig_parent_ref;
//	char new_working_file[2048];
	char new_filename[2048];
	PVFS_object_ref new_parent_ref;
	PVFS_credentials credentials;

	rc = lookupParent(origName, &orig_parent_ref, orig_filename);
	if (rc) {
		printf("Parent of %s not found!\n", origName);
		return rc;
	}

	rc = lookupParent(newName, &new_parent_ref, new_filename);
	if (rc) {
		printf("Parent of %s not found!\n", newName);
		return rc;
	}

	PVFS_util_gen_credentials(&credentials);
	rc = PVFS_sys_rename(orig_filename, orig_parent_ref, new_filename,
			new_parent_ref, &credentials, NULL);
	if (rc) {
		printf("Could not rename %s to %s!\n", origName, newName);
		return rc;
	}
	return 0;
}

#endif /* API_RENAME_H_ */
