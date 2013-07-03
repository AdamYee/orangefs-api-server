/*
 * api-mkdirs.h
 *
 *  Created on: Oct 4, 2011
 *      Author: adam
 */

#ifndef API_MKDIRS_H_
#define API_MKDIRS_H_

#include <stdlib.h>
#include <libgen.h>
#include "pvfs2.h"
#include "api-rename.h"

static int make_directory(PVFS_credentials     * credentials,
                          const PVFS_fs_id       fs_id,
                          const char           * dir,
                          const char           * pvfs_path,
                          const int              make_parent_dirs);

int api_mkdir(char * dir) {
	int ret = -1, status = 0; /* Get's set if error*/
	char pvfs_path[PVFS_NAME_MAX] = "";
	PVFS_fs_id fs_id;
	PVFS_credentials credentials;

	ret = PVFS_util_resolve(dir, &fs_id, pvfs_path,	PVFS_NAME_MAX);
	if (ret < 0) {
		printf("Error: could not find file system for %s\n", dir);
		return (-1);
	}

	PVFS_util_gen_credentials(&credentials);

	ret = make_directory(&credentials,
						 fs_id,
						 dir,
						 pvfs_path, 1);
	if (ret != 0) {
		printf("cannot create %s\n", dir);
		status = -1;
	}
	return (status);
}

static int make_directory(PVFS_credentials     * credentials,
                          const PVFS_fs_id       fs_id,
                          const char           * dir,
                          const char           * pvfs_path,
                          const int              make_parent_dirs)
{
    int ret = 0;
    char parent_dir[PVFS_NAME_MAX] = "";
    char base[PVFS_NAME_MAX]  = "";
    char realpath[PVFS_NAME_MAX]  = "";
    char * parentdir_ptr = NULL;
    char * basename_ptr = NULL;
    PVFS_sys_attr       attr;
    PVFS_sysresp_lookup resp_lookup;
    PVFS_object_ref     parent_ref;
    PVFS_sysresp_mkdir  resp_mkdir;

    /* Initialize any variables*/
    memset(&resp_lookup, 0, sizeof(resp_lookup));
    memset(&parent_ref,  0, sizeof(parent_ref));
    memset(&resp_mkdir,  0, sizeof(resp_mkdir));

    /* Copy the file name into structures to be passed to dirname and basename
    * These calls change the parameter, so we don't want to mess with original
    * */

    strcpy(parent_dir, pvfs_path);
    parentdir_ptr = dirname(parent_dir);

    strcpy(base,  pvfs_path);
    char* token = strtok(base, "/");
    do {
    	basename_ptr = token;
    	token = strtok(NULL, "/");
    } while (token != NULL);

    /* Make sure we don't try and create the root directory*/
    if( strcmp(basename_ptr, "/") == 0 )
    {
        printf("directory exists\n");
        return(-1);
    }

    /* Set the attributes for the new directory*/
    memset(&attr, 0, sizeof(PVFS_sys_attr));
    attr.owner = credentials->uid;
    attr.group = credentials->gid;
    attr.perms = 0700;
    attr.atime = time(NULL);
    attr.mtime = attr.atime;
    attr.mask = PVFS_ATTR_SYS_ALL_SETABLE;
    attr.dfile_count = 0;

    /* Clear out any info from previous calls*/
    memset(&resp_lookup,  0, sizeof(resp_lookup));

    ret = PVFS_sys_lookup(fs_id,
                          parentdir_ptr,
                          credentials,
                          &resp_lookup,
                          PVFS2_LOOKUP_LINK_FOLLOW, NULL);

    if( ret < 0 &&
        !make_parent_dirs)
    {
        PVFS_perror("PVFS_sys_lookup", ret);
        return(ret);
    }

    if( ret < 0         &&
        make_parent_dirs &&
        ret != -PVFS_ENOENT)
    {
        PVFS_perror("PVFS_sys_lookup", ret);
        return(ret);
    }

    /* The parent directory did not exist. Let's create the parent directory */
    if(ret == -PVFS_ENOENT &&
       make_parent_dirs)
    {
        strcpy(parent_dir, pvfs_path);
        strcpy(realpath,  dir);

        ret = make_directory(credentials,
                             fs_id,
                             dirname(realpath),
                             dirname(parent_dir),
                             make_parent_dirs);

        if(ret == 0)
        {
            ret = PVFS_sys_lookup(fs_id,
                                  parentdir_ptr,
                                  credentials,
                                  &resp_lookup,
                                  PVFS2_LOOKUP_LINK_FOLLOW, NULL);

            if(ret < 0)
            {
                PVFS_perror("PVFS_sys_lookup", ret);
                return(ret);
            }
        }
        else
        {
            return(ret);
        }
    }

    parent_ref.handle = resp_lookup.ref.handle;
    parent_ref.fs_id  = resp_lookup.ref.fs_id;

    /* Clear out any info from previous calls*/
    memset(&resp_mkdir, 0, sizeof(PVFS_sysresp_mkdir));

    ret = PVFS_sys_mkdir(basename_ptr,
                         parent_ref,
                         attr,
                         credentials,
                         &resp_mkdir, NULL);

    if (ret < 0)
    {
        PVFS_perror("PVFS_sys_mkdir", ret);
        return(ret);
    }

    return(0);
}


#endif /* API_MKDIRS_H_ */
