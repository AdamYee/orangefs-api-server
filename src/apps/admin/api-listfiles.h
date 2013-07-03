/*
 * api-listfiles.h
 *
 *  Created on: Oct 2, 2011
 *      Author: adam
 */

#ifndef API_LISTFILES_H_
#define API_LISTFILES_H_

#include <stdio.h>

#include "pvfs2.h"
#include "filename_linklist.h"

static void throwPVFS2Exception(int error){
    char error_string[256];
    PVFS_strerror_r(error, error_string, sizeof(error_string));
}

int api_listfiles(char* dirname, PVFS_object_ref ret_ref, struct filenames_list* filenames)
{

	PVFS_sysresp_readdir rd_response;
	PVFS_credentials credentials;
	uint64_t dir_version = 0;
	PVFS_ds_position token;

	int rc = api_lookup(dirname, &ret_ref);
//	printf("<listfiles> LOOKUP return code %d\n", rc);
	if (rc < 0)
	{ // failed to find file
		throwPVFS2Exception(rc);
		return rc;
	}
	else
	{
        token = 0;
        int ret, i;
        PVFS_util_gen_credentials(&credentials);
        do
        {
            memset(&rd_response, 0, sizeof(PVFS_sysresp_readdir));
            ret = PVFS_sys_readdir(ret_ref, (!token ? PVFS_READDIR_START : token), 64, &credentials, &rd_response, NULL);
            if(ret < 0)
            {
                throwPVFS2Exception(ret);
                return ret; // "java/io/IOException"
            }

            if (dir_version == 0)
            {
                dir_version = rd_response.directory_version;
            }
            else if (dir_version != rd_response.directory_version)
            {
                return -2; // "java/io/IOException" "Directory has been modified during a readdir operation"
            }


            for (i = 0; i < rd_response.pvfs_dirent_outcount; i++) {
				addNode(filenames, rd_response.dirent_array[i].d_name,
						strlen(rd_response.dirent_array[i].d_name));
			}

            token += rd_response.pvfs_dirent_outcount;

            if (rd_response.pvfs_dirent_outcount)
            {
                free(rd_response.dirent_array);
                rd_response.dirent_array = NULL;
            }
        } while(rd_response.pvfs_dirent_outcount == 64);
    }
	return 0;
}

#endif /* API_LISTFILES_H_ */
