/*
 * api-lookup.h
 *
 *  Created on: Sep 30, 2011
 *      Author: adam
 */

#ifndef API_LOOKUP_H_
#define API_LOOKUP_H_

#include <stdio.h>

#include "pvfs2.h"

int api_lookup(char *full_path, PVFS_object_ref *ret_ref) {
    char pvfs2_path[2048];
    int ret;
    PVFS_fs_id fs_id;
    PVFS_sysresp_lookup resp_lookup;
    PVFS_credentials credentials;

    PVFS_util_gen_credentials(&credentials);
    ret = PVFS_util_resolve(full_path, &fs_id, pvfs2_path, 256);
    if (ret < 0)
    {
        return ret;
    } else {
        memset(&resp_lookup, 0, sizeof(PVFS_sysresp_lookup));
        ret = PVFS_sys_lookup(fs_id, 
                              pvfs2_path,
                              &credentials, 
                              &resp_lookup,
                              PVFS2_LOOKUP_LINK_NO_FOLLOW, NULL);
        if (ret < 0)
        {
            return ret;
        }
        ret_ref->handle = resp_lookup.ref.handle;
        ret_ref->fs_id = resp_lookup.ref.fs_id;
    }
    return 0;
}

#endif /* API_LOOKUP_H_ */
