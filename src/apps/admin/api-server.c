/*
 ============================================================================
 Name        : api-server.c
 Author      : Adam Yee
 Version     :
 Copyright   : Your copyright notice
 Description : OrangeFS API server in C, Ansi-style
 Notes		 : As of 10/16/2011, this is a functional API to the AdamFileSystem
 	 	 	   Hadoop API.
 ============================================================================
 */

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <time.h>
#include <assert.h>
#include <errno.h>
#include <unistd.h>
#include <signal.h>
#include <getopt.h>
#include <pwd.h>
#include <grp.h>
#include <net/if.h>
#include <netdb.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sys/wait.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/resource.h>

/* OrangeFS includes */
#include "pvfs2.h"

/* API serve includes */
#include "api-createfile.h"
#include "api-lookup.h"
#include "api-listfiles.h"
#include "filename_linklist.h"
#include "api-delete.h"
#include "api-mkdirs.h"
#include "api-open.h"
#include "api-write.h"
#include "api-read.h"
#include "api-rename.h"

///////////////////////////// server ///////////////////////////

struct addrinfo *res;
char *buffer_read; // buffer to send bytes with
char *buffer_req;
int listen_sockfd;

typedef struct api_request
{
	char id[2];
	char * data;
} api_request;

typedef struct read_request
{
	uint64_t position;
	long length;
	int buffersize;
} read_request;

#define enable_fork

int sendall(int s, char *buf, int len)
{
	int total = 0;		 			 // how many bytes we've sent
	int bytesleft = len;			 // how many we have left to send
	int n = 0;

	while(total < len) {
		n = send(s, buf+total, bytesleft, 0);
		if (n == -1) { break; }
		total += n;
		bytesleft -= n;
		//printf("bytes_sent: %d\n", n);
	}

	return (n==-1)?-1:0; // return -1 on failure, 0 on success
}

void send_generic_response(int resp_id, int ret_code, int sockfd) {
	char response[5];
	int n;
	if (ret_code == 0) {
		n = sprintf(response, "0%d%d\r", resp_id, 1); // 1 success
	} else {
		n = sprintf(response, "0%d%d\r", resp_id, 0); // 0 failure
	}
	sendall(sockfd, response, n);
}

void log_response(FILE* fp, char* func, int code, int opcode) {
	if (code == 0) {
		fprintf(fp, "%s Succeeded, response: 0%d1\r\n", func, opcode);
	} else {
		fprintf(fp, "%s Failed, response: 0%d0\r\n", func, opcode);
	}
}

void sigchld_handler(int s)
{
    while(waitpid(-1, NULL, WNOHANG) > 0);
}

void sigcleanup_handler(int s)
{
  printf("API Server: Cleanup handler running for SIGTERM, SIGINT, or SIGSEGV\n");

  freeaddrinfo(res);
  free(buffer_read);
  free(buffer_req);
  close(listen_sockfd);
  exit(s);
}

int main(void)
{
	/* API server */
	struct sigaction sa;

	FILE *fp_parent;
	int r = 0;
#ifdef enable_fork
	int result = 0;
#endif
	signal(SIGINT, sigcleanup_handler);
	signal(SIGTERM, sigcleanup_handler);
	signal(SIGSEGV, sigcleanup_handler);

	fp_parent = fopen("/tmp/apiserver.log", "a+");
	fprintf(fp_parent, "\n----------------------------------\n");
	fprintf(fp_parent, "Starting the API server.\n");
	fprintf(fp_parent, "----------------------------------\n");
	printf("Starting the API server.\n");

	int new_sockfd, status;
	struct addrinfo hints;
	struct sockaddr_storage their_addr; // connectors' address info
	socklen_t sin_size; // size of in addr

	memset(&hints, 0, sizeof(struct addrinfo));
	hints.ai_family = AF_UNSPEC; // don't care if IPv4 or IPv6
	hints.ai_socktype = SOCK_STREAM; // TCP
	hints.ai_flags = AI_PASSIVE; // fill in my IP for me (listening IP)

	// Setup the IP
	status = getaddrinfo(NULL, "9999", &hints, &res);
	if (status != 0) {
		fprintf(fp_parent, "getaddrinfo error: %s\n", gai_strerror(status));
		exit(1);
	}
//	fprintf(fp_parent, "IP setup success\n");

	// Create the listening socket
	listen_sockfd = socket(res->ai_family, res->ai_socktype, res->ai_protocol);
	if (listen_sockfd < 0) {
		fprintf(fp_parent, "socket error: %s\n", strerror(listen_sockfd));
	}
//	fprintf(fp_parent, "create socket success\n");

	// lose the pesky "Address already in use" error message
	int yes=1;
	if (setsockopt(listen_sockfd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(int))
			== -1) {
		perror("setsockopt");
	}
//	fprintf(fp_parent, "setsockopt success\n");

	// Bind the socket to the IP
	status = bind(listen_sockfd, res->ai_addr, res->ai_addrlen);
	if (status == -1) {
		close(listen_sockfd);
		perror("server: bind");
	}
//	fprintf(fp_parent, "bind succes\n");

	// Listen
	status = listen(listen_sockfd, 10);
	if (status == -1) {
		perror("listen");
	}
//	fprintf(fp_parent, "listenning...\n");

	fclose(fp_parent);

	sa.sa_handler = sigchld_handler; // reap all dead processes
	sigemptyset(&sa.sa_mask);
	sa.sa_flags = SA_RESTART;
	if (sigaction(SIGCHLD, &sa, NULL) == -1) {
		perror("sigaction");
		exit(1);
	}

	int nbytes = 0;
	// only need a small buffer to receive commands from Hadoop
	char hadoopBuf[512] = "";
	char readBuf[512] = "";
	char tempReadBuf[513] = "";
	//char * strtok_r_ptr;
	struct filenames_list filenames;
	filenames.head = NULL;
	//char * buffer_read; // the buffer to send bytes with
	int buffer_read_size = 64*1024;
	buffer_read = (char*)malloc(buffer_read_size);
	//char * buffer_req;
	int buffer_req_size = 1024;
	buffer_req = (char *)malloc(buffer_req_size);

	while (1) // main accepting loop
	{
		// Accept connections
		sin_size = sizeof their_addr;
		new_sockfd = accept(listen_sockfd, (struct sockaddr *) &their_addr,
				&sin_size);
		if (new_sockfd == -1) {
			perror("accept");
			exit(1);
		}

#ifdef enable_fork
		result = fork();
		if(result < 0)
		{
			//printf("FATAL ERROR: Unable to fork\n");
			exit(-1);
		}
		else if(result == 0)
		{ // this is the child process
			close(listen_sockfd); // child doesn't need the listener
#endif
			//printf("CHILD %i RUNNING\n", getpid());

			/* Initialize the pvfs2 server - only need to do this ONCE */
			int ret;
			ret = PVFS_util_init_defaults();
			if(ret < 0)
			{
				PVFS_perror("PVFS_util_init_defaults", ret);
				return -1;
			}

//			memset(&hadoopBuf, 0, 512);
			FILE *fp_child;
			fp_child = fopen("/tmp/apiserver.log", "a+");
			if(fp_child==NULL)
			  printf("Unable to open file in child process!");

			// RECEIVE
			nbytes = recv(new_sockfd, hadoopBuf, 512, 0);
			if (nbytes == -1) {
				perror("recv");
			}
			if (hadoopBuf[0] == 0) continue; // cannot process empty buffers
			hadoopBuf[nbytes-1] = '\0'; // set null character 1 position before end to get rid of \n
//			fprintf(fp_child, "received: %s\n", hadoopBuf);
//			printf("received: %s\n", hadoopBuf);

			api_request req;
			pvfs2_file_object obj;
			strncpy(req.id, hadoopBuf, 2);

			if(buffer_req_size < 10 + nbytes - 2 + 1) // +1 for the null char
			  {
			    buffer_req = realloc(buffer_req, 10 + nbytes - 2 + 1);
			    buffer_req_size = 10 + nbytes - 2 + 1;
			  }

			req.data = buffer_req;
			strncpy(req.data, "/mnt/pvfs2", 10);
			strncpy(&req.data[10], &hadoopBuf[2], nbytes-2);

			if (!strncmp(req.id, "01", 2))
			{ // GET_FILE_STATUS
//				fprintf(fp_child, "LOOKUP path: %s\n", req.data);
				//printf("LOOKUP path: %s\n", req.data);
				PVFS_object_ref ref;
				memset(&ref,0,sizeof(PVFS_object_ref));
				r = api_lookup(req.data, &ref);

				PVFS_credentials credentials;
				PVFS_sysresp_getattr getattr_response;
				memset(&getattr_response,0,sizeof(PVFS_sysresp_getattr));
				char response[512];
				int n;
				if (r == 0)
				{
					if (PVFS_sys_getattr(ref, PVFS_ATTR_SYS_ALL, &credentials, &getattr_response, NULL) == 0)
					{ // found PVFS_obj

						int isDir = 0;
						if (getattr_response.attr.objtype == 4) {
							isDir = 1;
						}

						int length = getattr_response.attr.size;
						int blksize = getattr_response.attr.blksize;
						int mtime = getattr_response.attr.mtime;
						int atime = getattr_response.attr.atime;
						int owner = getattr_response.attr.owner;
						int group = getattr_response.attr.group;
						struct passwd* pw = getpwuid((uid_t)owner);
						struct group* grp = getgrgid((gid_t)group);
						int perms = getattr_response.attr.perms;

						n = sprintf(response, "01%d%d %d %d %d %d %o %s %s",
												1, // success_code
												length,
												isDir,
												blksize,
												mtime,
												atime,
												perms,
												pw->pw_name,
												grp->gr_name);
						strncat(response, "\r", 1);
						if (sendall(new_sockfd, response, n+1) == -1) {
							fprintf(fp_child, "sendall failed\n");
						}
	//					fprintf(fp_child, "LOOKUP Succeeded, response: %s\n", response);
					}
					else
					{ // PVFS_obj doesn't exist
						send_generic_response(1, -1, new_sockfd);
	//					log_response(fp, "LOOKUP", -1, 1);
					}
				}
				else
				{ // lookup failed
					send_generic_response(1, r, new_sockfd);
				}
			}
			else if (!strncmp(req.id, "02", 2))
			{ // LIST_STATUS
//				fprintf(fp_child, "LIST_STATUS path: %s\n", req.data);
				//printf("LIST_STATUS path: %s\n", req.data);
				char response[1000];
				PVFS_object_ref ref;
				int n, i = 0;
				r = api_listfiles(req.data, ref, &filenames);
				//printlist(&filenames);
				if (r == 0)
				{
					n = sprintf(response, "021"); // 1 success
					i += n;
					struct filename_node* fn = filenames.head;
					if (fn == NULL) { // insert carriage return -> "021\r"
						strncpy(&response[i], "\r", 1);
						i += 1;
					}
					while (fn != NULL) {
						strncpy(&response[i], fn->filename, strlen(fn->filename));
						i += strlen(fn->filename);
						if (fn->next == NULL) {
							strncpy(&response[i], "\r", 1);
						} else {
							strncpy(&response[i], " ", 1);
						}
						i += 1;
						fn = fn->next;
					}
					response[i] = '\0';
					sendall(new_sockfd, response, strlen(response));
//					fprintf(fp_child, "LIST_STATUS Succeeded, response: %s\n", response);
					destroylist(&filenames);
				}
				else {
					// failed to readdir
					send_generic_response(2, r, new_sockfd);
//					log_response(fp_child, "LIST_STATUS", -1, 2);
				}
			}
			else if (!strncmp(req.id, "03", 2))
			{ // DELETE
//				fprintf(fp_child, "DELETE file: %s\n", req.data);
				r = api_delete(req.data);
				send_generic_response(3, r, new_sockfd);
//				log_response(fp_child, "DELETE", r, 3);
			}
			else if (!strncmp(req.id, "04", 2))
			{ // MKDIRS
//				fprintf(fp_child, "MKDIRS directory: %s\n", req.data);
//				req.data = &req.data[10];
				r = api_mkdir(req.data);
				send_generic_response(4, r, new_sockfd);
//				log_response(fp_child, "MKDIR", r, 4);
			}
			else if (!strncmp(req.id, "05", 2))
			{ // CREATE_FILE
//				fprintf(fp_child, "CREATE_FILE file: %s\n", req.data);
				r = api_createfile(req.data);
				send_generic_response(5, r, new_sockfd);
//				log_response(fp_child, "CREATE_FILE", r, 5);
			}
			else if (!strncmp(req.id, "06", 2))
			{ // CREATE (write)
				// Step 1: lookup the write file
				PVFS_object_ref ref;
//				memset(&ref,0,sizeof(PVFS_object_ref));
//				fprintf(fp_child, "CREATE (write) file: %s\n", req.data);
//				printf("CREATE (write) file: %s\n", req.data);
				r = api_lookup(req.data, &ref); // doing our own lookup
				if (r == 0) {
					// Step 2: open the file to write and return success_code
					obj.ref = ref; // copying the lookup ref to PVFS_object_ref.ref
					PVFS_credentials credentials;
					PVFS_util_gen_credentials(&credentials);
					r = api_open(&obj, &credentials); // "OPEN" the file, getting attr info

					// Tell Hadoop we're ready to write data
					// If the open failed, Hadoop will be notified and throw the proper exception
					send_generic_response(6, r, new_sockfd);
//					log_response(fp_child, "CREATE (write) open", r, 6);

					if (r == 0) {
						// RECEIVE STREAM
						// Step 3: if the file was successfully opened, write to it
//						fprintf(fp_child, "CREATE opened file: %s\n", req.data);
						PVFS_credentials credentials;
						PVFS_util_gen_credentials(&credentials);
						// create a buffer to receive data
//						char buffer_write[4096]; // 4KB
						char buffer_write[1048576]; // 1MB
//						char buffer_write[8388608]; // 8MB
//						memset(&buffer_write, 0, 4096);
						uint64_t offset = 0;
						// Loop receive and write data to file until receive fails
						do {
							// Receiving Loop
							nbytes = recv(new_sockfd, buffer_write, 1048576, 0);
							// while receive data, write to the file
							r = api_generic_write(&obj, buffer_write, offset, nbytes,
									&credentials);
							if ( r < 0 ) {
								fprintf(fp_child, "CREATE failed, return code %d\n", r);
							} else {
								offset += r;
							}
						} while (nbytes > 0);
						// We don't need to respond to Hadoop's write. Either it worked or didn't.
//						fprintf(fp_child, "CREATE (write) finished, total bytes written: %d\n", offset);
//						printf("CREATE (write) finished, total bytes written: %d\n", offset);
						// reset the file object
						memset(&obj, 0, sizeof(pvfs2_file_object));
					} // end file open
					else {
						fprintf(fp_child, "CREATE failed to open file\n");
					}
				} else {
					// Failed to lookup file, r != 0
//					fprintf(fp_child, "CREATE api_lookup failed, return code %d\n", r);
					send_generic_response(6, r, new_sockfd);
//					log_response(fp_child, "CREATE-lookup", r, 6);
				} // end file lookup
			}
			else if (!strncmp(req.id, "07", 2))
			{ // OPEN (read)
				// Step 1: lookup the read file
				PVFS_object_ref ref;
				memset(&ref,0,sizeof(PVFS_object_ref));
//				fprintf(fp_child, "OPEN (read) file: %s\n", req.data);
//				printf("OPEN (read) file: %s\n", req.data);
				r = api_lookup(req.data, &ref); // doing our own lookup, *** RP2.a ***
				if (r ==0 ) {
					// Step 2: open the file to read and return success_code
					obj.ref = ref; // copying the lookup ref to PVFS_object_ref.ref
					PVFS_credentials credentials;
					PVFS_util_gen_credentials(&credentials);
					r = api_open(&obj, &credentials); // "OPEN" the file, getting attr info *** RP2.b ***

					// Tell Hadoop we're ready to stream data.
					// If the open failed, Hadoop will be notified and
					// throw the proper exception.
					char response[50];
					int n;
					if (r == 0) {
					  n = sprintf(response, "071%i\r", (int)obj.attr.size);
//						printf("OPEN (read) file size: %i\n", obj.attr.size);
						response[n] = '\0';
						sendall(new_sockfd, response, n); // ***RP3.a***
					} else {
						n = sprintf(response, "070\r");
//						printf("OPEN (read) failed to open file!\n");
						response[n] = '\0';
						sendall(new_sockfd, response, n); // ***RP3.a***
						continue;
					}
//					log_response(fp_child, "OPEN (read) open", r, 7);

					if (r == 0) {
//						fprintf(fp_child, "OPEN (read) - BLOCKING AT RECV\n");
//						printf("OPEN (read) - BLOCKING AT RECV\n");
						// now enter the read stream loop
						uint64_t offset = 0, total_read = 0;
						long bytes_to_read = 4096;
						read_request read_req;
						char * delim1, * delim2;
						int field_len;
						do { // *** RP3.b ***
							// Step 3: After we've opened the file, wait on
							// the specific read command
							nbytes = recv(new_sockfd, readBuf, 512, 0); // *** RP7.a ***
							// set null character 1 position before end to get rid of \n from Java's println
							readBuf[nbytes-1] = '\0';
//							fprintf(fp_child, "OPEN (read) received: %s\n", readBuf);
							//printf("OPEN (read) received: '%s'\n", readBuf);

							// Split into position:length fields
							// Some records are AAA:BBB
							// Other records are AAA:BBB\nCCC:DDD\n, ...
							delim1 = strchr(readBuf, ':');
							field_len = delim1-readBuf;
							strncpy(tempReadBuf, readBuf, field_len);
							tempReadBuf[field_len]='\0';
							//printf("trb='%s'\n", tempReadBuf);
							read_req.position = atol(tempReadBuf);

							delim2 = strchr(delim1, '\n');
							if(delim2 == NULL)
							  // Simple: AAA:BBB
							  strcpy(tempReadBuf, (delim1+1));
							else
							  {
							    // Complex: AAA:BBB\nCCC:DDD\n...
							    // delim1 should point to :
							    // delim2 should point to \n
							    field_len = delim2-delim1-1;
							    strncpy(tempReadBuf, (delim1+1), field_len);
							    tempReadBuf[field_len] = '\0';
							  }
							//printf("trb2='%s'\n", tempReadBuf);
							read_req.length = atol(tempReadBuf);
							
							/*
							  // JAS - Original Adam code
							  // Valgrind was unhappy because strtok would
							  // sometimes produce NULL tokens
                                                        strncpy(tempReadBuf, readBuf, nbytes);
                                                        // tokenize temp into position and length
							read_req.position = atol(strtok(tempReadBuf, ":"));
							read_req.length = atol(strtok(NULL, ":"));
							*/

							//printf(" pos=%li, len=%li\n", 
							//      read_req.position, read_req.length);

							// SEND STREAM
							// Step 4: if the file was successfully opened
							// read it!
							offset = read_req.position;
							bytes_to_read = read_req.length;
							if(buffer_read_size < bytes_to_read)
							  {
							    // Buffer too small - resize!
							    buffer_read = realloc(buffer_read, bytes_to_read);
							    buffer_read_size = bytes_to_read;
							  }

							r = api_generic_read(&obj, buffer_read, offset,
									bytes_to_read, &credentials); // *** RP7.b ***
							// stream it!
							if (r < 0) {
								fprintf(fp_child, "OPEN (read) stream to Hadoop failed, return code %d\n", r);
							} else {
								sendall(new_sockfd, buffer_read, r); // *** RP7.c ***
								total_read += r;
							}

						} while (r > 0);

//						fprintf(fp_child, "OPEN (read) finished, total bytes read: %d\n", total_read);
//						printf("OPEN (read) finished, total bytes read: %d\n", total_read);
						// reset the file object
//						memset(&obj, 0, sizeof(pvfs2_file_object));
						// free temp read buffer
						//free(tempReadBuf);

					} // end file open
					else {
						fprintf(fp_child, "OPEN failed to open the file\n");
					}
				}
				else {
					// Failed to lookup file, r != 0
					fprintf(fp_child, "READ api_lookup failed, return code %d\n", r);
					send_generic_response(7, r, new_sockfd);
//					log_response(fp_child, "READ-lookup", r, 7);
				} // end file lookup
			}
			else if (!strncmp(req.id, "09", 2))
			{ // RENAME
				char* origName = strtok(req.data, ":");
				char* new_name = strtok(NULL, "\r\n");
				char* newName = (char*)malloc(10 + strlen(new_name)+1);
				strncpy(newName, "/mnt/pvfs2", 10);
				strncpy(&newName[10], new_name, strlen(new_name));
				newName[10+strlen(new_name)] = '\0';
//				fprintf(fp_child, "RENAME %s to %s\n", origName, newName);
				r = api_rename(origName, newName);
				send_generic_response(9, r, new_sockfd);
//				log_response(fp_child, "RENAME", r, 9);
				free(newName);
			}
			else if (!strncmp(req.id, "10", 2))
			{ // GET_FILE_LOCATIONS
//				ret = PVFS_util_resolve(filename, &(obj->u.pvfs2.fs_id),
//					    obj->u.pvfs2.pvfs2_path, PVFS_NAME_MAX);
//				obj->fs_type = PVFS2_FILE;
//					strncpy(obj->u.pvfs2.user_path, filename, PVFS_NAME_MAX);

			}
			else
			{ // ECHO

			} // end request handling

			PVFS_sys_finalize();
			free(buffer_read);
			free(buffer_req);
			close(new_sockfd);
			fclose(fp_child);
			freeaddrinfo(res);
			//printf("CHILD %i EXITING\n", getpid());
#ifdef enable_fork
			exit(0);
		} // end if fork
		else
		{
		  // This is the parent process
		  // Parent doesn't need this
		  close(new_sockfd);
		  //printf("PARENT RUNNING AFTER FORKING PID %i\n", result);
		}
#endif

	} // end while

	// Should never get here - parent loops until CTRL-C

	return 0;
}
