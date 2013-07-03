/*
 * filename_linklist.h
 *
 *  Created on: Oct 3, 2011
 *      Author: adam
 */

#ifndef FILENAME_LINKLIST_H_
#define FILENAME_LINKLIST_H_

#include <stdlib.h>

struct filename_node {
	char* filename;
	struct filename_node* next;
};

struct filenames_list {
	struct filename_node* head;
};

void addNode(struct filenames_list* filenames, char* name, int len) {
	if (filenames->head == NULL) {
		struct filename_node* fn = (struct filename_node*)malloc(sizeof(struct filename_node*));
		fn->filename = (char*)malloc(len+1); // +1 for \0
		strncpy(fn->filename, name, len);
		fn->filename[len] = '\0';
		fn->next = NULL;
		filenames->head = fn;
	}
	else {
		struct filename_node* newfn = (struct filename_node*) malloc(
				sizeof(struct filename_node*));
		newfn->filename = (char*)malloc(len);
		strncpy(newfn->filename, name, len);
		newfn->filename[len] = '\0';
		newfn->next = NULL;

		struct filename_node* fn = filenames->head;
		while (fn->next != NULL) {
			fn = fn->next;
		}
		fn->next = newfn;
	}
}

//void buildlist(struct filenames_list* filenames ) {
//	int i = 0;
//	for (i = 0; i < 5; i++) {
//		addNode(filenames, "testfile");
//	}
//}

void destroylist(struct filenames_list* filenames) {
	struct filename_node* pntr = filenames->head;
	while (filenames->head != NULL) {
		filenames->head = filenames->head->next;
		free(pntr->filename);
		free(pntr);
		pntr = filenames->head;
	}
}

void printlist(struct filenames_list* filenames) {
	if (filenames->head == NULL) {
		printf("EMPTY LIST\n");
	}
	else {
		struct filename_node* fn = filenames->head;
		while (fn != NULL) {
			printf("%s\n", fn->filename);
			fn = fn->next;
		}
	}
}

#endif /* FILENAME_LINKLIST_H_ */
