#ifndef CROUTINE_EVENT_H
#define CROUTINE_EVENT_H

#include "croutine.h"

enum croutine_main_event_wait_result {
	CROUTINE_MAIN_EVENT_WAIT_DONE = 0,
	CROUTINE_MAIN_EVENT_WAIT_EMPTY,
	CROUTINE_MAIN_EVENT_WAIT_ERROR,
};

struct croutine_main_event_source {
	enum croutine_main_event_wait_result (*blocking_wait)(
		croutine_main_event_source *self);
	void (*collect)(croutine_main_event_source *self);
	int (*wake)(croutine_main_event_source *self);
	void (*suspend)(croutine_main_event_source *self);
	void (*destroy)(croutine_main_event_source *self);
};

int croutine_wait_handle_wake(croutine_wait_handle *handle);
int croutine_wait_handle_get(croutine_wait_handle *handle);
int croutine_wait_handle_release(croutine_wait_handle *handle);

#endif
