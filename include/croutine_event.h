#ifndef CROUTINE_EVENT_H
#define CROUTINE_EVENT_H

#include "croutine.h"

struct croutine_main_event_source {
	void (*blocking_wait)(croutine_main_event_source *self);
	void (*collect)(croutine_main_event_source *self);
	int (*wake)(croutine_main_event_source *self);
	void (*suspend)(croutine_main_event_source *self);
	int (*resume)(croutine_main_event_source *self);
	void (*destroy)(croutine_main_event_source *self);
};

int croutine_wait_handle_wake(croutine_wait_handle *handle);
int croutine_wait_handle_get(croutine_wait_handle *handle);
int croutine_wait_handle_release(croutine_wait_handle *handle);

#endif
