#ifndef CROUTINE_H
#define CROUTINE_H

#include <stddef.h>

typedef struct croutine_task croutine_task;
typedef struct croutine_wait_handle croutine_wait_handle;
typedef struct croutine_worker croutine_worker;
typedef struct croutine_scheduler croutine_scheduler;
typedef struct croutine_main_event_source croutine_main_event_source;

typedef void *(*croutine_task_fn)(void *arg);
typedef void (*croutine_schedule)(void);
typedef struct croutine_main_event_source *(
	*croutine_main_event_source_factory_fn)(croutine_worker *worker,
											void *args);

typedef struct croutine_main_event_source_config {
	croutine_main_event_source_factory_fn factory_fn;
	void *args;
} croutine_main_event_source_config;

typedef struct croutine_config {
	size_t workers;
	size_t main_queue_quota;
	struct croutine_main_event_source_config main_event_source_config;
} croutine_config;

int croutine_scheduler_create(croutine_scheduler **out,
							  const croutine_config *config);
int croutine_scheduler_start(croutine_scheduler *scheduler);
int croutine_scheduler_stop(croutine_scheduler *scheduler);
int croutine_scheduler_destroy(croutine_scheduler *scheduler);
croutine_scheduler *croutine_scheduler_current(void);

int croutine_spawn(croutine_scheduler *scheduler, croutine_task_fn func,
				   void *arg);

void croutine_yield(void);
croutine_task *croutine_task_current(void);

#endif
