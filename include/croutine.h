#ifndef CROUTINE_H
#define CROUTINE_H

#include <stddef.h>

#define CROUTINE_DEFAULT_WORKERS 1
#define CROUTINE_DEFAULT_STACK_SIZE (64 * 1024)
#define CROUTINE_DEFAULT_LOCAL_QUEUE_CAPACITY 1024
#define CROUTINE_DEFAULT_INBOX_QUEUE_CAPACITY CROUTINE_DEFAULT_LOCAL_QUEUE_CAPACITY
#define CROUTINE_DEFAULT_MAIN_QUEUE_CAPACITY 1024
#define CROUTINE_DEFAULT_QUEUE_CHECK_INTERVAL 1
#define CROUTINE_DEFAULT_STEAL_BATCH_MAX 64

typedef struct croutine_task croutine_task;
typedef struct croutine_wait_handle croutine_wait_handle;
typedef struct croutine_worker croutine_worker;
typedef struct croutine_scheduler croutine_scheduler;
typedef struct croutine_main_event_source croutine_main_event_source;

typedef void *(*croutine_task_fn)(void *arg);
typedef void (*croutine_schedule)(void);
typedef struct croutine_main_event_source *(*croutine_main_event_source_factory_fn)(croutine_worker *worker,
																					void *args);

typedef struct croutine_main_event_source_config {
	croutine_main_event_source_factory_fn factory_fn;
	void *args;
} croutine_main_event_source_config;

typedef struct croutine_config {
	size_t workers;
	size_t queue_check_interval;
	size_t steal_batch_max;
	struct croutine_main_event_source_config main_event_source_config;
	size_t stack_size;
	size_t local_queue_capacity;
	size_t inbox_queue_capacity;
	size_t main_queue_capacity;
} croutine_config;

int croutine_scheduler_create(croutine_scheduler **out, const croutine_config *config);
int croutine_scheduler_start(croutine_scheduler *scheduler);
int croutine_scheduler_stop(croutine_scheduler *scheduler);
int croutine_scheduler_destroy(croutine_scheduler *scheduler);
croutine_scheduler *croutine_scheduler_current(void);

int croutine_spawn(croutine_scheduler *scheduler, croutine_task_fn func, void *arg);

int croutine_await(void);
int croutine_cancel_await(void);
void croutine_yield(void);
croutine_task *croutine_task_current(void);

#endif
