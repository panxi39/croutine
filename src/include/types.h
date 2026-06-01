#ifndef CROUTINE_INTERNAL_TYPES_H
#define CROUTINE_INTERNAL_TYPES_H

#include <pthread.h>
#include <stddef.h>
#include <stdint.h>
#include <stdatomic.h>

#include "arch.h"
#include "croutine.h"
#include "croutine_structures.h"

enum croutine_scheduler_state {
	CROUTINE_SCHEDULER_INIT = 0,
	CROUTINE_SCHEDULER_RUNNING,
	CROUTINE_SCHEDULER_STOPPING,
	CROUTINE_SCHEDULER_STOPPED,
	CROUTINE_SCHEDULER_DESTROYING,
};

enum croutine_task_state {
	CROUTINE_TASK_PENDING = 0,
	CROUTINE_TASK_READY,
	CROUTINE_TASK_RUNNING,
	CROUTINE_TASK_WAITING,
	CROUTINE_TASK_FINISHED,
};

enum croutine_wait_handle_type {
	CROUTINE_WAIT_HANDLE_SIMPLE = 0,
	CROUTINE_WAIT_HANDLE_COMPLEX,
};

enum croutine_wait_handle_state {
	CROUTINE_WAIT_HANDLE_PENDING = 0,
	CROUTINE_WAIT_HANDLE_PROCESSING,
	CROUTINE_WAIT_HANDLE_FINISHED,
};

enum croutine_worker_start_state {
	CROUTINE_WORKER_STOPPED = 0,
	CROUTINE_WORKER_STARTED,
};

enum croutine_worker_state {
	CROUTINE_WORKER_RUNNING = 0,
	CROUTINE_WORKER_SEARCHING,
	CROUTINE_WORKER_SOURCE_WAITING,
	CROUTINE_WORKER_SUSPENDING,
	CROUTINE_WORKER_SUSPENDED,
	CROUTINE_WORKER_EXITING,
	CROUTINE_WORKER_EXITED,
};

enum croutine_task_enqueue_result {
	CROUTINE_TASK_ENQUEUE_LOCAL = 0,
	CROUTINE_TASK_ENQUEUE_MAIN,
	CROUTINE_TASK_ENQUEUE_ERROR,
};

enum croutine_task_result_policy {
	CROUTINE_TASK_RESULT_DETACHED = 0,
	CROUTINE_TASK_RESULT_COLLECT,
};

struct croutine_stack {
	void *top;
	void *bottom;
	size_t size;
	void *mmap_base;
	size_t mmap_size;
};

struct croutine_task {
	struct croutine_arch_context context;
	struct croutine_scheduler *scheduler;
	struct croutine_worker *worker;

	croutine_list_head scheduler_node;
	croutine_list_head state_node;

	struct croutine_stack *stack;
	croutine_task_fn func;
	void *arg, *result;
	enum croutine_task_result_policy result_policy;
	_Atomic enum croutine_task_state state;
	_Atomic int schedulable;
};

struct croutine_wait_handle {
	croutine_refcount refcount;
	struct croutine_scheduler *scheduler;
	struct croutine_task *task;
	enum croutine_wait_handle_type type;
	_Atomic enum croutine_wait_handle_state state;
	void *data;
	int (*checker)(void *data);
};

struct croutine_worker {
	struct croutine_scheduler *scheduler;
	pthread_t tid;
	enum croutine_worker_start_state start_state;
	_Atomic enum croutine_worker_state state;

	croutine_schedule schedule;

	croutine_queue local_queue;
	struct croutine_stack *scheduler_stack;
	struct croutine_arch_context scheduler_context;
	size_t local_turns;

	struct croutine_main_event_source *main_event_source;
	size_t reported_suspend_epoch;
};

struct croutine_scheduler {
	struct croutine_worker *workers;
	size_t worker_count;

	pthread_mutex_t state_lock;
	pthread_cond_t state_cond;

	croutine_queue main_queue;

	pthread_mutex_t tasks_lock;
	croutine_list_head tasks;
	croutine_list_head finished_tasks;

	_Atomic enum croutine_scheduler_state state;
	size_t suspended_workers;
	size_t suspend_epoch;

	_Atomic size_t searching_workers;
	_Atomic size_t steal_index;
	_Atomic size_t wake_index;

	struct croutine_config config;
};

#endif
