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

enum croutine_worker_quiescent_state {
	CROUTINE_WORKER_ACTIVE = 0,
	CROUTINE_WORKER_QUIESCENT,
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
	enum croutine_worker_quiescent_state quiescent_state;

	croutine_schedule schedule;

	pthread_mutex_t local_queue_lock;
	int local_queue_lock_initialized;
	croutine_queue local_queue;
	croutine_list_head dead_tasks;

	struct croutine_main_event_source *main_event_source;
	size_t local_turns;
};

struct croutine_scheduler {
	struct croutine_worker *workers;
	size_t worker_count;
	size_t next_worker;

	pthread_mutex_t state_lock;
	pthread_cond_t state_cond;

	pthread_mutex_t main_queue_lock;
	croutine_queue main_queue;

	pthread_mutex_t tasks_lock;
	croutine_list_head tasks;
	croutine_list_head finished_tasks;

	croutine_list_head event_sources;

	_Atomic uint32_t state;
	_Atomic uint32_t quiescent_workers;

	struct croutine_config config;
};

#endif
