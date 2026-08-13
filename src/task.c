#include "scheduler.h"
#include "stack.h"
#include "task.h"
#include "tls.h"
#include "types.h"
#include "worker.h"

#include <stdatomic.h>
#include <stdlib.h>
#include <string.h>

static void croutine_task_entry_wrapper(void) {
	struct croutine_task *task = croutine_current_task;

	if (task == NULL || task->scheduler == NULL || task->func == NULL)
		abort();

	task->result = task->func(task->arg);
	atomic_store_explicit(&task->state, CROUTINE_TASK_FINISHED, memory_order_release);
	croutine_task_enter_scheduler();
	abort();
}

int croutine_task_init(struct croutine_task *task, struct croutine_scheduler *scheduler, croutine_task_fn func,
					   void *arg) {
	if (task == NULL || scheduler == NULL || func == NULL)
		return -1;

	memset(task, 0, sizeof(*task));
	task->scheduler = scheduler;
	atomic_init(&task->worker, NULL);
	croutine_list_init(&task->scheduler_node);
	croutine_list_init(&task->state_node);
	task->stack = croutine_stack_alloc(task->scheduler->config.stack_size);
	if (task->stack == CROUTINE_STACK_ERROR)
		return -1;
	task->func = func;
	task->arg = arg;
	task->result = NULL;
	task->result_policy = CROUTINE_TASK_RESULT_DETACHED;
	atomic_init(&task->state, CROUTINE_TASK_PENDING);

	if (croutine_arch_context_init(&task->context, task->stack->bottom, task->stack->size,
								   croutine_task_entry_wrapper) != 0) {
		croutine_stack_free(task->stack);
		task->stack = NULL;
		return -1;
	}

	return 0;
}

void croutine_task_init_current(struct croutine_task *task) {
	if (task == NULL) {
		croutine_current_task = NULL;
		return;
	}

	atomic_store_explicit(&task->state, CROUTINE_TASK_RUNNING, memory_order_release);
	croutine_current_task = task;
}

void croutine_task_enter_scheduler(void) {
	struct croutine_task *task = croutine_current_task;
	struct croutine_worker *worker;

	if (task == NULL)
		abort();

	worker = atomic_load_explicit(&task->worker, memory_order_relaxed);
	if (worker == NULL || worker != croutine_current_worker || worker->schedule == NULL)
		abort();

	croutine_arch_store_and_call(&task->context, worker->schedule);
}

void croutine_task_resume(struct croutine_task *task) {
	struct croutine_worker *worker;
	enum croutine_task_state expected;

	if (task == NULL)
		abort();

	worker = atomic_load_explicit(&task->worker, memory_order_relaxed);
	if (worker == NULL || worker != croutine_current_worker)
		abort();

	expected = CROUTINE_TASK_READY;
	if (!atomic_compare_exchange_strong_explicit(&task->state, &expected, CROUTINE_TASK_RUNNING, memory_order_acq_rel,
												 memory_order_acquire))
		abort();

	croutine_current_task = task;
	croutine_arch_resume_and_ret(&task->context);
	abort();
}

enum croutine_task_enqueue_result croutine_task_enqueue(struct croutine_task *task) {
	struct croutine_worker *home;

	if (task == NULL || task->scheduler == NULL ||
		atomic_load_explicit(&task->state, memory_order_acquire) != CROUTINE_TASK_READY)
		return CROUTINE_TASK_ENQUEUE_ERROR;

	home = atomic_load_explicit(&task->worker, memory_order_relaxed);
	if (home != NULL) {
		if (home == croutine_current_worker && croutine_worker_enqueue_local(home, task) == 0)
			return CROUTINE_TASK_ENQUEUE_LOCAL;
		if (croutine_worker_enqueue_inbox(home, task) == 0)
			return CROUTINE_TASK_ENQUEUE_INBOX;
	}

	if (croutine_scheduler_enqueue_main(task->scheduler, task) != 0)
		abort();
	return CROUTINE_TASK_ENQUEUE_MAIN;
}

int croutine_task_wake(struct croutine_task *task) {
	enum croutine_scheduler_state scheduler_state;
	enum croutine_task_state expected;
	enum croutine_task_state next;
	int enqueue;

	if (task == NULL || task->scheduler == NULL)
		return -1;
	scheduler_state = atomic_load_explicit(&task->scheduler->state, memory_order_acquire);
	if (scheduler_state == CROUTINE_SCHEDULER_DESTROYING)
		return -1;

	expected = atomic_load_explicit(&task->state, memory_order_acquire);
	for (;;) {
		switch (expected) {
		case CROUTINE_TASK_PENDING:
		case CROUTINE_TASK_WAITING:
			next = CROUTINE_TASK_READY;
			enqueue = 1;
			break;
		case CROUTINE_TASK_PARKING:
			next = CROUTINE_TASK_NOTIFIED;
			enqueue = 0;
			break;
		default:
			return -1;
		}

		if (atomic_compare_exchange_strong_explicit(&task->state, &expected, next, memory_order_acq_rel,
													memory_order_acquire))
			break;
	}

	if (enqueue && croutine_task_enqueue(task) == CROUTINE_TASK_ENQUEUE_ERROR)
		abort();

	return 0;
}

int croutine_await(void) {
	struct croutine_task *task = croutine_current_task;
	enum croutine_task_state expected;

	if (task == NULL)
		return -1;

	expected = CROUTINE_TASK_RUNNING;
	if (!atomic_compare_exchange_strong_explicit(&task->state, &expected, CROUTINE_TASK_PARKING, memory_order_acq_rel,
												 memory_order_acquire))
		return -1;

	return 0;
}

int croutine_cancel_await(void) {
	struct croutine_task *task = croutine_current_task;
	enum croutine_task_state expected;

	if (task == NULL)
		return -1;

	expected = CROUTINE_TASK_PARKING;
	if (!atomic_compare_exchange_strong_explicit(&task->state, &expected, CROUTINE_TASK_RUNNING, memory_order_acq_rel,
												 memory_order_acquire))
		return -1;

	return 0;
}

void croutine_yield(void) {
	struct croutine_task *task = croutine_current_task;
	enum croutine_task_state state;

	if (task == NULL)
		abort();

	state = atomic_load_explicit(&task->state, memory_order_acquire);
	if (state != CROUTINE_TASK_RUNNING && state != CROUTINE_TASK_PARKING && state != CROUTINE_TASK_NOTIFIED)
		abort();

	croutine_task_enter_scheduler();
}

struct croutine_task *croutine_task_current(void) {
	return croutine_current_task;
}
