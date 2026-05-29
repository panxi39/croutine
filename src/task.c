#include "croutine_event.h"
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
	atomic_store_explicit(&task->state, CROUTINE_TASK_FINISHED,
						  memory_order_release);
	croutine_task_enter_scheduler();
	abort();
}

int croutine_task_init(struct croutine_task *task,
					   struct croutine_scheduler *scheduler,
					   croutine_task_fn func, void *arg) {
	if (task == NULL || scheduler == NULL || func == NULL)
		return -1;

	memset(task, 0, sizeof(*task));
	task->scheduler = scheduler;
	task->worker = NULL;
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

	if (croutine_arch_context_init(&task->context, task->stack->bottom,
								   task->stack->size,
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

	atomic_store_explicit(&task->state, CROUTINE_TASK_RUNNING,
						  memory_order_release);
	croutine_current_task = task;
}

void croutine_task_enter_scheduler(void) {
	struct croutine_task *task = croutine_current_task;

	if (task == NULL || task->worker == NULL || task->worker->schedule == NULL)
		abort();

	croutine_arch_store_and_call(&task->context, task->worker->schedule);
}

void croutine_task_resume(struct croutine_task *task) {
	if (task == NULL || task->worker == NULL)
		abort();

	croutine_current_worker = task->worker;
	croutine_current_task = task;
	atomic_store_explicit(&task->state, CROUTINE_TASK_RUNNING,
						  memory_order_release);

	croutine_arch_resume_and_ret(&task->context);
	abort();
}

enum croutine_task_enqueue_result
croutine_task_enqueue(struct croutine_task *task) {
	struct croutine_worker *worker;
	struct croutine_scheduler *scheduler;

	if (task == NULL || task->scheduler == NULL)
		return CROUTINE_TASK_ENQUEUE_ERROR;

	scheduler = task->scheduler;
	worker = task->worker;
	if (worker != NULL && croutine_worker_enqueue_local(worker, task) == 0)
		return CROUTINE_TASK_ENQUEUE_LOCAL;

	task->worker = NULL;
	if (croutine_scheduler_enqueue_main(scheduler, task) != 0)
		return CROUTINE_TASK_ENQUEUE_ERROR;
	return CROUTINE_TASK_ENQUEUE_MAIN;
}

int croutine_task_wake(struct croutine_task *task) {
	struct croutine_worker *worker;
	struct croutine_scheduler *scheduler;
	enum croutine_task_state expected;
	enum croutine_task_enqueue_result enqueue_result;
	size_t index;

	if (task == NULL || task->scheduler == NULL)
		return -1;

	scheduler = task->scheduler;
	worker = task->worker;
	expected = CROUTINE_TASK_PENDING;
	if (!atomic_compare_exchange_strong_explicit(
			&task->state, &expected, CROUTINE_TASK_READY, memory_order_acq_rel,
			memory_order_acquire)) {
		expected = CROUTINE_TASK_WAITING;
		if (!atomic_compare_exchange_strong_explicit(
				&task->state, &expected, CROUTINE_TASK_READY,
				memory_order_acq_rel, memory_order_acquire))
			return -1;
	}

	enqueue_result = croutine_task_enqueue(task);
	if (enqueue_result == CROUTINE_TASK_ENQUEUE_ERROR)
		abort();

	if (enqueue_result == CROUTINE_TASK_ENQUEUE_LOCAL) {
		if (worker != croutine_current_worker)
			croutine_worker_wake(worker);
		return 0;
	}

	for (index = 0; index < scheduler->worker_count; index++)
		croutine_worker_wake(&scheduler->workers[index]);
	return 0;
}

void croutine_task_wait(void) {
	struct croutine_task *task = croutine_current_task;

	if (task == NULL)
		abort();

	atomic_store_explicit(&task->state, CROUTINE_TASK_WAITING,
						  memory_order_release);
	croutine_task_enter_scheduler();
}

void croutine_yield(void) {
	struct croutine_task *task = croutine_current_task;

	if (task == NULL)
		abort();

	atomic_store_explicit(&task->state, CROUTINE_TASK_READY,
						  memory_order_release);
	croutine_task_enter_scheduler();
}

struct croutine_task *croutine_task_current(void) {
	return croutine_current_task;
}
