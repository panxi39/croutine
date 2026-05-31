#include "croutine_event.h"
#include "scheduler.h"
#include "stack.h"
#include "task.h"
#include "tls.h"
#include "worker.h"

#include <stdatomic.h>
#include <stdlib.h>

static void croutine_worker_source_wake(struct croutine_worker *worker) {
	struct croutine_main_event_source *source;

	if (worker == NULL)
		return;

	source = worker->main_event_source;
	if (source != NULL && source->wake != NULL)
		(void)source->wake(source);
}

static void croutine_worker_report_suspended(struct croutine_worker *worker) {
	struct croutine_scheduler *scheduler;
	enum croutine_scheduler_state state;

	if (worker == NULL || worker->scheduler == NULL)
		return;

	scheduler = worker->scheduler;
	pthread_mutex_lock(&scheduler->state_lock);
	state = atomic_load_explicit(&scheduler->state, memory_order_acquire);
	if ((state == CROUTINE_SCHEDULER_STOPPING ||
		 state == CROUTINE_SCHEDULER_STOPPED) &&
		worker->reported_suspend_epoch != scheduler->suspend_epoch) {
		worker->reported_suspend_epoch = scheduler->suspend_epoch;
		scheduler->suspended_workers++;
		if (scheduler->suspended_workers >= scheduler->worker_count)
			pthread_cond_broadcast(&scheduler->state_cond);
	}
	pthread_mutex_unlock(&scheduler->state_lock);
}

static void croutine_worker_exit_thread(struct croutine_worker *worker) {
	if (worker != NULL)
		atomic_store_explicit(&worker->state, CROUTINE_WORKER_EXITED,
							  memory_order_release);

	croutine_current_task = NULL;
	croutine_current_worker = NULL;
	croutine_sched = NULL;
	pthread_exit(NULL);
}

static void croutine_worker_schedule_entry(void) {
	struct croutine_worker *worker = croutine_current_worker;

	if (worker == NULL || worker->scheduler_stack == NULL)
		abort();

	if (croutine_arch_context_init(
			&worker->scheduler_context, worker->scheduler_stack->bottom,
			worker->scheduler_stack->size, croutine_worker_schedule) != 0)
		abort();

	croutine_arch_resume_and_ret(&worker->scheduler_context);
	abort();
}

static void *croutine_worker_main(void *arg) {
	struct croutine_worker *worker = arg;

	croutine_sched = worker->scheduler;
	croutine_current_worker = worker;
	croutine_current_task = NULL;

	croutine_worker_schedule_entry();
	abort();
}

int croutine_worker_init(struct croutine_worker *worker,
						 struct croutine_scheduler *scheduler) {
	if (worker == NULL || scheduler == NULL)
		return -1;

	worker->scheduler = scheduler;
	worker->start_state = CROUTINE_WORKER_STOPPED;
	atomic_init(&worker->state, CROUTINE_WORKER_RUNNING);
	worker->schedule = croutine_worker_schedule_entry;
	if (croutine_queue_init(&worker->local_queue,
							(uint32_t)scheduler->config.local_queue_capacity) !=
		0)
		return -1;
	worker->scheduler_stack =
		croutine_stack_alloc(scheduler->config.stack_size);
	if (worker->scheduler_stack == CROUTINE_STACK_ERROR) {
		worker->scheduler_stack = NULL;
		croutine_queue_destroy(&worker->local_queue);
		return -1;
	}
	worker->local_turns = 0;
	worker->main_event_source = NULL;
	worker->reported_suspend_epoch = 0;
	return 0;
}

int croutine_worker_start(struct croutine_worker *worker) {
	if (worker == NULL || worker->start_state == CROUTINE_WORKER_STARTED)
		return -1;

	atomic_store_explicit(&worker->state, CROUTINE_WORKER_RUNNING,
						  memory_order_release);
	if (pthread_create(&worker->tid, NULL, croutine_worker_main, worker) != 0)
		return -1;

	worker->start_state = CROUTINE_WORKER_STARTED;
	return 0;
}

int croutine_worker_join(struct croutine_worker *worker) {
	if (worker == NULL || worker->start_state == CROUTINE_WORKER_STOPPED)
		return 0;

	if (pthread_join(worker->tid, NULL) != 0)
		return -1;

	worker->start_state = CROUTINE_WORKER_STOPPED;
	return 0;
}

void croutine_worker_destroy(struct croutine_worker *worker) {
	if (worker == NULL)
		return;

	worker->scheduler = NULL;
	worker->main_event_source = NULL;
	croutine_stack_free(worker->scheduler_stack);
	worker->scheduler_stack = NULL;
	croutine_queue_destroy(&worker->local_queue);
}

int croutine_worker_enqueue_local(struct croutine_worker *worker,
								  struct croutine_task *task) {
	if (worker == NULL || task == NULL)
		return -1;

	if (croutine_queue_push_owner(&worker->local_queue, task) != 0)
		return -1;
	task->worker = worker;
	return 0;
}

void croutine_worker_request_suspend(struct croutine_worker *worker) {
	enum croutine_worker_state state;
	enum croutine_worker_state expected;

	if (worker == NULL)
		return;

	for (;;) {
		state = atomic_load_explicit(&worker->state, memory_order_acquire);
		switch (state) {
		case CROUTINE_WORKER_RUNNING:
		case CROUTINE_WORKER_SEARCHING:
			expected = state;
			if (atomic_compare_exchange_weak_explicit(
					&worker->state, &expected, CROUTINE_WORKER_SUSPENDING,
					memory_order_acq_rel, memory_order_acquire))
				return;
			break;
		case CROUTINE_WORKER_SOURCE_WAITING:
			expected = CROUTINE_WORKER_SOURCE_WAITING;
			if (atomic_compare_exchange_weak_explicit(
					&worker->state, &expected, CROUTINE_WORKER_SUSPENDING,
					memory_order_acq_rel, memory_order_acquire)) {
				croutine_worker_source_wake(worker);
				return;
			}
			break;
		case CROUTINE_WORKER_SUSPENDED:
			croutine_worker_report_suspended(worker);
			return;
		case CROUTINE_WORKER_SUSPENDING:
		case CROUTINE_WORKER_EXITING:
		case CROUTINE_WORKER_EXITED:
			return;
		default:
			abort();
		}
	}
}

void croutine_worker_wake(struct croutine_worker *worker) {
	enum croutine_worker_state state;
	enum croutine_worker_state expected;

	if (worker == NULL)
		return;

	for (;;) {
		state = atomic_load_explicit(&worker->state, memory_order_acquire);
		switch (state) {
		case CROUTINE_WORKER_SOURCE_WAITING:
		case CROUTINE_WORKER_SUSPENDING:
		case CROUTINE_WORKER_SUSPENDED:
			expected = state;
			if (atomic_compare_exchange_weak_explicit(
					&worker->state, &expected, CROUTINE_WORKER_RUNNING,
					memory_order_acq_rel, memory_order_acquire)) {
				croutine_worker_source_wake(worker);
				return;
			}
			break;
		case CROUTINE_WORKER_RUNNING:
		case CROUTINE_WORKER_SEARCHING:
		case CROUTINE_WORKER_EXITING:
		case CROUTINE_WORKER_EXITED:
			return;
		default:
			abort();
		}
	}
}

void croutine_worker_request_exit(struct croutine_worker *worker) {
	enum croutine_worker_state state;
	enum croutine_worker_state expected;

	if (worker == NULL)
		return;

	for (;;) {
		state = atomic_load_explicit(&worker->state, memory_order_acquire);
		switch (state) {
		case CROUTINE_WORKER_RUNNING:
		case CROUTINE_WORKER_SEARCHING:
			expected = state;
			if (atomic_compare_exchange_weak_explicit(
					&worker->state, &expected, CROUTINE_WORKER_EXITING,
					memory_order_acq_rel, memory_order_acquire))
				return;
			break;
		case CROUTINE_WORKER_SOURCE_WAITING:
		case CROUTINE_WORKER_SUSPENDING:
		case CROUTINE_WORKER_SUSPENDED:
			expected = state;
			if (atomic_compare_exchange_weak_explicit(
					&worker->state, &expected, CROUTINE_WORKER_EXITING,
					memory_order_acq_rel, memory_order_acquire)) {
				croutine_worker_source_wake(worker);
				return;
			}
			break;
		case CROUTINE_WORKER_EXITING:
		case CROUTINE_WORKER_EXITED:
			return;
		default:
			abort();
		}
	}
}

static void croutine_worker_process_current(struct croutine_worker *worker) {
	struct croutine_task *task = croutine_current_task;
	enum croutine_task_state task_state;
	enum croutine_task_enqueue_result enqueue_result;

	if (task == NULL)
		return;

	croutine_current_task = NULL;
	task_state = atomic_load_explicit(&task->state, memory_order_acquire);
	switch (task_state) {
	case CROUTINE_TASK_FINISHED:
		switch (task->result_policy) {
		case CROUTINE_TASK_RESULT_COLLECT:
			croutine_scheduler_add_finished(worker->scheduler, task);
			break;
		case CROUTINE_TASK_RESULT_DETACHED:
			if (!croutine_list_empty(&task->state_node))
				abort();
			croutine_scheduler_reclaim_task(worker->scheduler, task);
			break;
		default:
			abort();
		}
		break;
	case CROUTINE_TASK_YIELDING:
		atomic_store_explicit(&task->state, CROUTINE_TASK_READY,
							  memory_order_release);
		atomic_store_explicit(&task->schedulable, 1, memory_order_release);
		enqueue_result = croutine_task_enqueue(task);
		if (enqueue_result == CROUTINE_TASK_ENQUEUE_ERROR)
			abort();
		break;
	case CROUTINE_TASK_WAITING:
	case CROUTINE_TASK_READY:
		atomic_store_explicit(&task->schedulable, 1, memory_order_release);
		break;
	case CROUTINE_TASK_PENDING:
	case CROUTINE_TASK_RUNNING:
	default:
		abort();
	}
}

static struct croutine_task *
croutine_worker_pop_local(struct croutine_worker *worker) {
	struct croutine_task *task = NULL;
	void *item;

	if (worker == NULL)
		return NULL;

	if (croutine_queue_pop(&worker->local_queue, &item) == 0) {
		task = item;
		if (atomic_load_explicit(&task->schedulable, memory_order_acquire) == 0) {
			if (croutine_queue_push_owner(&worker->local_queue, task) != 0)
				abort();
			return NULL;
		}
		task->worker = worker;
	}
	return task;
}

static int croutine_worker_enter_searching(struct croutine_worker *worker) {
	enum croutine_worker_state expected;

	if (worker == NULL || worker->scheduler == NULL)
		return 0;

	expected = CROUTINE_WORKER_RUNNING;
	if (!atomic_compare_exchange_strong_explicit(
			&worker->state, &expected, CROUTINE_WORKER_SEARCHING,
			memory_order_acq_rel, memory_order_acquire))
		return 0;

	atomic_fetch_add_explicit(&worker->scheduler->searching_workers, 1,
							  memory_order_acq_rel);
	return 1;
}

static void croutine_worker_leave_searching(struct croutine_worker *worker) {
	enum croutine_worker_state expected;

	if (worker == NULL || worker->scheduler == NULL)
		return;

	atomic_fetch_sub_explicit(&worker->scheduler->searching_workers, 1,
							  memory_order_acq_rel);
	expected = CROUTINE_WORKER_SEARCHING;
	(void)atomic_compare_exchange_strong_explicit(
		&worker->state, &expected, CROUTINE_WORKER_RUNNING,
		memory_order_acq_rel, memory_order_acquire);
}

static struct croutine_task *
croutine_worker_steal(struct croutine_worker *worker) {
	struct croutine_scheduler *scheduler;
	size_t worker_count;
	size_t start;

	if (worker == NULL || worker->scheduler == NULL)
		return NULL;

	scheduler = worker->scheduler;
	worker_count = scheduler->worker_count;
	if (worker_count <= 1)
		return NULL;

	start = atomic_fetch_add_explicit(&scheduler->steal_index, 1,
									  memory_order_relaxed) %
			worker_count;
	for (size_t step = 0; step < worker_count; step++) {
		struct croutine_worker *victim;
		struct croutine_task *task;
		void *item = NULL;
		size_t index = (start + step) % worker_count;

		victim = &scheduler->workers[index];
		if (victim == worker)
			continue;
		if (croutine_queue_steal_half(&victim->local_queue,
									  &worker->local_queue, &item) == 0)
			continue;
		task = item;
		if (task == NULL)
			abort();
		if (atomic_load_explicit(&task->schedulable, memory_order_acquire) == 0) {
			if (croutine_scheduler_enqueue_main(scheduler, task) != 0)
				abort();
			return NULL;
		}
		task->worker = worker;
		return task;
	}

	return NULL;
}

static int croutine_worker_scheduler_running(struct croutine_worker *worker) {
	enum croutine_scheduler_state state;

	if (worker == NULL || worker->scheduler == NULL)
		return 0;

	state =
		atomic_load_explicit(&worker->scheduler->state, memory_order_acquire);
	return state == CROUTINE_SCHEDULER_RUNNING;
}

static struct croutine_task *
croutine_worker_next_task(struct croutine_worker *worker) {
	struct croutine_task *task = NULL;
	size_t quota;

	quota = worker->scheduler->config.main_queue_quota;
	if (quota == 0)
		quota = 1;

	if (worker->local_turns >= quota) {
		task = croutine_scheduler_pop_main(worker);
		worker->local_turns = 0;
	}

	if (task == NULL) {
		task = croutine_worker_pop_local(worker);
		if (task != NULL)
			worker->local_turns++;
	}

	if (task != NULL)
		return task;

	if (!croutine_worker_enter_searching(worker))
		return NULL;
	task = croutine_worker_steal(worker);
	if (task == NULL)
		task = croutine_scheduler_pop_main(worker);
	if (task != NULL)
		worker->local_turns = 0;
	croutine_worker_leave_searching(worker);
	return task;
}

void croutine_worker_schedule(void) {
	struct croutine_worker *worker = croutine_current_worker;
	struct croutine_task *task;
	struct croutine_main_event_source *source;
	enum croutine_worker_state worker_state;
	enum croutine_worker_state expected_worker_state;
	enum croutine_main_event_wait_result wait_result;

	if (worker == NULL)
		abort();

	for (;;) {
		croutine_worker_process_current(worker);

		worker_state =
			atomic_load_explicit(&worker->state, memory_order_acquire);
		switch (worker_state) {
		case CROUTINE_WORKER_EXITING:
		case CROUTINE_WORKER_EXITED:
			croutine_worker_exit_thread(worker);
			break;
		case CROUTINE_WORKER_SUSPENDING:
			expected_worker_state = CROUTINE_WORKER_SUSPENDING;
			if (!atomic_compare_exchange_strong_explicit(
					&worker->state, &expected_worker_state,
					CROUTINE_WORKER_SUSPENDED, memory_order_acq_rel,
					memory_order_acquire))
				continue;
			croutine_worker_report_suspended(worker);
			source = worker->main_event_source;
			if (source != NULL && source->suspend != NULL)
				source->suspend(source);
			continue;
		case CROUTINE_WORKER_SUSPENDED:
			croutine_worker_report_suspended(worker);
			source = worker->main_event_source;
			if (source != NULL && source->suspend != NULL)
				source->suspend(source);
			continue;
		case CROUTINE_WORKER_SOURCE_WAITING:
			expected_worker_state = CROUTINE_WORKER_SOURCE_WAITING;
			(void)atomic_compare_exchange_strong_explicit(
				&worker->state, &expected_worker_state, CROUTINE_WORKER_RUNNING,
				memory_order_acq_rel, memory_order_acquire);
			continue;
		case CROUTINE_WORKER_RUNNING:
		case CROUTINE_WORKER_SEARCHING:
			break;
		default:
			abort();
		}

		if (!croutine_worker_scheduler_running(worker)) {
			croutine_worker_request_suspend(worker);
			continue;
		}

		source = worker->main_event_source;
		if (source != NULL && source->collect != NULL)
			source->collect(source);
		if (!croutine_worker_scheduler_running(worker))
			continue;

		task = croutine_worker_next_task(worker);
		if (task != NULL) {
			if (atomic_load_explicit(&worker->state, memory_order_acquire) ==
					CROUTINE_WORKER_RUNNING &&
				croutine_worker_scheduler_running(worker))
				croutine_task_resume(task);
			if (croutine_task_enqueue(task) == CROUTINE_TASK_ENQUEUE_ERROR)
				abort();
			continue;
		}

		if (atomic_load_explicit(&worker->state, memory_order_acquire) !=
			CROUTINE_WORKER_RUNNING)
			continue;

		expected_worker_state = CROUTINE_WORKER_RUNNING;
		if (!atomic_compare_exchange_strong_explicit(
				&worker->state, &expected_worker_state,
				CROUTINE_WORKER_SOURCE_WAITING, memory_order_acq_rel,
				memory_order_acquire))
			continue;

		source = worker->main_event_source;
		if (source != NULL && source->collect != NULL)
			source->collect(source);
		if (atomic_load_explicit(&worker->state, memory_order_acquire) !=
			CROUTINE_WORKER_SOURCE_WAITING)
			continue;

		task = croutine_worker_pop_local(worker);
		if (task == NULL)
			task = croutine_scheduler_pop_main(worker);
		if (task != NULL) {
			expected_worker_state = CROUTINE_WORKER_SOURCE_WAITING;
			if (atomic_compare_exchange_strong_explicit(
					&worker->state, &expected_worker_state,
					CROUTINE_WORKER_RUNNING, memory_order_acq_rel,
					memory_order_acquire))
				croutine_task_resume(task);
			if (croutine_task_enqueue(task) == CROUTINE_TASK_ENQUEUE_ERROR)
				abort();
			continue;
		}

		if (atomic_load_explicit(&worker->state, memory_order_acquire) !=
			CROUTINE_WORKER_SOURCE_WAITING)
			continue;

		source = worker->main_event_source;
		if (source == NULL || source->blocking_wait == NULL) {
			expected_worker_state = CROUTINE_WORKER_SOURCE_WAITING;
			(void)atomic_compare_exchange_strong_explicit(
				&worker->state, &expected_worker_state,
				CROUTINE_WORKER_SUSPENDING, memory_order_acq_rel,
				memory_order_acquire);
			continue;
		}

		wait_result = source->blocking_wait(source);
		switch (wait_result) {
		case CROUTINE_MAIN_EVENT_WAIT_DONE:
			expected_worker_state = CROUTINE_WORKER_SOURCE_WAITING;
			(void)atomic_compare_exchange_strong_explicit(
				&worker->state, &expected_worker_state, CROUTINE_WORKER_RUNNING,
				memory_order_acq_rel, memory_order_acquire);
			break;
		case CROUTINE_MAIN_EVENT_WAIT_EMPTY:
			expected_worker_state = CROUTINE_WORKER_SOURCE_WAITING;
			(void)atomic_compare_exchange_strong_explicit(
				&worker->state, &expected_worker_state,
				CROUTINE_WORKER_SUSPENDING, memory_order_acq_rel,
				memory_order_acquire);
			break;
		case CROUTINE_MAIN_EVENT_WAIT_ERROR:
		default:
			expected_worker_state = CROUTINE_WORKER_SOURCE_WAITING;
			(void)atomic_compare_exchange_strong_explicit(
				&worker->state, &expected_worker_state, CROUTINE_WORKER_RUNNING,
				memory_order_acq_rel, memory_order_acquire);
			abort();
		}
	}
}
