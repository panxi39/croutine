#include "croutine_event.h"
#include "scheduler.h"
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

static void croutine_worker_reclaim_dead(struct croutine_worker *worker) {
	struct croutine_list_head *pos;
	struct croutine_list_head *tmp;

	croutine_list_for_each_safe(pos, tmp, &worker->dead_tasks) {
		struct croutine_task *task;

		task = croutine_list_entry(pos, struct croutine_task, state_node);
		croutine_list_remove(&task->state_node);
		croutine_scheduler_reclaim_task(worker->scheduler, task);
	}
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

static void *croutine_worker_main(void *arg) {
	struct croutine_worker *worker = arg;

	croutine_sched = worker->scheduler;
	croutine_current_worker = worker;
	croutine_current_task = NULL;

	croutine_worker_schedule();
	abort();
}

int croutine_worker_init(struct croutine_worker *worker,
						 struct croutine_scheduler *scheduler) {
	if (worker == NULL || scheduler == NULL)
		return -1;

	worker->scheduler = scheduler;
	worker->start_state = CROUTINE_WORKER_STOPPED;
	atomic_init(&worker->state, CROUTINE_WORKER_RUNNING);
	worker->schedule = croutine_worker_schedule;
	if (pthread_mutex_init(&worker->local_queue_lock, NULL) != 0)
		return -1;
	worker->local_queue_lock_initialized = 1;
	if (croutine_queue_init(&worker->local_queue,
							(uint32_t)scheduler->config.local_queue_capacity) !=
		0) {
		pthread_mutex_destroy(&worker->local_queue_lock);
		worker->local_queue_lock_initialized = 0;
		return -1;
	}
	croutine_list_init(&worker->dead_tasks);
	worker->main_event_source = NULL;
	worker->local_turns = 0;
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
	croutine_queue_destroy(&worker->local_queue);
	if (worker->local_queue_lock_initialized) {
		pthread_mutex_destroy(&worker->local_queue_lock);
		worker->local_queue_lock_initialized = 0;
	}
}

int croutine_worker_enqueue_local(struct croutine_worker *worker,
								  struct croutine_task *task) {
	int ret;

	if (worker == NULL || task == NULL)
		return -1;

	pthread_mutex_lock(&worker->local_queue_lock);
	ret = croutine_queue_push(&worker->local_queue, task);
	pthread_mutex_unlock(&worker->local_queue_lock);
	return ret;
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
			expected = CROUTINE_WORKER_RUNNING;
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
			expected = CROUTINE_WORKER_RUNNING;
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

void croutine_worker_schedule(void) {
	struct croutine_worker *worker = croutine_current_worker;
	struct croutine_task *task;
	struct croutine_main_event_source *source;
	void *item;
	enum croutine_task_state task_state;
	enum croutine_task_enqueue_result enqueue_result;
	enum croutine_worker_state worker_state;
	enum croutine_worker_state expected_worker_state;
	enum croutine_scheduler_state scheduler_state;
	enum croutine_main_event_wait_result wait_result;
	size_t quota;

	if (worker == NULL)
		abort();

	croutine_worker_reclaim_dead(worker);

	task = croutine_current_task;
	if (task != NULL) {
		croutine_current_task = NULL;
		task_state = atomic_load_explicit(&task->state, memory_order_acquire);

		if (task_state == CROUTINE_TASK_FINISHED) {
			switch (task->result_policy) {
			case CROUTINE_TASK_RESULT_COLLECT:
				croutine_scheduler_add_finished(worker->scheduler, task);
				break;
			case CROUTINE_TASK_RESULT_DETACHED:
				if (!croutine_list_empty(&task->state_node))
					abort();
				croutine_list_push_back(&worker->dead_tasks, &task->state_node);
				break;
			default:
				abort();
			}
		} else if (task_state == CROUTINE_TASK_READY) {
			enqueue_result = croutine_task_enqueue(task);
			if (enqueue_result == CROUTINE_TASK_ENQUEUE_ERROR)
				abort();
		}
	}

	for (;;) {
		task = NULL;
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
			break;
		default:
			abort();
		}

		if (worker->scheduler == NULL)
			scheduler_state = CROUTINE_SCHEDULER_DESTROYING;
		else
			scheduler_state = atomic_load_explicit(&worker->scheduler->state,
												   memory_order_acquire);

		if (scheduler_state == CROUTINE_SCHEDULER_DESTROYING) {
			croutine_worker_request_exit(worker);
			continue;
		}
		if (scheduler_state == CROUTINE_SCHEDULER_STOPPING ||
			scheduler_state == CROUTINE_SCHEDULER_STOPPED) {
			croutine_worker_request_suspend(worker);
			continue;
		}

		source = worker->main_event_source;
		if (source != NULL && source->collect != NULL)
			source->collect(source);
		if (atomic_load_explicit(&worker->state, memory_order_acquire) !=
			CROUTINE_WORKER_RUNNING)
			continue;

		quota = worker->scheduler->config.main_queue_quota;
		if (quota == 0)
			quota = 1;

		if (worker->local_turns >= quota) {
			task = croutine_scheduler_pop_main(worker);
			worker->local_turns = 0;
		}

		if (task == NULL) {
			pthread_mutex_lock(&worker->local_queue_lock);
			if (croutine_queue_pop(&worker->local_queue, &item) == 0)
				task = item;
			pthread_mutex_unlock(&worker->local_queue_lock);
			if (task != NULL)
				worker->local_turns++;
		}

		if (task == NULL) {
			task = croutine_scheduler_pop_main(worker);
			if (task != NULL)
				worker->local_turns = 0;
		}

		if (task != NULL)
			croutine_task_resume(task);

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
