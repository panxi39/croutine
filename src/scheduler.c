#define _POSIX_C_SOURCE 200809L

#include "croutine_event.h"
#include "scheduler.h"
#include "stack.h"
#include "task.h"
#include "tls.h"
#include "worker.h"

#include <signal.h>
#include <stdatomic.h>
#include <stdint.h>
#include <stdlib.h>

static int croutine_scheduler_state_allows_spawn(uint32_t state) {
	return state == CROUTINE_SCHEDULER_INIT ||
		   state == CROUTINE_SCHEDULER_RUNNING;
}

static int
croutine_scheduler_validate_source(struct croutine_main_event_source *source) {
	return source != NULL && source->blocking_wait != NULL &&
		   source->wake != NULL && source->suspend != NULL &&
		   source->resume != NULL && source->destroy != NULL;
}

static int croutine_scheduler_has_started_workers(
	const struct croutine_scheduler *scheduler) {
	size_t index;

	for (index = 0; index < scheduler->worker_count; index++) {
		if (scheduler->workers[index].start_state ==
			CROUTINE_WORKER_STARTED)
			return 1;
	}

	return 0;
}

static int
croutine_scheduler_register_task(struct croutine_scheduler *scheduler,
								 struct croutine_task *task) {
	if (scheduler == NULL || task == NULL || task->scheduler != scheduler ||
		!croutine_list_empty(&task->scheduler_node))
		return -1;

	croutine_list_push_back(&scheduler->tasks, &task->scheduler_node);
	return 0;
}

static void
croutine_scheduler_unregister_task(struct croutine_scheduler *scheduler,
								   struct croutine_task *task) {
	if (scheduler == NULL || task == NULL || task->scheduler != scheduler ||
		croutine_list_empty(&task->scheduler_node))
		return;

	croutine_list_remove(&task->scheduler_node);
}

void croutine_scheduler_add_finished(struct croutine_scheduler *scheduler,
									 struct croutine_task *task) {
	if (scheduler == NULL || task == NULL)
		return;

	pthread_mutex_lock(&scheduler->tasks_lock);
	if (!croutine_list_empty(&task->state_node))
		abort();
	croutine_list_push_back(&scheduler->finished_tasks, &task->state_node);
	pthread_mutex_unlock(&scheduler->tasks_lock);
}

void croutine_scheduler_reclaim_task(struct croutine_scheduler *scheduler,
									 struct croutine_task *task) {
	if (scheduler == NULL || task == NULL || task->scheduler != scheduler)
		return;

	pthread_mutex_lock(&scheduler->tasks_lock);
	croutine_scheduler_unregister_task(scheduler, task);
	if (!croutine_list_empty(&task->state_node))
		croutine_list_remove(&task->state_node);
	pthread_mutex_unlock(&scheduler->tasks_lock);

	croutine_stack_free(task->stack);
	free(task);
}

static void
croutine_scheduler_destroy_tasks(struct croutine_scheduler *scheduler) {
	while (!croutine_list_empty(&scheduler->tasks)) {
		struct croutine_task *task;

		task = croutine_list_entry(scheduler->tasks.next, struct croutine_task,
								   scheduler_node);
		croutine_scheduler_reclaim_task(scheduler, task);
	}
}

static void
croutine_scheduler_destroy_sources(struct croutine_scheduler *scheduler) {
	size_t index;

	for (index = 0; index < scheduler->worker_count; index++) {
		struct croutine_main_event_source *source;

		source = scheduler->workers[index].main_event_source;
		if (source != NULL && source->destroy != NULL)
			source->destroy(source);
		scheduler->workers[index].main_event_source = NULL;
	}
}

static void croutine_scheduler_cleanup(struct croutine_scheduler *scheduler) {
	size_t index;

	if (scheduler == NULL)
		return;

	if (scheduler->workers != NULL)
		croutine_scheduler_destroy_sources(scheduler);
	croutine_scheduler_destroy_tasks(scheduler);
	for (index = 0; index < scheduler->worker_count; index++)
		croutine_worker_destroy(&scheduler->workers[index]);
	free(scheduler->workers);
	croutine_queue_destroy(&scheduler->main_queue);
	pthread_mutex_destroy(&scheduler->tasks_lock);
	pthread_mutex_destroy(&scheduler->main_queue_lock);
	pthread_cond_destroy(&scheduler->state_cond);
	pthread_mutex_destroy(&scheduler->state_lock);
	free(scheduler);
}

int croutine_scheduler_enqueue_main(struct croutine_scheduler *scheduler,
									struct croutine_task *task) {
	int ret;

	if (scheduler == NULL || task == NULL)
		return -1;

	pthread_mutex_lock(&scheduler->main_queue_lock);
	ret = croutine_queue_push(&scheduler->main_queue, task);
	pthread_mutex_unlock(&scheduler->main_queue_lock);
	return ret;
}

struct croutine_task *
croutine_scheduler_pop_main(struct croutine_worker *worker) {
	struct croutine_scheduler *scheduler;
	struct croutine_task *task = NULL;
	void *item;

	if (worker == NULL)
		return NULL;

	scheduler = worker->scheduler;
	pthread_mutex_lock(&scheduler->main_queue_lock);
	if (croutine_queue_pop(&scheduler->main_queue, &item) == 0)
		task = item;
	pthread_mutex_unlock(&scheduler->main_queue_lock);

	if (task != NULL)
		task->worker = worker;

	return task;
}

int croutine_scheduler_is_stopping(struct croutine_scheduler *scheduler) {
	uint32_t state;

	if (scheduler == NULL)
		return 1;

	state = atomic_load_explicit(&scheduler->state, memory_order_acquire);
	return state == CROUTINE_SCHEDULER_STOPPING ||
		   state == CROUTINE_SCHEDULER_STOPPED;
}

int croutine_scheduler_is_destroying(struct croutine_scheduler *scheduler) {
	if (scheduler == NULL)
		return 1;

	return atomic_load_explicit(&scheduler->state, memory_order_acquire) ==
		   CROUTINE_SCHEDULER_DESTROYING;
}

void croutine_scheduler_worker_quiescent(struct croutine_worker *worker) {
	struct croutine_scheduler *scheduler;
	uint32_t count;

	if (worker == NULL)
		return;

	scheduler = worker->scheduler;
	pthread_mutex_lock(&scheduler->state_lock);
	if (worker->quiescent_state == CROUTINE_WORKER_ACTIVE) {
		worker->quiescent_state = CROUTINE_WORKER_QUIESCENT;
		count = atomic_fetch_add_explicit(&scheduler->quiescent_workers, 1,
										  memory_order_acq_rel) +
				1;
		if (count >= scheduler->worker_count)
			pthread_cond_broadcast(&scheduler->state_cond);
	}
	pthread_mutex_unlock(&scheduler->state_lock);
}

void croutine_scheduler_wake_all(struct croutine_scheduler *scheduler) {
	size_t index;

	if (scheduler == NULL)
		return;

	for (index = 0; index < scheduler->worker_count; index++) {
		struct croutine_main_event_source *source;

		if (&scheduler->workers[index] == croutine_current_worker)
			continue;

		source = scheduler->workers[index].main_event_source;
		if (source != NULL && source->wake != NULL)
			(void)source->wake(source);
	}
}

void croutine_scheduler_resume_all(struct croutine_scheduler *scheduler) {
	size_t index;

	if (scheduler == NULL)
		return;

	for (index = 0; index < scheduler->worker_count; index++) {
		struct croutine_main_event_source *source;

		source = scheduler->workers[index].main_event_source;
		if (source != NULL && source->resume != NULL)
			(void)source->resume(source);
	}
}

int croutine_scheduler_create(croutine_scheduler **out,
							  const croutine_config *config) {
	struct croutine_scheduler *scheduler;
	struct croutine_config normalized;
	uint32_t main_queue_capacity;
	size_t index;

	if (out == NULL || config == NULL ||
		config->main_event_source_config.factory_fn == NULL)
		return -1;

	*out = NULL;
	normalized = *config;
	if (normalized.workers == 0)
		normalized.workers = 1;
	if (normalized.workers > UINT32_MAX)
		return -1;
	if (normalized.main_queue_quota == 0)
		normalized.main_queue_quota = 1;
	if (normalized.workers > UINT32_MAX / CROUTINE_DEFAULT_READY_QUEUE_CAPACITY)
		return -1;
	main_queue_capacity =
		(uint32_t)normalized.workers * CROUTINE_DEFAULT_READY_QUEUE_CAPACITY;

	scheduler = calloc(1, sizeof(*scheduler));
	if (scheduler == NULL)
		return -1;

	scheduler->worker_count = normalized.workers;
	scheduler->config = normalized;
	atomic_init(&scheduler->state, CROUTINE_SCHEDULER_INIT);
	atomic_init(&scheduler->quiescent_workers, 0);
	croutine_list_init(&scheduler->tasks);
	croutine_list_init(&scheduler->finished_tasks);
	croutine_list_init(&scheduler->event_sources);

	if (pthread_mutex_init(&scheduler->state_lock, NULL) != 0)
		goto fail_free;
	if (pthread_cond_init(&scheduler->state_cond, NULL) != 0)
		goto fail_state_lock;
	if (pthread_mutex_init(&scheduler->main_queue_lock, NULL) != 0)
		goto fail_state_cond;
	if (pthread_mutex_init(&scheduler->tasks_lock, NULL) != 0)
		goto fail_main_queue_lock;
	if (croutine_queue_init(&scheduler->main_queue, main_queue_capacity) != 0)
		goto fail_tasks_lock;

	scheduler->workers =
		calloc(scheduler->worker_count, sizeof(scheduler->workers[0]));
	if (scheduler->workers == NULL)
		goto fail_main_queue;

	for (index = 0; index < scheduler->worker_count; index++) {
		struct croutine_worker *worker = &scheduler->workers[index];
		struct croutine_main_event_source *source;

		if (croutine_worker_init(worker, scheduler) != 0)
			goto fail_workers;

		source = normalized.main_event_source_config.factory_fn(
			worker, normalized.main_event_source_config.args);
		if (!croutine_scheduler_validate_source(source)) {
			if (source != NULL && source->destroy != NULL)
				source->destroy(source);
			goto fail_workers;
		}

		worker->main_event_source = source;
	}

	*out = scheduler;
	return 0;

fail_workers:
	croutine_scheduler_destroy_sources(scheduler);
	for (index = 0; index < scheduler->worker_count; index++)
		croutine_worker_destroy(&scheduler->workers[index]);
	free(scheduler->workers);
fail_main_queue:
	croutine_queue_destroy(&scheduler->main_queue);
fail_tasks_lock:
	pthread_mutex_destroy(&scheduler->tasks_lock);
fail_main_queue_lock:
	pthread_mutex_destroy(&scheduler->main_queue_lock);
fail_state_cond:
	pthread_cond_destroy(&scheduler->state_cond);
fail_state_lock:
	pthread_mutex_destroy(&scheduler->state_lock);
fail_free:
	free(scheduler);
	return -1;
}

int croutine_scheduler_start(croutine_scheduler *scheduler) {
	sigset_t all_signals;
	sigset_t old_signals;
	uint32_t state;
	size_t index;
	int ret;

	if (scheduler == NULL)
		return -1;

	pthread_mutex_lock(&scheduler->state_lock);
	state = atomic_load_explicit(&scheduler->state, memory_order_acquire);
	if (state == CROUTINE_SCHEDULER_RUNNING) {
		pthread_mutex_unlock(&scheduler->state_lock);
		return 0;
	}

	if (state == CROUTINE_SCHEDULER_STOPPED) {
		if (!croutine_scheduler_has_started_workers(scheduler)) {
			pthread_mutex_unlock(&scheduler->state_lock);
			return -1;
		}
		atomic_store_explicit(&scheduler->state, CROUTINE_SCHEDULER_RUNNING,
							  memory_order_release);
		atomic_store_explicit(&scheduler->quiescent_workers, 0,
							  memory_order_release);
		for (index = 0; index < scheduler->worker_count; index++)
			scheduler->workers[index].quiescent_state =
				CROUTINE_WORKER_ACTIVE;
		pthread_mutex_unlock(&scheduler->state_lock);
		croutine_scheduler_resume_all(scheduler);
		return 0;
	}

	if (state != CROUTINE_SCHEDULER_INIT) {
		pthread_mutex_unlock(&scheduler->state_lock);
		return -1;
	}

	atomic_store_explicit(&scheduler->state, CROUTINE_SCHEDULER_RUNNING,
						  memory_order_release);
	pthread_mutex_unlock(&scheduler->state_lock);

	if (sigfillset(&all_signals) != 0) {
		pthread_mutex_lock(&scheduler->state_lock);
		atomic_store_explicit(&scheduler->state, CROUTINE_SCHEDULER_INIT,
							  memory_order_release);
		pthread_mutex_unlock(&scheduler->state_lock);
		return -1;
	}
	if (pthread_sigmask(SIG_SETMASK, &all_signals, &old_signals) != 0) {
		pthread_mutex_lock(&scheduler->state_lock);
		atomic_store_explicit(&scheduler->state, CROUTINE_SCHEDULER_INIT,
							  memory_order_release);
		pthread_mutex_unlock(&scheduler->state_lock);
		return -1;
	}

	ret = 0;
	for (index = 0; index < scheduler->worker_count; index++) {
		if (croutine_worker_start(&scheduler->workers[index]) != 0) {
			ret = -1;
			break;
		}
	}

	if (pthread_sigmask(SIG_SETMASK, &old_signals, NULL) != 0)
		ret = -1;

	if (ret == 0)
		return 0;

	pthread_mutex_lock(&scheduler->state_lock);
	atomic_store_explicit(&scheduler->state, CROUTINE_SCHEDULER_DESTROYING,
						  memory_order_release);
	pthread_mutex_unlock(&scheduler->state_lock);
	croutine_scheduler_wake_all(scheduler);
	croutine_scheduler_resume_all(scheduler);
	for (index = 0; index < scheduler->worker_count; index++)
		(void)croutine_worker_join(&scheduler->workers[index]);
	pthread_mutex_lock(&scheduler->state_lock);
	atomic_store_explicit(&scheduler->state, CROUTINE_SCHEDULER_STOPPED,
						  memory_order_release);
	pthread_mutex_unlock(&scheduler->state_lock);
	return -1;
}

int croutine_scheduler_stop(croutine_scheduler *scheduler) {
	uint32_t state;

	if (scheduler == NULL)
		return -1;

	pthread_mutex_lock(&scheduler->state_lock);
	state = atomic_load_explicit(&scheduler->state, memory_order_acquire);
	if (state == CROUTINE_SCHEDULER_INIT ||
		state == CROUTINE_SCHEDULER_STOPPED) {
		pthread_mutex_unlock(&scheduler->state_lock);
		return 0;
	}
	if (state != CROUTINE_SCHEDULER_RUNNING) {
		pthread_mutex_unlock(&scheduler->state_lock);
		return -1;
	}

	atomic_store_explicit(&scheduler->state, CROUTINE_SCHEDULER_STOPPING,
						  memory_order_release);
	atomic_store_explicit(&scheduler->quiescent_workers, 0,
						  memory_order_release);
	for (size_t index = 0; index < scheduler->worker_count; index++)
		scheduler->workers[index].quiescent_state = CROUTINE_WORKER_ACTIVE;
	pthread_mutex_unlock(&scheduler->state_lock);

	croutine_scheduler_wake_all(scheduler);

	pthread_mutex_lock(&scheduler->state_lock);
	while (atomic_load_explicit(&scheduler->quiescent_workers,
								memory_order_acquire) < scheduler->worker_count)
		pthread_cond_wait(&scheduler->state_cond, &scheduler->state_lock);
	atomic_store_explicit(&scheduler->state, CROUTINE_SCHEDULER_STOPPED,
						  memory_order_release);
	pthread_mutex_unlock(&scheduler->state_lock);

	return 0;
}

int croutine_scheduler_destroy(croutine_scheduler *scheduler) {
	uint32_t state;
	size_t index;

	if (scheduler == NULL)
		return -1;

	pthread_mutex_lock(&scheduler->state_lock);
	state = atomic_load_explicit(&scheduler->state, memory_order_acquire);
	if (state != CROUTINE_SCHEDULER_INIT &&
		state != CROUTINE_SCHEDULER_STOPPED) {
		pthread_mutex_unlock(&scheduler->state_lock);
		return -1;
	}
	atomic_store_explicit(&scheduler->state, CROUTINE_SCHEDULER_DESTROYING,
						  memory_order_release);
	pthread_mutex_unlock(&scheduler->state_lock);

	croutine_scheduler_wake_all(scheduler);
	croutine_scheduler_resume_all(scheduler);
	for (index = 0; index < scheduler->worker_count; index++)
		(void)croutine_worker_join(&scheduler->workers[index]);

	croutine_scheduler_cleanup(scheduler);
	return 0;
}

croutine_scheduler *croutine_scheduler_current(void) {
	return croutine_sched;
}

int croutine_spawn(croutine_scheduler *scheduler, croutine_task_fn func,
				   void *arg) {
	struct croutine_task *task;
	struct croutine_worker *worker;
	uint32_t state;

	if (scheduler == NULL || func == NULL)
		return -1;

	state = atomic_load_explicit(&scheduler->state, memory_order_acquire);
	if (!croutine_scheduler_state_allows_spawn(state))
		return -1;

	task = calloc(1, sizeof(*task));
	if (task == NULL)
		goto fail_alloc;

	pthread_mutex_lock(&scheduler->tasks_lock);
	state = atomic_load_explicit(&scheduler->state, memory_order_acquire);
	if (!croutine_scheduler_state_allows_spawn(state)) {
		pthread_mutex_unlock(&scheduler->tasks_lock);
		goto fail_alloc;
	}

	worker =
		&scheduler->workers[scheduler->next_worker % scheduler->worker_count];
	scheduler->next_worker++;
	if (croutine_task_init(task, worker, func, arg) != 0) {
		pthread_mutex_unlock(&scheduler->tasks_lock);
		goto fail_alloc;
	}
	if (croutine_scheduler_register_task(scheduler, task) != 0) {
		pthread_mutex_unlock(&scheduler->tasks_lock);
		goto fail_alloc;
	}
	pthread_mutex_unlock(&scheduler->tasks_lock);

	if (croutine_task_wake(task) != 0)
		abort();

	return 0;

fail_alloc:
	if (task != NULL)
		croutine_stack_free(task->stack);
	free(task);
	return -1;
}
