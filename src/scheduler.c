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

static int croutine_scheduler_state_allows_spawn(enum croutine_scheduler_state state) {
	return state == CROUTINE_SCHEDULER_INIT || state == CROUTINE_SCHEDULER_RUNNING;
}

static int croutine_scheduler_normalize_config(struct croutine_config *out, const croutine_config *config) {
	if (out == NULL || config == NULL || config->main_event_source_config.factory_fn == NULL)
		return -1;

	*out = *config;
	if (out->workers == 0)
		out->workers = CROUTINE_DEFAULT_WORKERS;
	if (out->queue_check_interval == 0)
		out->queue_check_interval = CROUTINE_DEFAULT_QUEUE_CHECK_INTERVAL;
	if (out->steal_batch_max == 0)
		out->steal_batch_max = CROUTINE_DEFAULT_STEAL_BATCH_MAX;
	if (out->stack_size == 0)
		out->stack_size = CROUTINE_DEFAULT_STACK_SIZE;
	if (out->local_queue_capacity == 0)
		out->local_queue_capacity = CROUTINE_DEFAULT_LOCAL_QUEUE_CAPACITY;
	if (out->inbox_queue_capacity == 0)
		out->inbox_queue_capacity = out->local_queue_capacity;
	if (out->main_queue_capacity == 0)
		out->main_queue_capacity = CROUTINE_DEFAULT_MAIN_QUEUE_CAPACITY;
	if (out->workers > UINT32_MAX || out->stack_size < 64)
		return -1;

	return 0;
}

static int croutine_scheduler_validate_source(struct croutine_main_event_source *source) {
	return source != NULL && source->blocking_wait != NULL && source->collect != NULL && source->wake != NULL &&
		   source->suspend != NULL && source->resume != NULL && source->destroy != NULL;
}

static void croutine_scheduler_release_start_gate(struct croutine_scheduler *scheduler) {
	pthread_mutex_lock(&scheduler->start_lock);
	scheduler->start_released = 1;
	pthread_cond_broadcast(&scheduler->start_cond);
	pthread_mutex_unlock(&scheduler->start_lock);
}

static size_t croutine_scheduler_started_workers(const struct croutine_scheduler *scheduler) {
	size_t started = 0;

	for (size_t index = 0; index < scheduler->worker_count; index++) {
		if (scheduler->workers[index].start_state == CROUTINE_WORKER_STARTED)
			started++;
	}
	return started;
}

static int croutine_scheduler_register_task(struct croutine_scheduler *scheduler, struct croutine_task *task) {
	if (scheduler == NULL || task == NULL || task->scheduler != scheduler ||
		!croutine_list_empty(&task->scheduler_node))
		return -1;

	croutine_list_push_back(&scheduler->tasks, &task->scheduler_node);
	return 0;
}

static void croutine_scheduler_unregister_task(struct croutine_scheduler *scheduler, struct croutine_task *task) {
	if (scheduler == NULL || task == NULL || task->scheduler != scheduler || croutine_list_empty(&task->scheduler_node))
		return;

	croutine_list_remove(&task->scheduler_node);
}

void croutine_scheduler_add_finished(struct croutine_scheduler *scheduler, struct croutine_task *task) {
	if (scheduler == NULL || task == NULL)
		return;

	pthread_mutex_lock(&scheduler->tasks_lock);
	if (!croutine_list_empty(&task->state_node))
		abort();
	croutine_list_push_back(&scheduler->finished_tasks, &task->state_node);
	pthread_mutex_unlock(&scheduler->tasks_lock);
}

void croutine_scheduler_reclaim_task(struct croutine_scheduler *scheduler, struct croutine_task *task) {
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

static void croutine_scheduler_destroy_tasks(struct croutine_scheduler *scheduler) {
	while (!croutine_list_empty(&scheduler->tasks)) {
		struct croutine_task *task;

		task = croutine_list_entry(scheduler->tasks.next, struct croutine_task, scheduler_node);
		croutine_scheduler_reclaim_task(scheduler, task);
	}
}

static void croutine_scheduler_destroy_sources(struct croutine_scheduler *scheduler) {
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
	croutine_mpmc_queue_destroy(scheduler->main_queue);
	pthread_mutex_destroy(&scheduler->tasks_lock);
	pthread_cond_destroy(&scheduler->start_cond);
	pthread_mutex_destroy(&scheduler->start_lock);
	pthread_cond_destroy(&scheduler->state_cond);
	pthread_mutex_destroy(&scheduler->state_lock);
	free(scheduler);
}

void croutine_scheduler_wake_one(struct croutine_scheduler *scheduler) {
	size_t worker_count;
	size_t start;

	if (scheduler == NULL || scheduler->workers == NULL ||
		atomic_load_explicit(&scheduler->state, memory_order_acquire) != CROUTINE_SCHEDULER_RUNNING)
		return;

	atomic_thread_fence(memory_order_seq_cst);
	if (atomic_load_explicit(&scheduler->waiting_workers, memory_order_acquire) == 0)
		return;

	worker_count = scheduler->worker_count;
	if (worker_count == 0)
		return;
	start = atomic_fetch_add_explicit(&scheduler->wake_index, 1, memory_order_relaxed) % worker_count;
	for (size_t step = 0; step < worker_count; step++) {
		struct croutine_worker *worker = &scheduler->workers[(start + step) % worker_count];

		if (atomic_load_explicit(&worker->state, memory_order_acquire) != CROUTINE_WORKER_SOURCE_WAITING)
			continue;
		if (atomic_load_explicit(&scheduler->state, memory_order_acquire) == CROUTINE_SCHEDULER_RUNNING)
			croutine_worker_work_wake(worker);
		return;
	}
}

int croutine_scheduler_enqueue_main(struct croutine_scheduler *scheduler, struct croutine_task *task) {
	struct croutine_worker *home;

	if (scheduler == NULL || task == NULL)
		return -1;

	home = atomic_exchange_explicit(&task->worker, NULL, memory_order_relaxed);
	if (croutine_mpmc_queue_push(scheduler->main_queue, task) != 1) {
		atomic_store_explicit(&task->worker, home, memory_order_relaxed);
		return -1;
	}

	croutine_scheduler_wake_one(scheduler);
	return 0;
}

int croutine_scheduler_create(croutine_scheduler **out, const croutine_config *config) {
	struct croutine_scheduler *scheduler;
	struct croutine_config normalized;
	size_t index;

	if (out == NULL)
		return -1;

	*out = NULL;
	if (croutine_scheduler_normalize_config(&normalized, config) != 0)
		return -1;

	scheduler = calloc(1, sizeof(*scheduler));
	if (scheduler == NULL)
		return -1;

	scheduler->worker_count = normalized.workers;
	scheduler->config = normalized;
	atomic_init(&scheduler->state, CROUTINE_SCHEDULER_INIT);
	scheduler->suspended_workers = 0;
	scheduler->suspend_epoch = 0;
	scheduler->start_released = 0;
	atomic_init(&scheduler->scan_index, 0);
	atomic_init(&scheduler->wake_index, 0);
	atomic_init(&scheduler->waiting_workers, 0);
	croutine_list_init(&scheduler->tasks);
	croutine_list_init(&scheduler->finished_tasks);

	if (pthread_mutex_init(&scheduler->state_lock, NULL) != 0)
		goto fail_free;
	if (pthread_cond_init(&scheduler->state_cond, NULL) != 0)
		goto fail_state_lock;
	if (pthread_mutex_init(&scheduler->start_lock, NULL) != 0)
		goto fail_state_cond;
	if (pthread_cond_init(&scheduler->start_cond, NULL) != 0)
		goto fail_start_lock;
	if (pthread_mutex_init(&scheduler->tasks_lock, NULL) != 0)
		goto fail_start_cond;
	scheduler->main_queue = croutine_mpmc_queue_init(normalized.main_queue_capacity);
	if (scheduler->main_queue == NULL)
		goto fail_tasks_lock;

	scheduler->workers = calloc(scheduler->worker_count, sizeof(scheduler->workers[0]));
	if (scheduler->workers == NULL)
		goto fail_main_queue;

	for (index = 0; index < scheduler->worker_count; index++) {
		struct croutine_worker *worker = &scheduler->workers[index];
		struct croutine_main_event_source *source;

		worker->index = index;
		if (croutine_worker_init(worker, scheduler) != 0)
			goto fail_workers;

		source = normalized.main_event_source_config.factory_fn(worker, normalized.main_event_source_config.args);
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
	croutine_mpmc_queue_destroy(scheduler->main_queue);
fail_tasks_lock:
	pthread_mutex_destroy(&scheduler->tasks_lock);
fail_start_cond:
	pthread_cond_destroy(&scheduler->start_cond);
fail_start_lock:
	pthread_mutex_destroy(&scheduler->start_lock);
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
	enum croutine_scheduler_state state;
	int ret = 0;

	if (scheduler == NULL)
		return -1;

	pthread_mutex_lock(&scheduler->state_lock);
	state = atomic_load_explicit(&scheduler->state, memory_order_acquire);
	if (state == CROUTINE_SCHEDULER_RUNNING) {
		pthread_mutex_unlock(&scheduler->state_lock);
		return 0;
	}
	if (state == CROUTINE_SCHEDULER_STOPPED) {
		if (croutine_scheduler_started_workers(scheduler) != scheduler->worker_count) {
			pthread_mutex_unlock(&scheduler->state_lock);
			return -1;
		}
		if (atomic_load_explicit(&scheduler->waiting_workers, memory_order_acquire) != 0)
			abort();
		scheduler->suspended_workers = 0;
		atomic_store_explicit(&scheduler->state, CROUTINE_SCHEDULER_RUNNING, memory_order_release);
		for (size_t index = 0; index < scheduler->worker_count; index++)
			croutine_worker_resume(&scheduler->workers[index]);
		pthread_mutex_unlock(&scheduler->state_lock);
		return 0;
	}
	if (state != CROUTINE_SCHEDULER_INIT) {
		pthread_mutex_unlock(&scheduler->state_lock);
		return -1;
	}

	if (sigfillset(&all_signals) != 0 || pthread_sigmask(SIG_SETMASK, &all_signals, &old_signals) != 0) {
		pthread_mutex_unlock(&scheduler->state_lock);
		return -1;
	}
	atomic_store_explicit(&scheduler->state, CROUTINE_SCHEDULER_RUNNING, memory_order_release);
	for (size_t index = 0; index < scheduler->worker_count; index++) {
		if (croutine_worker_start(&scheduler->workers[index]) != 0) {
			ret = -1;
			break;
		}
	}
	if (pthread_sigmask(SIG_SETMASK, &old_signals, NULL) != 0)
		ret = -1;
	if (ret == 0) {
		croutine_scheduler_release_start_gate(scheduler);
		pthread_mutex_unlock(&scheduler->state_lock);
		return 0;
	}

	atomic_store_explicit(&scheduler->state, CROUTINE_SCHEDULER_DESTROYING, memory_order_release);
	croutine_scheduler_release_start_gate(scheduler);
	for (size_t index = 0; index < scheduler->worker_count; index++) {
		if (scheduler->workers[index].start_state == CROUTINE_WORKER_STARTED)
			croutine_worker_work_wake(&scheduler->workers[index]);
	}
	for (size_t index = 0; index < scheduler->worker_count; index++) {
		if (croutine_worker_join(&scheduler->workers[index]) != 0)
			abort();
	}
	atomic_store_explicit(&scheduler->state, CROUTINE_SCHEDULER_STOPPED, memory_order_release);
	pthread_mutex_unlock(&scheduler->state_lock);
	return -1;
}

int croutine_scheduler_stop(croutine_scheduler *scheduler) {
	enum croutine_scheduler_state state;
	size_t started;

	if (scheduler == NULL)
		return -1;

	pthread_mutex_lock(&scheduler->state_lock);
	state = atomic_load_explicit(&scheduler->state, memory_order_acquire);
	if (state == CROUTINE_SCHEDULER_INIT || state == CROUTINE_SCHEDULER_STOPPED) {
		pthread_mutex_unlock(&scheduler->state_lock);
		return 0;
	}
	if (state != CROUTINE_SCHEDULER_RUNNING) {
		pthread_mutex_unlock(&scheduler->state_lock);
		return -1;
	}

	started = croutine_scheduler_started_workers(scheduler);
	if (started != scheduler->worker_count)
		abort();
	atomic_store_explicit(&scheduler->state, CROUTINE_SCHEDULER_STOPPING, memory_order_release);
	scheduler->suspend_epoch++;
	scheduler->suspended_workers = 0;
	for (size_t index = 0; index < scheduler->worker_count; index++)
		croutine_worker_work_wake(&scheduler->workers[index]);

	while (scheduler->suspended_workers < started)
		pthread_cond_wait(&scheduler->state_cond, &scheduler->state_lock);
	if (atomic_load_explicit(&scheduler->waiting_workers, memory_order_acquire) != 0)
		abort();
	atomic_store_explicit(&scheduler->state, CROUTINE_SCHEDULER_STOPPED, memory_order_release);
	pthread_mutex_unlock(&scheduler->state_lock);
	return 0;
}

int croutine_scheduler_destroy(croutine_scheduler *scheduler) {
	enum croutine_scheduler_state state;

	if (scheduler == NULL)
		return -1;

	pthread_mutex_lock(&scheduler->state_lock);
	state = atomic_load_explicit(&scheduler->state, memory_order_acquire);
	if (state != CROUTINE_SCHEDULER_INIT && state != CROUTINE_SCHEDULER_STOPPED) {
		pthread_mutex_unlock(&scheduler->state_lock);
		return -1;
	}
	atomic_store_explicit(&scheduler->state, CROUTINE_SCHEDULER_DESTROYING, memory_order_release);
	if (state == CROUTINE_SCHEDULER_STOPPED) {
		for (size_t index = 0; index < scheduler->worker_count; index++) {
			if (scheduler->workers[index].start_state == CROUTINE_WORKER_STARTED)
				croutine_worker_resume(&scheduler->workers[index]);
		}
	}
	for (size_t index = 0; index < scheduler->worker_count; index++) {
		if (croutine_worker_join(&scheduler->workers[index]) != 0)
			abort();
	}
	pthread_mutex_unlock(&scheduler->state_lock);

	croutine_scheduler_cleanup(scheduler);
	return 0;
}

croutine_scheduler *croutine_scheduler_current(void) {
	return croutine_sched;
}

int croutine_spawn(croutine_scheduler *scheduler, croutine_task_fn func, void *arg) {
	struct croutine_task *task;
	enum croutine_scheduler_state state;

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

	if (croutine_task_init(task, scheduler, func, arg) != 0) {
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
