#include "croutine_event.h"
#include "scheduler.h"
#include "stack.h"
#include "task.h"
#include "tls.h"
#include "worker.h"

#include <stdatomic.h>
#include <stdlib.h>

void croutine_worker_work_wake(struct croutine_worker *worker) {
	if (worker == NULL || worker->main_event_source == NULL ||
		worker->main_event_source->wake(worker->main_event_source) != 0)
		abort();
}

void croutine_worker_resume(struct croutine_worker *worker) {
	if (worker == NULL || worker->main_event_source == NULL)
		abort();
	worker->main_event_source->resume(worker->main_event_source);
}

static void croutine_worker_schedule_entry(void) {
	struct croutine_worker *worker = croutine_current_worker;

	if (worker == NULL || worker->scheduler_stack == NULL)
		abort();

	if (croutine_arch_context_init(&worker->scheduler_context, worker->scheduler_stack->bottom,
								   worker->scheduler_stack->size, croutine_worker_schedule) != 0)
		abort();

	croutine_arch_resume_and_ret(&worker->scheduler_context);
	abort();
}

static void *croutine_worker_main(void *arg) {
	struct croutine_worker *worker = arg;
	struct croutine_scheduler *scheduler = worker->scheduler;

	pthread_mutex_lock(&scheduler->start_lock);
	while (!scheduler->start_released)
		(void)pthread_cond_wait(&scheduler->start_cond, &scheduler->start_lock);
	pthread_mutex_unlock(&scheduler->start_lock);

	croutine_sched = scheduler;
	croutine_current_worker = worker;
	croutine_current_task = NULL;
	croutine_worker_schedule_entry();
	abort();
}

int croutine_worker_init(struct croutine_worker *worker, struct croutine_scheduler *scheduler) {
	if (worker == NULL || scheduler == NULL)
		return -1;

	worker->scheduler = scheduler;
	worker->start_state = CROUTINE_WORKER_STOPPED;
	atomic_init(&worker->state, CROUTINE_WORKER_RUNNING);
	worker->schedule = croutine_worker_schedule_entry;
	worker->local_queue = croutine_cldeque_init(scheduler->config.local_queue_capacity);
	if (worker->local_queue == NULL)
		return -1;
	worker->inbox_queue = croutine_mpmc_queue_init(scheduler->config.inbox_queue_capacity);
	if (worker->inbox_queue == NULL) {
		croutine_cldeque_destroy(worker->local_queue);
		worker->local_queue = NULL;
		return -1;
	}
	worker->scheduler_stack = croutine_stack_alloc(scheduler->config.stack_size);
	if (worker->scheduler_stack == CROUTINE_STACK_ERROR) {
		worker->scheduler_stack = NULL;
		croutine_mpmc_queue_destroy(worker->inbox_queue);
		worker->inbox_queue = NULL;
		croutine_cldeque_destroy(worker->local_queue);
		worker->local_queue = NULL;
		return -1;
	}
	worker->queue_check_turns = 0;
	worker->steal_seed = (uint32_t)(worker->index + 1) * UINT32_C(0x9e3779b9);
	worker->main_event_source = NULL;
	worker->reported_suspend_epoch = 0;
	return 0;
}

int croutine_worker_start(struct croutine_worker *worker) {
	if (worker == NULL || worker->start_state == CROUTINE_WORKER_STARTED)
		return -1;

	atomic_store_explicit(&worker->state, CROUTINE_WORKER_RUNNING, memory_order_release);
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
	croutine_mpmc_queue_destroy(worker->inbox_queue);
	worker->inbox_queue = NULL;
	croutine_cldeque_destroy(worker->local_queue);
	worker->local_queue = NULL;
}

int croutine_worker_enqueue_local(struct croutine_worker *worker, struct croutine_task *task) {
	if (worker == NULL || task == NULL ||
		atomic_load_explicit(&task->state, memory_order_acquire) != CROUTINE_TASK_READY)
		return -1;

	atomic_store_explicit(&task->worker, worker, memory_order_relaxed);
	if (croutine_cldeque_push(worker->local_queue, task) != 1)
		return -1;
	return 0;
}

int croutine_worker_enqueue_inbox(struct croutine_worker *worker, struct croutine_task *task) {
	if (worker == NULL || worker->scheduler == NULL || task == NULL ||
		atomic_load_explicit(&task->state, memory_order_acquire) != CROUTINE_TASK_READY)
		return -1;
	if (croutine_mpmc_queue_push(worker->inbox_queue, task) != 1)
		return -1;

	atomic_thread_fence(memory_order_seq_cst);
	if (atomic_load_explicit(&worker->scheduler->state, memory_order_acquire) == CROUTINE_SCHEDULER_RUNNING &&
		atomic_load_explicit(&worker->state, memory_order_acquire) == CROUTINE_WORKER_SOURCE_WAITING)
		croutine_worker_work_wake(worker);
	return 0;
}

static void croutine_worker_suspend(struct croutine_worker *worker) {
	struct croutine_scheduler *scheduler = worker->scheduler;
	struct croutine_main_event_source *source = worker->main_event_source;
	enum croutine_scheduler_state state;

	if (atomic_load_explicit(&worker->state, memory_order_acquire) != CROUTINE_WORKER_RUNNING)
		abort();

	atomic_store_explicit(&worker->state, CROUTINE_WORKER_SUSPENDING, memory_order_release);
	atomic_store_explicit(&worker->state, CROUTINE_WORKER_SUSPENDED, memory_order_release);
	for (;;) {
		pthread_mutex_lock(&scheduler->state_lock);
		state = atomic_load_explicit(&scheduler->state, memory_order_acquire);
		if (state == CROUTINE_SCHEDULER_STOPPING && worker->reported_suspend_epoch != scheduler->suspend_epoch) {
			worker->reported_suspend_epoch = scheduler->suspend_epoch;
			scheduler->suspended_workers++;
			if (scheduler->suspended_workers >= scheduler->worker_count)
				pthread_cond_broadcast(&scheduler->state_cond);
		}
		pthread_mutex_unlock(&scheduler->state_lock);

		source->suspend(source);
		state = atomic_load_explicit(&scheduler->state, memory_order_acquire);
		switch (state) {
		case CROUTINE_SCHEDULER_RUNNING:
			atomic_store_explicit(&worker->state, CROUTINE_WORKER_RUNNING, memory_order_release);
			return;
		case CROUTINE_SCHEDULER_STOPPING:
		case CROUTINE_SCHEDULER_STOPPED:
			break;
		case CROUTINE_SCHEDULER_DESTROYING:
			atomic_store_explicit(&worker->state, CROUTINE_WORKER_EXITING, memory_order_release);
			return;
		case CROUTINE_SCHEDULER_INIT:
		default:
			abort();
		}
	}
}

static void croutine_worker_process_current(struct croutine_worker *worker) {
	struct croutine_task *task = croutine_current_task;
	enum croutine_task_state state;
	enum croutine_task_state expected;
	int enqueue = 0;

	if (task == NULL)
		return;

	croutine_current_task = NULL;
	state = atomic_load_explicit(&task->state, memory_order_acquire);
	switch (state) {
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
		return;
	case CROUTINE_TASK_RUNNING:
		expected = CROUTINE_TASK_RUNNING;
		if (!atomic_compare_exchange_strong_explicit(&task->state, &expected, CROUTINE_TASK_READY, memory_order_acq_rel,
													 memory_order_acquire))
			abort();
		enqueue = 1;
		break;
	case CROUTINE_TASK_PARKING:
		expected = CROUTINE_TASK_PARKING;
		if (atomic_compare_exchange_strong_explicit(&task->state, &expected, CROUTINE_TASK_WAITING,
													memory_order_acq_rel, memory_order_acquire))
			return;
		if (expected != CROUTINE_TASK_NOTIFIED)
			abort();
		state = CROUTINE_TASK_NOTIFIED;
		/* Fall through. */
	case CROUTINE_TASK_NOTIFIED:
		expected = state;
		if (!atomic_compare_exchange_strong_explicit(&task->state, &expected, CROUTINE_TASK_READY, memory_order_acq_rel,
													 memory_order_acquire))
			abort();
		enqueue = 1;
		break;
	case CROUTINE_TASK_PENDING:
	case CROUTINE_TASK_READY:
	case CROUTINE_TASK_WAITING:
	default:
		abort();
	}

	if (enqueue && croutine_task_enqueue(task) == CROUTINE_TASK_ENQUEUE_ERROR)
		abort();
}

static struct croutine_task *croutine_worker_pop_local(struct croutine_worker *worker) {
	struct croutine_task *task;
	void *item;

	if (worker == NULL)
		return NULL;

	item = croutine_cldeque_pop(worker->local_queue);
	if (item == NULL)
		return NULL;

	task = item;
	if (atomic_load_explicit(&task->state, memory_order_acquire) != CROUTINE_TASK_READY ||
		atomic_load_explicit(&task->worker, memory_order_relaxed) != worker)
		abort();
	return task;
}

static int croutine_worker_enter_waiting(struct croutine_worker *worker) {
	struct croutine_scheduler *scheduler = worker->scheduler;

	if (atomic_load_explicit(&scheduler->state, memory_order_acquire) != CROUTINE_SCHEDULER_RUNNING)
		return 0;
	if (atomic_load_explicit(&worker->state, memory_order_acquire) != CROUTINE_WORKER_RUNNING)
		abort();

	atomic_store_explicit(&worker->state, CROUTINE_WORKER_SOURCE_WAITING, memory_order_release);
	atomic_fetch_add_explicit(&scheduler->waiting_workers, 1, memory_order_release);
	atomic_thread_fence(memory_order_seq_cst);
	return 1;
}

static void croutine_worker_leave_waiting(struct croutine_worker *worker) {
	struct croutine_scheduler *scheduler = worker->scheduler;

	if (atomic_load_explicit(&worker->state, memory_order_acquire) != CROUTINE_WORKER_SOURCE_WAITING ||
		atomic_fetch_sub_explicit(&scheduler->waiting_workers, 1, memory_order_acq_rel) == 0)
		abort();
	atomic_store_explicit(&worker->state, CROUTINE_WORKER_RUNNING, memory_order_release);
}

static size_t croutine_worker_local_space(struct croutine_worker *worker) {
	size_t len;

	if (worker == NULL || worker->local_queue == NULL || worker->local_queue->capacity == 0)
		return 0;

	len = croutine_cldeque_len(worker->local_queue);
	if (len >= worker->local_queue->capacity)
		return 0;
	return (size_t)(worker->local_queue->capacity - len);
}

static void croutine_worker_requeue_main(struct croutine_worker *worker, struct croutine_task *task) {
	if (croutine_scheduler_enqueue_main(worker->scheduler, task) != 0)
		abort();
}

static void croutine_worker_drain_inbox_snapshot(struct croutine_worker *worker) {
	size_t snapshot;

	if (worker == NULL || worker->inbox_queue == NULL)
		return;

	snapshot = croutine_mpmc_queue_len(worker->inbox_queue);
	if (snapshot > worker->inbox_queue->capacity)
		snapshot = worker->inbox_queue->capacity;
	for (size_t index = 0; index < snapshot; index++) {
		struct croutine_task *task = croutine_mpmc_queue_pop(worker->inbox_queue);

		if (task == NULL)
			break;
		if (atomic_load_explicit(&task->state, memory_order_acquire) != CROUTINE_TASK_READY ||
			atomic_load_explicit(&task->worker, memory_order_relaxed) != worker)
			abort();
		if (croutine_worker_enqueue_local(worker, task) != 0)
			croutine_worker_requeue_main(worker, task);
	}
}

static struct croutine_task *croutine_worker_pop_main(struct croutine_worker *worker) {
	struct croutine_task *task;

	if (worker == NULL || worker->scheduler == NULL)
		return NULL;
	task = croutine_mpmc_queue_pop(worker->scheduler->main_queue);
	if (task == NULL)
		return NULL;
	if (atomic_load_explicit(&task->state, memory_order_acquire) != CROUTINE_TASK_READY ||
		atomic_load_explicit(&task->worker, memory_order_relaxed) != NULL)
		abort();
	atomic_store_explicit(&task->worker, worker, memory_order_relaxed);
	return task;
}

static struct croutine_task *croutine_worker_take_main_batch(struct croutine_worker *worker) {
	struct croutine_scheduler *scheduler;
	struct croutine_task *direct = NULL;
	size_t batch;
	size_t space;

	if (worker == NULL || worker->scheduler == NULL)
		return NULL;

	scheduler = worker->scheduler;
	batch = croutine_mpmc_queue_len(scheduler->main_queue);
	if (batch == 0)
		batch = 1;
	else
		batch -= batch / 2;
	space = croutine_worker_local_space(worker);
	if (batch > space + 1)
		batch = space + 1;

	for (size_t index = 0; index < batch; index++) {
		struct croutine_task *task = croutine_worker_pop_main(worker);

		if (task == NULL)
			break;
		if (direct == NULL) {
			direct = task;
			continue;
		}
		if (croutine_worker_enqueue_local(worker, task) != 0)
			croutine_worker_requeue_main(worker, task);
	}
	return direct;
}

static struct croutine_task *croutine_worker_take_peer_batch(struct croutine_worker *worker) {
	struct croutine_scheduler *scheduler;
	size_t worker_count;
	size_t start;

	if (worker == NULL || worker->scheduler == NULL)
		return NULL;

	scheduler = worker->scheduler;
	worker_count = scheduler->worker_count;
	if (worker_count <= 1)
		return NULL;

	if (worker_count <= 8) {
		start = atomic_fetch_add_explicit(&scheduler->scan_index, 1, memory_order_relaxed) % worker_count;
	} else {
		worker->steal_seed ^= worker->steal_seed << 13;
		worker->steal_seed ^= worker->steal_seed >> 17;
		worker->steal_seed ^= worker->steal_seed << 5;
		start = (size_t)worker->steal_seed % worker_count;
	}
	for (size_t step = 0; step < worker_count; step++) {
		struct croutine_worker *victim;
		struct croutine_task *direct = NULL;
		size_t index = (start + step) % worker_count;
		size_t batch;
		size_t space;

		victim = &scheduler->workers[index];
		if (victim == worker)
			continue;
		batch = croutine_cldeque_len(victim->local_queue) / 2;
		if (batch == 0)
			batch = 1;
		if (batch > scheduler->config.steal_batch_max)
			batch = scheduler->config.steal_batch_max;
		space = croutine_worker_local_space(worker);
		if (batch > space + 1)
			batch = space + 1;

		for (size_t item_index = 0; item_index < batch; item_index++) {
			struct croutine_task *task = croutine_cldeque_steal(victim->local_queue);

			if (task == NULL)
				break;
			if (atomic_load_explicit(&task->state, memory_order_acquire) != CROUTINE_TASK_READY ||
				atomic_load_explicit(&task->worker, memory_order_relaxed) != victim)
				abort();
			atomic_store_explicit(&task->worker, worker, memory_order_relaxed);
			if (direct == NULL) {
				direct = task;
				continue;
			}
			if (croutine_worker_enqueue_local(worker, task) != 0)
				croutine_worker_requeue_main(worker, task);
		}
		if (direct != NULL)
			return direct;
	}

	return NULL;
}

static void croutine_worker_chain_wake(struct croutine_worker *worker) {
	if (worker != NULL && croutine_cldeque_len(worker->local_queue) != 0)
		croutine_scheduler_wake_one(worker->scheduler);
}

static struct croutine_task *croutine_worker_next_task(struct croutine_worker *worker) {
	struct croutine_task *task;

	croutine_worker_drain_inbox_snapshot(worker);
	if (worker->queue_check_turns >= worker->scheduler->config.queue_check_interval) {
		worker->queue_check_turns = 0;
		task = croutine_worker_pop_main(worker);
		if (task != NULL)
			return task;
	}

	task = croutine_worker_pop_local(worker);
	if (task != NULL) {
		worker->queue_check_turns++;
		return task;
	}

	task = croutine_worker_take_main_batch(worker);
	if (task != NULL) {
		worker->queue_check_turns = 0;
		return task;
	}

	task = croutine_worker_take_peer_batch(worker);
	if (task != NULL)
		worker->queue_check_turns++;
	return task;
}

static void croutine_worker_dispatch_task(struct croutine_worker *worker, struct croutine_task *task) {
	if (task == NULL)
		return;

	if (atomic_load_explicit(&worker->state, memory_order_acquire) == CROUTINE_WORKER_RUNNING &&
		atomic_load_explicit(&worker->scheduler->state, memory_order_acquire) == CROUTINE_SCHEDULER_RUNNING)
		croutine_task_resume(task);
	if (croutine_task_enqueue(task) == CROUTINE_TASK_ENQUEUE_ERROR)
		abort();
}

void croutine_worker_schedule(void) {
	struct croutine_worker *worker = croutine_current_worker;
	struct croutine_main_event_source *source;
	struct croutine_task *task;
	enum croutine_main_event_wait_result wait_result;
	enum croutine_scheduler_state scheduler_state;

	if (worker == NULL || worker->main_event_source == NULL)
		abort();
	source = worker->main_event_source;

	for (;;) {
		croutine_worker_process_current(worker);
		scheduler_state = atomic_load_explicit(&worker->scheduler->state, memory_order_acquire);
		switch (scheduler_state) {
		case CROUTINE_SCHEDULER_RUNNING:
			if (atomic_load_explicit(&worker->state, memory_order_acquire) != CROUTINE_WORKER_RUNNING)
				abort();
			break;
		case CROUTINE_SCHEDULER_STOPPING:
			croutine_worker_suspend(worker);
			continue;
		case CROUTINE_SCHEDULER_DESTROYING:
			atomic_store_explicit(&worker->state, CROUTINE_WORKER_EXITING, memory_order_release);
			atomic_store_explicit(&worker->state, CROUTINE_WORKER_EXITED, memory_order_release);
			croutine_current_task = NULL;
			croutine_current_worker = NULL;
			croutine_sched = NULL;
			pthread_exit(NULL);
		case CROUTINE_SCHEDULER_INIT:
		case CROUTINE_SCHEDULER_STOPPED:
		default:
			abort();
		}

		source->collect(source);
		if (atomic_load_explicit(&worker->scheduler->state, memory_order_acquire) != CROUTINE_SCHEDULER_RUNNING)
			continue;

		atomic_store_explicit(&worker->state, CROUTINE_WORKER_SEARCHING, memory_order_release);
		task = croutine_worker_next_task(worker);
		if (atomic_load_explicit(&worker->state, memory_order_acquire) != CROUTINE_WORKER_SEARCHING)
			abort();
		atomic_store_explicit(&worker->state, CROUTINE_WORKER_RUNNING, memory_order_release);
		if (task != NULL) {
			croutine_worker_chain_wake(worker);
			croutine_worker_dispatch_task(worker, task);
			continue;
		}

		if (!croutine_worker_enter_waiting(worker))
			continue;
		source->collect(source);
		task = croutine_worker_next_task(worker);
		if (task != NULL) {
			croutine_worker_leave_waiting(worker);
			croutine_worker_chain_wake(worker);
			croutine_worker_dispatch_task(worker, task);
			continue;
		}
		if (atomic_load_explicit(&worker->scheduler->state, memory_order_acquire) != CROUTINE_SCHEDULER_RUNNING) {
			croutine_worker_leave_waiting(worker);
			continue;
		}

		wait_result = source->blocking_wait(source);
		croutine_worker_leave_waiting(worker);
		if (wait_result != CROUTINE_MAIN_EVENT_WAIT_DONE)
			abort();
	}
}
