#include "croutine_event.h"
#include "scheduler.h"
#include "task.h"
#include "tls.h"
#include "worker.h"

#include <stdatomic.h>
#include <stdlib.h>

static struct croutine_task *
croutine_worker_pop_local(struct croutine_worker *worker) {
	void *item;

	if (worker == NULL)
		return NULL;

	pthread_mutex_lock(&worker->local_queue_lock);
	if (croutine_queue_pop(&worker->local_queue, &item) != 0)
		item = NULL;
	pthread_mutex_unlock(&worker->local_queue_lock);

	return item;
}

static void croutine_worker_process_current(struct croutine_worker *worker) {
	struct croutine_task *task;
	enum croutine_task_state state;

	task = croutine_current_task;
	if (task == NULL)
		return;

	croutine_current_task = NULL;
	state = atomic_load_explicit(&task->state, memory_order_acquire);

	if (state == CROUTINE_TASK_FINISHED) {
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
		return;
	}

	if (state == CROUTINE_TASK_READY) {
		if (croutine_task_enqueue(task) != 0)
			abort();
		return;
	}
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

static struct croutine_task *
croutine_worker_next_task(struct croutine_worker *worker) {
	struct croutine_scheduler *scheduler;
	struct croutine_task *task;
	size_t quota;

	scheduler = worker->scheduler;
	quota = scheduler->config.main_queue_quota;
	if (quota == 0)
		quota = 1;

	if (worker->local_turns >= quota) {
		task = croutine_scheduler_pop_main(worker);
		worker->local_turns = 0;
		if (task != NULL)
			return task;
	}

	task = croutine_worker_pop_local(worker);
	if (task != NULL) {
		worker->local_turns++;
		return task;
	}

	task = croutine_scheduler_pop_main(worker);
	if (task != NULL)
		worker->local_turns = 0;
	return task;
}

static void croutine_worker_wait(struct croutine_worker *worker) {
	struct croutine_main_event_source *source;

	source = worker->main_event_source;
	if (source != NULL && source->blocking_wait != NULL)
		source->blocking_wait(source);
}

static void croutine_worker_suspend(struct croutine_worker *worker) {
	struct croutine_main_event_source *source;

	source = worker->main_event_source;
	if (source != NULL && source->suspend != NULL)
		source->suspend(source);
}

static void croutine_worker_exit_thread(struct croutine_worker *worker) {
	(void)worker;

	croutine_current_task = NULL;
	croutine_current_worker = NULL;
	croutine_sched = NULL;
	pthread_exit(NULL);
}

static void croutine_worker_exit_if_destroying(struct croutine_worker *worker) {
	if (croutine_scheduler_is_destroying(worker->scheduler))
		croutine_worker_exit_thread(worker);
}

static void croutine_worker_collect(struct croutine_worker *worker) {
	struct croutine_main_event_source *source;

	source = worker->main_event_source;
	if (source != NULL && source->collect != NULL)
		source->collect(source);
}

static void croutine_worker_wait_while_stopping(struct croutine_worker *worker) {
	while (croutine_scheduler_is_stopping(worker->scheduler)) {
		croutine_scheduler_worker_quiescent(worker);
		croutine_worker_suspend(worker);
		croutine_worker_exit_if_destroying(worker);
	}
}

static struct croutine_task *
croutine_worker_select_next_task(struct croutine_worker *worker) {
	for (;;) {
		struct croutine_task *task;

		croutine_worker_exit_if_destroying(worker);
		croutine_worker_wait_while_stopping(worker);
		croutine_worker_collect(worker);

		task = croutine_worker_next_task(worker);
		if (task != NULL)
			return task;

		croutine_worker_wait(worker);
	}
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
	worker->quiescent_state = CROUTINE_WORKER_ACTIVE;
	worker->schedule = croutine_worker_schedule;
	if (pthread_mutex_init(&worker->local_queue_lock, NULL) != 0)
		return -1;
	worker->local_queue_lock_initialized = 1;
	if (croutine_queue_init(&worker->local_queue,
							CROUTINE_DEFAULT_READY_QUEUE_CAPACITY) != 0) {
		pthread_mutex_destroy(&worker->local_queue_lock);
		worker->local_queue_lock_initialized = 0;
		return -1;
	}
	croutine_list_init(&worker->dead_tasks);
	worker->main_event_source = NULL;
	worker->local_turns = 0;
	return 0;
}

int croutine_worker_start(struct croutine_worker *worker) {
	if (worker == NULL || worker->start_state == CROUTINE_WORKER_STARTED)
		return -1;

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

void croutine_worker_schedule(void) {
	struct croutine_worker *worker = croutine_current_worker;
	struct croutine_task *task;

	if (worker == NULL)
		abort();

	croutine_worker_reclaim_dead(worker);
	croutine_worker_process_current(worker);
	task = croutine_worker_select_next_task(worker);
	croutine_task_resume(task);
	abort();
}
