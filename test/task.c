#define _POSIX_C_SOURCE 200809L

#include "croutine_event.h"
#include "task.h"
#include "types.h"
#include "worker.h"

#include <pthread.h>
#include <signal.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#define WAIT_LIMIT 5000
#define WAIT_NS 1000000L
#define RACE_ROUNDS 1000
#define LIFECYCLE_ROUNDS 100

struct test_source {
	croutine_main_event_source base;
	pthread_mutex_t lock;
	pthread_cond_t work_cond;
	pthread_cond_t resume_cond;
	int work_pending;
	int resume_pending;
};

struct test_context {
	struct croutine_task *task;
	_Atomic int stage;
	_Atomic int failed;
};

struct affinity_context {
	struct croutine_task *task;
	struct croutine_worker *home;
	_Atomic int stage;
	_Atomic int failed;
};

struct inbox_priority_context {
	struct croutine_task *waiting_task;
	_Atomic int waiting_ready;
	_Atomic int runner_ready;
	_Atomic int wake_done;
	_Atomic int order;
	_Atomic int waiting_order;
	_Atomic int runner_order;
	_Atomic int failed;
};

struct main_interval_context {
	_Atomic int runner_ready;
	_Atomic int main_spawned;
	_Atomic int main_done;
	_Atomic int runner_runs;
	_Atomic int observed_runs;
};

static struct test_source *test_source_from_base(croutine_main_event_source *base) {
	return (struct test_source *)base;
}

static enum croutine_main_event_wait_result source_blocking_wait(croutine_main_event_source *base) {
	struct test_source *source = test_source_from_base(base);

	pthread_mutex_lock(&source->lock);
	while (!source->work_pending)
		pthread_cond_wait(&source->work_cond, &source->lock);
	source->work_pending = 0;
	pthread_mutex_unlock(&source->lock);
	return CROUTINE_MAIN_EVENT_WAIT_DONE;
}

static void source_collect(croutine_main_event_source *base) {
	struct test_source *source = test_source_from_base(base);

	pthread_mutex_lock(&source->lock);
	source->work_pending = 0;
	pthread_mutex_unlock(&source->lock);
}

static int source_wake(croutine_main_event_source *base) {
	struct test_source *source = test_source_from_base(base);

	pthread_mutex_lock(&source->lock);
	source->work_pending = 1;
	pthread_cond_signal(&source->work_cond);
	pthread_mutex_unlock(&source->lock);
	return 0;
}

static void source_suspend(croutine_main_event_source *base) {
	struct test_source *source = test_source_from_base(base);

	pthread_mutex_lock(&source->lock);
	while (!source->resume_pending)
		pthread_cond_wait(&source->resume_cond, &source->lock);
	source->resume_pending = 0;
	pthread_mutex_unlock(&source->lock);
}

static void source_resume(croutine_main_event_source *base) {
	struct test_source *source = test_source_from_base(base);

	pthread_mutex_lock(&source->lock);
	source->resume_pending = 1;
	pthread_cond_signal(&source->resume_cond);
	pthread_mutex_unlock(&source->lock);
}

static void source_destroy(croutine_main_event_source *base) {
	struct test_source *source = test_source_from_base(base);

	pthread_cond_destroy(&source->resume_cond);
	pthread_cond_destroy(&source->work_cond);
	pthread_mutex_destroy(&source->lock);
	free(source);
}

static croutine_main_event_source *source_factory(croutine_worker *worker, void *arg) {
	struct test_source *source;

	(void)worker;
	(void)arg;
	source = calloc(1, sizeof(*source));
	if (source == NULL)
		return NULL;
	if (pthread_mutex_init(&source->lock, NULL) != 0)
		goto fail_free;
	if (pthread_cond_init(&source->work_cond, NULL) != 0)
		goto fail_lock;
	if (pthread_cond_init(&source->resume_cond, NULL) != 0)
		goto fail_work_cond;

	source->base.blocking_wait = source_blocking_wait;
	source->base.collect = source_collect;
	source->base.wake = source_wake;
	source->base.suspend = source_suspend;
	source->base.resume = source_resume;
	source->base.destroy = source_destroy;
	return &source->base;

fail_work_cond:
	pthread_cond_destroy(&source->work_cond);
fail_lock:
	pthread_mutex_destroy(&source->lock);
fail_free:
	free(source);
	return NULL;
}

static int task_state_is(struct croutine_task *task, enum croutine_task_state state) {
	return atomic_load_explicit(&task->state, memory_order_acquire) == state;
}

static void fail(struct test_context *context) {
	atomic_store_explicit(&context->failed, 1, memory_order_release);
}

static void *state_task(void *arg) {
	struct test_context *context = arg;
	struct croutine_task *task = croutine_task_current();

	if (task == NULL || !task_state_is(task, CROUTINE_TASK_RUNNING)) {
		fail(context);
		return NULL;
	}

	if (croutine_await() != 0 || !task_state_is(task, CROUTINE_TASK_PARKING) || croutine_task_wake(task) != 0 ||
		!task_state_is(task, CROUTINE_TASK_NOTIFIED)) {
		fail(context);
		return NULL;
	}
	croutine_yield();
	if (!task_state_is(task, CROUTINE_TASK_RUNNING)) {
		fail(context);
		return NULL;
	}

	if (croutine_await() != 0 || croutine_cancel_await() != 0 || !task_state_is(task, CROUTINE_TASK_RUNNING)) {
		fail(context);
		return NULL;
	}

	croutine_yield();
	if (!task_state_is(task, CROUTINE_TASK_RUNNING) || croutine_await() != 0) {
		fail(context);
		return NULL;
	}

	context->task = task;
	atomic_store_explicit(&context->stage, 1, memory_order_release);
	croutine_yield();
	if (!task_state_is(task, CROUTINE_TASK_RUNNING)) {
		fail(context);
		return NULL;
	}

	for (size_t round = 0; round < RACE_ROUNDS; round++) {
		if (croutine_await() != 0) {
			fail(context);
			return NULL;
		}
		atomic_store_explicit(&context->stage, (int)round + 2, memory_order_release);
		croutine_yield();
		if (!task_state_is(task, CROUTINE_TASK_RUNNING)) {
			fail(context);
			return NULL;
		}
	}

	atomic_store_explicit(&context->stage, RACE_ROUNDS + 2, memory_order_release);
	return NULL;
}

static int wait_for_stage(struct test_context *context, int stage) {
	struct timespec pause = { .tv_nsec = WAIT_NS };

	for (size_t attempt = 0; attempt < WAIT_LIMIT; attempt++) {
		if (atomic_load_explicit(&context->failed, memory_order_acquire) != 0)
			return -1;
		if (atomic_load_explicit(&context->stage, memory_order_acquire) >= stage)
			return 0;
		nanosleep(&pause, NULL);
	}
	return -1;
}

static int wait_for_task_state_failed(struct croutine_task *task, _Atomic int *failed, enum croutine_task_state state) {
	struct timespec pause = { .tv_nsec = WAIT_NS };

	for (size_t attempt = 0; attempt < WAIT_LIMIT; attempt++) {
		if (task != NULL && task_state_is(task, state))
			return 0;
		if (failed != NULL && atomic_load_explicit(failed, memory_order_acquire) != 0)
			return -1;
		nanosleep(&pause, NULL);
	}
	return -1;
}

static int wait_for_task_state(struct test_context *context, enum croutine_task_state state) {
	return wait_for_task_state_failed(context->task, &context->failed, state);
}

static int wait_for_worker_state(struct croutine_worker *worker, enum croutine_worker_state state) {
	struct timespec pause = { .tv_nsec = WAIT_NS };

	for (size_t attempt = 0; attempt < WAIT_LIMIT; attempt++) {
		if (atomic_load_explicit(&worker->state, memory_order_acquire) == state)
			return 0;
		nanosleep(&pause, NULL);
	}
	return -1;
}

static int wait_for_affinity_stage(struct affinity_context *context, int stage) {
	struct timespec pause = { .tv_nsec = WAIT_NS };

	for (size_t attempt = 0; attempt < WAIT_LIMIT; attempt++) {
		if (atomic_load_explicit(&context->failed, memory_order_acquire) != 0)
			return -1;
		if (atomic_load_explicit(&context->stage, memory_order_acquire) >= stage)
			return 0;
		nanosleep(&pause, NULL);
	}
	return -1;
}

static void *affinity_task(void *arg) {
	struct affinity_context *context = arg;
	struct croutine_task *task = croutine_task_current();
	struct croutine_worker *home;

	if (task == NULL)
		goto fail;
	home = atomic_load_explicit(&task->worker, memory_order_relaxed);
	if (home == NULL)
		goto fail;
	context->task = task;
	context->home = home;
	if (croutine_await() != 0)
		goto fail;
	atomic_store_explicit(&context->stage, 1, memory_order_release);
	croutine_yield();
	if (atomic_load_explicit(&task->worker, memory_order_relaxed) != home)
		goto fail;

	if (croutine_await() != 0)
		goto fail;
	atomic_store_explicit(&context->stage, 2, memory_order_release);
	croutine_yield();
	if (atomic_load_explicit(&task->worker, memory_order_relaxed) != home)
		goto fail;
	atomic_store_explicit(&context->stage, 3, memory_order_release);
	return NULL;

fail:
	atomic_store_explicit(&context->failed, 1, memory_order_release);
	return NULL;
}

static void *inbox_waiting_task(void *arg) {
	struct inbox_priority_context *context = arg;

	context->waiting_task = croutine_task_current();
	if (context->waiting_task == NULL || croutine_await() != 0) {
		atomic_store_explicit(&context->failed, 1, memory_order_release);
		return NULL;
	}
	atomic_store_explicit(&context->waiting_ready, 1, memory_order_release);
	croutine_yield();
	atomic_store_explicit(&context->waiting_order,
						  atomic_fetch_add_explicit(&context->order, 1, memory_order_relaxed) + 1,
						  memory_order_release);
	return NULL;
}

static void *inbox_runner_task(void *arg) {
	struct inbox_priority_context *context = arg;

	atomic_store_explicit(&context->runner_ready, 1, memory_order_release);
	while (atomic_load_explicit(&context->wake_done, memory_order_acquire) == 0)
		;
	croutine_yield();
	atomic_store_explicit(&context->runner_order,
						  atomic_fetch_add_explicit(&context->order, 1, memory_order_relaxed) + 1,
						  memory_order_release);
	return NULL;
}

static int wait_for_atomic(_Atomic int *value, int expected) {
	struct timespec pause = { .tv_nsec = WAIT_NS };

	for (size_t attempt = 0; attempt < WAIT_LIMIT; attempt++) {
		if (atomic_load_explicit(value, memory_order_acquire) >= expected)
			return 0;
		nanosleep(&pause, NULL);
	}
	return -1;
}

static int test_inbox_priority(const croutine_config *base_config) {
	struct inbox_priority_context context = { 0 };
	croutine_config config = *base_config;
	croutine_scheduler *scheduler = NULL;
	int status = -1;

	config.workers = 1;
	config.queue_check_interval = 100;
	atomic_init(&context.waiting_ready, 0);
	atomic_init(&context.runner_ready, 0);
	atomic_init(&context.wake_done, 0);
	atomic_init(&context.order, 0);
	atomic_init(&context.waiting_order, 0);
	atomic_init(&context.runner_order, 0);
	atomic_init(&context.failed, 0);
	if (croutine_scheduler_create(&scheduler, &config) != 0 || croutine_scheduler_start(scheduler) != 0 ||
		croutine_spawn(scheduler, inbox_waiting_task, &context) != 0 ||
		wait_for_atomic(&context.waiting_ready, 1) != 0 ||
		wait_for_task_state_failed(context.waiting_task, &context.failed, CROUTINE_TASK_WAITING) != 0 ||
		croutine_spawn(scheduler, inbox_runner_task, &context) != 0 || wait_for_atomic(&context.runner_ready, 1) != 0 ||
		croutine_task_wake(context.waiting_task) != 0)
		goto out;
	atomic_store_explicit(&context.wake_done, 1, memory_order_release);
	if (wait_for_atomic(&context.waiting_order, 1) != 0 || wait_for_atomic(&context.runner_order, 2) != 0 ||
		atomic_load_explicit(&context.failed, memory_order_acquire) != 0)
		goto out;
	status = 0;

out:
	atomic_store_explicit(&context.wake_done, 1, memory_order_release);
	if (scheduler != NULL && croutine_scheduler_stop(scheduler) != 0)
		status = -1;
	if (scheduler != NULL && croutine_scheduler_destroy(scheduler) != 0)
		status = -1;
	return status;
}

static void *main_interval_runner(void *arg) {
	struct main_interval_context *context = arg;

	atomic_store_explicit(&context->runner_ready, 1, memory_order_release);
	while (atomic_load_explicit(&context->main_spawned, memory_order_acquire) == 0)
		;
	for (;;) {
		croutine_yield();
		if (atomic_load_explicit(&context->main_done, memory_order_acquire) != 0)
			break;
		atomic_fetch_add_explicit(&context->runner_runs, 1, memory_order_relaxed);
	}
	return NULL;
}

static void *main_interval_task(void *arg) {
	struct main_interval_context *context = arg;

	atomic_store_explicit(&context->observed_runs, atomic_load_explicit(&context->runner_runs, memory_order_relaxed),
						  memory_order_release);
	atomic_store_explicit(&context->main_done, 1, memory_order_release);
	return NULL;
}

static int test_main_interval(const croutine_config *base_config) {
	struct main_interval_context context = { 0 };
	croutine_config config = *base_config;
	croutine_scheduler *scheduler = NULL;
	int status = -1;

	config.workers = 1;
	config.queue_check_interval = 3;
	atomic_init(&context.runner_ready, 0);
	atomic_init(&context.main_spawned, 0);
	atomic_init(&context.main_done, 0);
	atomic_init(&context.runner_runs, 0);
	atomic_init(&context.observed_runs, 0);
	if (croutine_scheduler_create(&scheduler, &config) != 0 || croutine_scheduler_start(scheduler) != 0 ||
		croutine_spawn(scheduler, main_interval_runner, &context) != 0 ||
		wait_for_atomic(&context.runner_ready, 1) != 0 || croutine_spawn(scheduler, main_interval_task, &context) != 0)
		goto out;
	atomic_store_explicit(&context.main_spawned, 1, memory_order_release);
	if (wait_for_atomic(&context.main_done, 1) != 0)
		goto out;
	if (atomic_load_explicit(&context.observed_runs, memory_order_acquire) != 3) {
		fprintf(stderr, "observed main after %d runner runs\n",
				atomic_load_explicit(&context.observed_runs, memory_order_acquire));
		goto out;
	}
	status = 0;

out:
	atomic_store_explicit(&context.main_spawned, 1, memory_order_release);
	atomic_store_explicit(&context.main_done, 1, memory_order_release);
	if (scheduler != NULL && croutine_scheduler_stop(scheduler) != 0)
		status = -1;
	if (scheduler != NULL && croutine_scheduler_destroy(scheduler) != 0)
		status = -1;
	return status;
}

static int test_inbox_affinity(const croutine_config *base_config) {
	struct affinity_context context = { 0 };
	croutine_config config = *base_config;
	croutine_scheduler *scheduler = NULL;
	int status = -1;

	config.workers = 2;
	atomic_init(&context.stage, 0);
	atomic_init(&context.failed, 0);
	if (croutine_scheduler_create(&scheduler, &config) != 0 || croutine_scheduler_start(scheduler) != 0 ||
		croutine_spawn(scheduler, affinity_task, &context) != 0)
		goto out;
	if (wait_for_affinity_stage(&context, 1) != 0 || context.task == NULL || context.home == NULL ||
		wait_for_task_state_failed(context.task, &context.failed, CROUTINE_TASK_WAITING) != 0 ||
		croutine_task_wake(context.task) != 0 || wait_for_affinity_stage(&context, 2) != 0 ||
		wait_for_task_state_failed(context.task, &context.failed, CROUTINE_TASK_WAITING) != 0)
		goto out;

	if (croutine_scheduler_stop(scheduler) != 0 || croutine_task_wake(context.task) != 0 ||
		croutine_mpmc_queue_len(context.home->inbox_queue) != 1 ||
		croutine_mpmc_queue_len(scheduler->main_queue) != 0 || croutine_scheduler_start(scheduler) != 0 ||
		wait_for_affinity_stage(&context, 3) != 0 || atomic_load_explicit(&context.failed, memory_order_acquire) != 0)
		goto out;
	status = 0;

out:
	if (scheduler != NULL && croutine_scheduler_stop(scheduler) != 0)
		status = -1;
	if (scheduler != NULL && croutine_scheduler_destroy(scheduler) != 0)
		status = -1;
	return status;
}

static void init_ready_task(struct croutine_task *task, struct croutine_scheduler *scheduler,
							struct croutine_worker *home) {
	memset(task, 0, sizeof(*task));
	task->scheduler = scheduler;
	atomic_init(&task->worker, home);
	atomic_init(&task->state, CROUTINE_TASK_READY);
}

static int test_enqueue_fallback(const croutine_config *base_config) {
	struct croutine_task tasks[5];
	croutine_config config = *base_config;
	croutine_scheduler *scheduler = NULL;
	struct croutine_worker *worker;
	int status = -1;

	config.workers = 1;
	config.local_queue_capacity = 2;
	config.inbox_queue_capacity = 2;
	config.main_queue_capacity = 2;
	if (croutine_scheduler_create(&scheduler, &config) != 0)
		return -1;
	worker = &scheduler->workers[0];
	for (size_t index = 0; index < 5; index++)
		init_ready_task(&tasks[index], scheduler, worker);

	if (croutine_worker_enqueue_local(worker, &tasks[0]) != 0 ||
		croutine_worker_enqueue_local(worker, &tasks[1]) != 0 ||
		croutine_task_enqueue(&tasks[2]) != CROUTINE_TASK_ENQUEUE_INBOX ||
		croutine_task_enqueue(&tasks[3]) != CROUTINE_TASK_ENQUEUE_INBOX ||
		croutine_task_enqueue(&tasks[4]) != CROUTINE_TASK_ENQUEUE_MAIN)
		goto out;
	if (atomic_load_explicit(&tasks[2].worker, memory_order_relaxed) != worker ||
		atomic_load_explicit(&tasks[3].worker, memory_order_relaxed) != worker ||
		atomic_load_explicit(&tasks[4].worker, memory_order_relaxed) != NULL ||
		croutine_cldeque_pop(worker->local_queue) != &tasks[1] ||
		croutine_cldeque_pop(worker->local_queue) != &tasks[0] ||
		croutine_mpmc_queue_pop(worker->inbox_queue) != &tasks[2] ||
		croutine_mpmc_queue_pop(worker->inbox_queue) != &tasks[3] ||
		croutine_mpmc_queue_pop(scheduler->main_queue) != &tasks[4])
		goto out;
	status = 0;

out:
	if (croutine_scheduler_destroy(scheduler) != 0)
		status = -1;
	return status;
}

static int create_must_fail(croutine_config config) {
	croutine_scheduler *scheduler = NULL;

	if (croutine_scheduler_create(&scheduler, &config) == 0) {
		(void)croutine_scheduler_destroy(scheduler);
		return -1;
	}
	return 0;
}

static int test_config_normalization(const croutine_config *base_config) {
	croutine_config config = *base_config;
	croutine_scheduler *scheduler = NULL;

	config.workers = 3;
	config.queue_check_interval = 7;
	config.steal_batch_max = 17;
	config.local_queue_capacity = 8;
	config.inbox_queue_capacity = 0;
	config.main_queue_capacity = 0;
	if (croutine_scheduler_create(&scheduler, &config) != 0)
		return -1;
	if (scheduler->config.inbox_queue_capacity != 8 ||
		scheduler->config.main_queue_capacity != CROUTINE_DEFAULT_MAIN_QUEUE_CAPACITY ||
		scheduler->config.queue_check_interval != 7 || scheduler->config.steal_batch_max != 17 ||
		croutine_scheduler_destroy(scheduler) != 0)
		return -1;

	config = *base_config;
	config.local_queue_capacity = 3;
	if (create_must_fail(config) != 0)
		return -1;
	config = *base_config;
	config.local_queue_capacity = 8;
	config.inbox_queue_capacity = 16;
	if (croutine_scheduler_create(&scheduler, &config) != 0 || croutine_scheduler_destroy(scheduler) != 0)
		return -1;
	config = *base_config;
	config.inbox_queue_capacity = 3;
	if (create_must_fail(config) != 0)
		return -1;
	config = *base_config;
	config.main_queue_capacity = 3;
	if (create_must_fail(config) != 0)
		return -1;

	config = *base_config;
	config.queue_check_interval = 9;
	if (croutine_scheduler_create(&scheduler, &config) != 0)
		return -1;
	return scheduler->config.queue_check_interval == 9 && croutine_scheduler_destroy(scheduler) == 0 ? 0 : -1;
}

static int test_overload_abort(const croutine_config *base_config) {
	pid_t child = fork();
	int child_status;

	if (child < 0)
		return -1;
	if (child == 0) {
		struct croutine_task tasks[7];
		croutine_config config = *base_config;
		croutine_scheduler *scheduler = NULL;
		struct croutine_worker *worker;

		config.workers = 1;
		config.local_queue_capacity = 2;
		config.inbox_queue_capacity = 2;
		config.main_queue_capacity = 2;
		if (croutine_scheduler_create(&scheduler, &config) != 0)
			_exit(2);
		worker = &scheduler->workers[0];
		for (size_t index = 0; index < 7; index++)
			init_ready_task(&tasks[index], scheduler, worker);
		if (croutine_worker_enqueue_local(worker, &tasks[0]) != 0 ||
			croutine_worker_enqueue_local(worker, &tasks[1]) != 0)
			_exit(2);
		for (size_t index = 2; index < 7; index++)
			(void)croutine_task_enqueue(&tasks[index]);
		_exit(3);
	}
	if (waitpid(child, &child_status, 0) != child)
		return -1;
	return WIFSIGNALED(child_status) && WTERMSIG(child_status) == SIGABRT ? 0 : -1;
}

int main(void) {
	struct test_context context = { 0 };
	croutine_scheduler *unstarted = NULL;
	croutine_scheduler *scheduler = NULL;
	croutine_config config = {
		.workers = 1,
		.main_event_source_config = {
			.factory_fn = source_factory,
		},
		.local_queue_capacity = 8,
		.main_queue_capacity = 8,
	};
	int status = 1;

	atomic_init(&context.stage, 0);
	atomic_init(&context.failed, 0);
	if (test_config_normalization(&config) != 0) {
		fprintf(stderr, "config normalization failed\n");
		return 1;
	}
	if (test_enqueue_fallback(&config) != 0) {
		fprintf(stderr, "enqueue fallback failed\n");
		return 1;
	}
	if (test_overload_abort(&config) != 0) {
		fprintf(stderr, "overload abort failed\n");
		return 1;
	}
	if (test_inbox_priority(&config) != 0) {
		fprintf(stderr, "inbox priority failed\n");
		return 1;
	}
	if (test_main_interval(&config) != 0) {
		fprintf(stderr, "main interval failed\n");
		return 1;
	}
	if (test_inbox_affinity(&config) != 0) {
		fprintf(stderr, "inbox affinity failed\n");
		return 1;
	}
	if (croutine_scheduler_create(&unstarted, &config) != 0 || croutine_scheduler_destroy(unstarted) != 0)
		return 1;
	if (croutine_scheduler_create(&scheduler, &config) != 0 || croutine_scheduler_stop(scheduler) != 0 ||
		atomic_load_explicit(&scheduler->state, memory_order_acquire) != CROUTINE_SCHEDULER_INIT ||
		croutine_spawn(scheduler, state_task, &context) != 0 || croutine_scheduler_start(scheduler) != 0 ||
		croutine_scheduler_start(scheduler) != 0 || croutine_scheduler_destroy(scheduler) != -1)
		goto out;

	if (wait_for_stage(&context, 1) != 0 || context.task == NULL ||
		wait_for_task_state(&context, CROUTINE_TASK_WAITING) != 0)
		goto out_stop;
	if (croutine_task_wake(context.task) != 0)
		goto out_stop;
	for (size_t round = 0; round < RACE_ROUNDS; round++) {
		if (wait_for_stage(&context, (int)round + 2) != 0 || croutine_task_wake(context.task) != 0)
			goto out_stop;
	}
	if (wait_for_stage(&context, RACE_ROUNDS + 2) != 0 ||
		atomic_load_explicit(&context.failed, memory_order_acquire) != 0 ||
		wait_for_worker_state(&scheduler->workers[0], CROUTINE_WORKER_SOURCE_WAITING) != 0 ||
		atomic_load_explicit(&scheduler->waiting_workers, memory_order_acquire) != 1)
		goto out_stop;

	if (croutine_scheduler_stop(scheduler) != 0 || croutine_scheduler_stop(scheduler) != 0 ||
		croutine_spawn(scheduler, state_task, &context) != -1 ||
		atomic_load_explicit(&scheduler->state, memory_order_acquire) != CROUTINE_SCHEDULER_STOPPED ||
		atomic_load_explicit(&scheduler->workers[0].state, memory_order_acquire) != CROUTINE_WORKER_SUSPENDED ||
		atomic_load_explicit(&scheduler->waiting_workers, memory_order_acquire) != 0)
		goto out_stop;

	if (scheduler->workers[0].main_event_source->wake(scheduler->workers[0].main_event_source) != 0)
		goto out_stop;
	{
		struct timespec pause = { .tv_nsec = 10 * WAIT_NS };

		nanosleep(&pause, NULL);
	}
	if (atomic_load_explicit(&scheduler->workers[0].state, memory_order_acquire) != CROUTINE_WORKER_SUSPENDED)
		goto out_stop;

	for (size_t round = 0; round < LIFECYCLE_ROUNDS; round++) {
		if (croutine_scheduler_start(scheduler) != 0 || croutine_scheduler_stop(scheduler) != 0 ||
			atomic_load_explicit(&scheduler->workers[0].state, memory_order_acquire) != CROUTINE_WORKER_SUSPENDED ||
			atomic_load_explicit(&scheduler->waiting_workers, memory_order_acquire) != 0)
			goto out_stop;
	}
	status = 0;

out_stop:
	if (croutine_scheduler_stop(scheduler) != 0)
		status = 1;
out:
	if (scheduler != NULL && croutine_scheduler_destroy(scheduler) != 0)
		status = 1;
	if (status == 0)
		puts("All task state tests passed successfully!");
	return status;
}
